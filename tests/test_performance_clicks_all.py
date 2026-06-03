"""Tests for the AI-102 labeled-coexistence clicks fix.

Background (AI-102): the filed "clicks means different things per grain"
claim did not reproduce — every grain already stores Meta Link Clicks, and
the 7,268 the audit matched to CSV "Clicks (all)" was the adjacent
`engagements` field. The REAL defects, all fixed here:

  (A) `clicks` means a different thing per PLATFORM (Meta: link clicks;
      Google/StackAdapt: all clicks; Snapchat: swipes; …), silently summed
      into the project total with no labeling anywhere. → `clicks` stays
      UNCHANGED (it is the canonical destination-intent click and the F1
      Meta benchmark is calibrated against it); the API now surfaces
      `clicks_definitions` (per-platform definition strings) for tooltips.
  (B) Meta/TikTok all-clicks were stored under the `engagements` label,
      inflating the displayed Eng. rate ~2x project-wide. → new first-class
      `clicks_all` column at every grain; Meta `engagements` remapped to
      Post_Engagement; TikTok `engagements` to NULL (its real engagement
      columns are still unmapped). That remap lives in the transform SQL
      (see test_transform_sql_clicks_all.py); these tests pin the API
      surface.

These tests mirror the QueryRecorder stub pattern in
test_performance_rf_stopgap.py: bq is patched so no SQL executes. We assert:

  (a) every grain's SQL selects SUM(f.clicks_all) AND leaves SUM(f.clicks)
      untouched (canonical definition unchanged — the load-bearing
      guarantee of the labeled-coexistence design);
  (b) clicks_all flows through to the response at every grain (totals,
      daily, by_platform, campaigns, adsets, ads, creatives), with None
      for platforms that don't report it / pre-backfill history;
  (c) `clicks_definitions` is present, keyed by active platform_id, with
      the documented definition strings (unknown platforms get the
      fallback);
  (d) `clicks` values themselves are passed through unchanged.

Real-world anchors (26018 Meta flight-to-date as of 2026-06-02): clicks
7,363 (link, unchanged) / clicks_all 26,012 (previously hiding under
`engagements`) / engagements 12,736 (= post_engagement) after the FULL
backfill.
"""

from __future__ import annotations

import datetime
from unittest.mock import patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.routers import performance as perf_router


# ── Helpers (mirroring test_performance_rf_stopgap.py) ───────────────


def _make_app() -> FastAPI:
    app = FastAPI()
    app.include_router(perf_router.router)
    return app


class QueryRecorder:
    """Stub for bq.run_query that records every call and returns canned rows."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, list]] = []
        self.responses: list[list[dict]] = []

    def __call__(self, sql: str, params=None):
        self.calls.append((sql, list(params or [])))
        if self.responses:
            return self.responses.pop(0)
        return []


def _string_param(name, value):
    return ("string", name, value)


def _date_param(name, value):
    return ("date", name, value)


def _array_param(name, type_, values):
    return ("array", name, type_, list(values))


def _table(name):
    return f"`dummy.{name}`"


def _bq_patches(rec):
    return [
        patch.object(perf_router.bq, "run_query", side_effect=rec),
        patch.object(perf_router.bq, "string_param", _string_param),
        patch.object(perf_router.bq, "date_param", _date_param),
        patch.object(perf_router.bq, "array_param", _array_param),
        patch.object(perf_router.bq, "table", _table),
    ]


D1 = datetime.date(2026, 4, 27)
D2 = datetime.date(2026, 6, 2)


def _totals_row(**overrides):
    base = {
        "min_date": D1,
        "max_date": D2,
        "total_spend": 7341.0,
        "total_impressions": 2_030_000,
        "total_clicks": 7363,
        "total_clicks_all": 26012,
        "total_conversions": 951.0,
        "total_reach": 800_000,
        "total_frequency": 2.4,
        "total_video_views": 100_000,
        "total_video_completions": 60_000,
        "total_vcr": 0.6,
        "total_engagements": 12736,
        "total_cpa": 7.72,
        "total_conversion_rate": 0.129,
    }
    base.update(overrides)
    return base


def _daily_row(**overrides):
    base = {
        "date": D1,
        "spend": 250.0,
        "impressions": 70_000,
        "clicks": 260,
        "clicks_all": 920,
        "conversions": 30.0,
        "cpm": 3.57,
        "cpc": 0.96,
        "ctr": 0.0037,
        "reach": 50_000,
        "frequency": 1.4,
        "video_views": 3_000,
        "video_completions": 1_800,
        "vcr": 0.6,
        "engagements": 450,
        "cpa": 8.33,
        "conversion_rate": 0.115,
    }
    base.update(overrides)
    return base


def _platform_row(platform_id, **overrides):
    base = {
        "platform_id": platform_id,
        "spend": 7341.0,
        "impressions": 1_683_000,
        "clicks": 7363,
        "clicks_all": 26012,
        "conversions": 951.0,
        "reach": 200_000,
        "frequency": 2.0,
        "video_views": 50_000,
        "video_completions": 30_000,
        "engagements": 12736,
    }
    base.update(overrides)
    return base


def _campaign_row(platform_id, campaign_id="c-1", **overrides):
    base = {
        "campaign_id": campaign_id,
        "campaign_name": f"{platform_id} campaign",
        "platform_id": platform_id,
        "spend": 2781.0,
        "impressions": 265_641,
        "clicks": 3887,
        "clicks_all": 19656,
        "conversions": 951.0,
        "cpm": 10.47,
        "cpc": 0.72,
        "ctr": 0.0146,
        "reach": 200_000,
        "frequency": 2.0,
        "video_views": 50_000,
        "video_completions": 30_000,
        "vcr": 0.6,
        "engagements": 6_000,
        "cpa": 2.92,
        "conversion_rate": 0.245,
    }
    base.update(overrides)
    return base


def _get_performance(rec, code="26018"):
    """Drive GET /api/performance/{code} with the canned response queue.

    Query order in the router:
      1. totals_sql      2. daily_sql        3. adset_daily_sql
      4. sum_sql         5. plat_sql         6. warn_sql
      7. platform_sql    8. campaign_sql     9. media_plan_objectives
    """
    patches = _bq_patches(rec)
    for p in patches:
        p.start()
    try:
        client = TestClient(_make_app())
        return client.get(f"/api/performance/{code}")
    finally:
        for p in patches:
            p.stop()


def _project_responses(rec, platform_rows=None, campaign_rows=None,
                       totals=None, daily=None):
    rec.responses = [
        [totals or _totals_row()],                              # totals_sql
        daily if daily is not None else [_daily_row()],         # daily_sql
        [],                                                     # adset_daily_sql
        [{"max_reach": 500_000, "avg_freq": 2.1}],              # sum_sql
        [{"platform_id": "meta"}],                              # plat_sql
        [],                                                     # warn_sql
        platform_rows or [_platform_row("meta")],               # platform_sql
        campaign_rows or [_campaign_row("meta", "c-meta")],     # campaign_sql
        [],                                                     # media_plan_objectives
    ]
    return rec


def _get(rec, path):
    patches = _bq_patches(rec)
    for p in patches:
        p.start()
    try:
        client = TestClient(_make_app())
        return client.get(path)
    finally:
        for p in patches:
            p.stop()


# ── Constant / wiring ────────────────────────────────────────────────


def test_clicks_definitions_constant_covers_all_named_platforms():
    """Every platform in PLATFORM_NAMES that flows through fact_digital_daily
    has a documented clicks definition (perion is DOOH — fact_dooh_daily —
    and never reaches the clicks surface)."""
    expected = {
        "meta", "google_ads", "stackadapt", "tiktok",
        "snapchat", "linkedin", "reddit", "pinterest",
    }
    assert set(perf_router.CLICKS_DEFINITIONS) == expected
    # The two load-bearing definitions (AI-102's Meta confusion + the
    # clicks_all pointer).
    assert "Link clicks" in perf_router.CLICKS_DEFINITIONS["meta"]
    assert "clicks_all" in perf_router.CLICKS_DEFINITIONS["meta"]
    assert "Destination clicks" in perf_router.CLICKS_DEFINITIONS["tiktok"]
    assert "Swipe" in perf_router.CLICKS_DEFINITIONS["snapchat"]


# ── SQL shape: clicks_all selected, clicks untouched, at every grain ─


def test_main_endpoint_sql_sums_clicks_all_and_keeps_clicks():
    rec = _project_responses(QueryRecorder())
    resp = _get_performance(rec)
    assert resp.status_code == 200, resp.text

    sqls = [sql for sql, _ in rec.calls]
    totals_sql, daily_sql = sqls[0], sqls[1]
    platform_sql, campaign_sql = sqls[6], sqls[7]

    for label, sql in [
        ("totals_sql", totals_sql),
        ("daily_sql", daily_sql),
        ("platform_sql", platform_sql),
        ("campaign_sql", campaign_sql),
    ]:
        assert "SUM(f.clicks_all)" in sql, (
            f"{label} must aggregate the new clicks_all column:\n{sql}"
        )
        # Canonical clicks UNCHANGED — definitionally load-bearing (F1's
        # Meta benchmark is calibrated to link-click CTR).
        assert "SUM(f.clicks)" in sql or "COALESCE(SUM(f.clicks), 0)" in sql, (
            f"{label} must keep the canonical clicks aggregate:\n{sql}"
        )
    assert "AS total_clicks_all" in totals_sql


def test_drilldown_sql_sums_clicks_all_at_each_grain():
    # /adsets
    rec = QueryRecorder()
    rec.responses = [[]]
    resp = _get(rec, "/api/performance/26018/adsets")
    assert resp.status_code == 200, resp.text
    assert "SUM(f.clicks_all) AS clicks_all" in rec.calls[0][0]
    assert "SUM(f.clicks) AS clicks" in rec.calls[0][0]

    # /ads
    rec = QueryRecorder()
    rec.responses = [[]]
    resp = _get(rec, "/api/performance/26018/ads")
    assert resp.status_code == 200, resp.text
    assert "SUM(f.clicks_all) AS clicks_all" in rec.calls[0][0]
    assert "SUM(f.clicks) AS clicks" in rec.calls[0][0]

    # /creatives — first call is the alias-table probe, second the real query
    rec = QueryRecorder()
    rec.responses = [[], []]
    resp = _get(rec, "/api/performance/26018/creatives")
    assert resp.status_code == 200, resp.text
    creative_sql = rec.calls[1][0]
    # Inner ad_agg CTE and the outer rollup both carry clicks_all.
    assert "SUM(f.clicks_all) AS clicks_all" in creative_sql
    assert "SUM(clicks_all) AS clicks_all" in creative_sql


# ── Response passthrough: totals + clicks_definitions ───────────────


def test_totals_carry_clicks_all_and_definitions():
    rec = _project_responses(QueryRecorder())
    resp = _get_performance(rec)
    assert resp.status_code == 200, resp.text
    body = resp.json()

    # AI-102 ground truth shape (26018 Meta flight-to-date).
    assert body["total_clicks"] == 7363
    assert body["total_clicks_all"] == 26012
    assert body["total_engagements"] == 12736

    defs = body["clicks_definitions"]
    assert defs == {
        "meta": "Link clicks (Meta). All-clicks available as clicks_all.",
    }


def test_clicks_definitions_keyed_by_active_platforms_with_fallback():
    rec = _project_responses(
        QueryRecorder(),
        platform_rows=[
            _platform_row("meta"),
            _platform_row("stackadapt", clicks_all=None),
            _platform_row("newplatform", clicks_all=None),
        ],
        campaign_rows=[_campaign_row("meta", "c-meta")],
    )
    resp = _get_performance(rec)
    assert resp.status_code == 200, resp.text
    defs = resp.json()["clicks_definitions"]

    assert set(defs) == {"meta", "stackadapt", "newplatform"}
    assert defs["stackadapt"] == "Clicks (StackAdapt)."
    # Unknown platform → documented fallback, never a KeyError.
    assert defs["newplatform"] == "Platform-reported clicks."


def test_total_clicks_all_none_when_no_platform_reports_it():
    """SUM over all-NULL clicks_all is NULL in BQ → None in the response
    (StackAdapt-only projects, and all projects pre-backfill)."""
    rec = _project_responses(
        QueryRecorder(),
        totals=_totals_row(total_clicks_all=None),
        platform_rows=[_platform_row("stackadapt", clicks_all=None)],
        campaign_rows=[_campaign_row("stackadapt", "c-sa", clicks_all=None)],
        daily=[_daily_row(clicks_all=None)],
    )
    resp = _get_performance(rec)
    assert resp.status_code == 200, resp.text
    body = resp.json()

    assert body["total_clicks_all"] is None
    assert body["total_clicks"] == 7363  # canonical clicks unaffected
    assert body["daily"][0]["clicks_all"] is None
    assert body["by_platform"][0]["clicks_all"] is None
    assert body["campaigns"][0]["clicks_all"] is None


# ── Response passthrough: per-row at every grain ─────────────────────


def test_daily_platform_campaign_rows_carry_clicks_all():
    rec = _project_responses(QueryRecorder())
    resp = _get_performance(rec)
    assert resp.status_code == 200, resp.text
    body = resp.json()

    assert body["daily"][0]["clicks"] == 260
    assert body["daily"][0]["clicks_all"] == 920

    plat = body["by_platform"][0]
    assert plat["platform_id"] == "meta"
    assert plat["clicks"] == 7363
    assert plat["clicks_all"] == 26012
    # The engagements field now carries post_engagement-derived values —
    # distinct from clicks_all (pre-AI-102 they were the same number).
    assert plat["engagements"] == 12736
    assert plat["engagements"] != plat["clicks_all"]

    camp = body["campaigns"][0]
    assert camp["clicks"] == 3887
    assert camp["clicks_all"] == 19656


def test_adsets_rows_carry_clicks_all():
    rec = QueryRecorder()
    rec.responses = [[
        {
            "ad_set_id": "as-1",
            "ad_set_name": "EN audience",
            "platform_id": "meta",
            "campaign_name": "meta campaign",
            "spend": 5000.0,
            "impressions": 400_000,
            "clicks": 1200,
            "clicks_all": 4300,
            "conversions": 10.0,
            "engagements": 800,
            "video_views": 20_000,
            "video_completions": 12_000,
            "cpm": 12.5,
            "cpc": 4.17,
            "ctr": 0.003,
            "vcr": 0.6,
            "engagement_rate": 0.002,
            "reach": 100_000,
            "frequency": 2.5,
            "reach_window": "7d",
            "cost_per_reach": 50.0,
            "ad_count": 4,
        },
        {
            "ad_set_id": "as-2",
            "ad_set_name": "SA audience",
            "platform_id": "stackadapt",
            "campaign_name": "sa campaign",
            "spend": 3000.0,
            "impressions": 350_000,
            "clicks": 385,
            "clicks_all": None,
            "conversions": 0.0,
            "engagements": 0,
            "video_views": 0,
            "video_completions": 0,
            "cpm": 8.57,
            "cpc": 7.79,
            "ctr": 0.0011,
            "vcr": None,
            "engagement_rate": 0.0,
            "reach": None,
            "frequency": None,
            "reach_window": None,
            "cost_per_reach": None,
            "ad_count": 2,
        },
    ]]
    resp = _get(rec, "/api/performance/26018/adsets")
    assert resp.status_code == 200, resp.text
    rows = {r["platform_id"]: r for r in resp.json()["ad_sets"]}

    assert rows["meta"]["clicks"] == 1200
    assert rows["meta"]["clicks_all"] == 4300
    assert rows["stackadapt"]["clicks"] == 385
    assert rows["stackadapt"]["clicks_all"] is None


def test_ads_rows_carry_clicks_all():
    rec = QueryRecorder()
    rec.responses = [[
        {
            "ad_id": "ad-1",
            "ad_name": "Ad D EN",
            "ad_set_name": "EN audience",
            "platform_id": "meta",
            "campaign_name": "meta campaign",
            "spend": 1000.0,
            "impressions": 100_000,
            "clicks": 1240,
            "clicks_all": 5800,
            "conversions": 100.0,
            "engagements": 700,
            "video_views": 5_000,
            "video_completions": 3_000,
            "cpm": 10.0,
            "cpc": 0.81,
            "ctr": 0.0124,
            "vcr": 0.6,
            "engagement_rate": 0.007,
        },
    ]]
    resp = _get(rec, "/api/performance/26018/ads")
    assert resp.status_code == 200, resp.text
    ad = resp.json()["ads"][0]

    assert ad["clicks"] == 1240
    assert ad["clicks_all"] == 5800
    # The AI-102 audit confusion: clicks must NOT equal the all-clicks
    # number at ad grain — they are now separately labeled fields.
    assert ad["clicks"] != ad["clicks_all"]


def test_creatives_rows_carry_clicks_all():
    rec = QueryRecorder()
    rec.responses = [
        [],  # alias-table probe
        [{
            "creative_variant": "Hero Video",
            "ad_names": ["Hero Video EN", "Hero Video FR"],
            "platforms": ["meta"],
            "ad_set_names": ["EN audience", "FR audience"],
            "ad_count": 2,
            "spend": 2000.0,
            "impressions": 200_000,
            "clicks": 900,
            "clicks_all": 3100,
            "conversions": 40.0,
            "engagements": 500,
            "video_views": 10_000,
            "video_completions": 6_000,
            "cpm": 10.0,
            "cpc": 2.22,
            "ctr": 0.0045,
            "vcr": 0.6,
            "engagement_rate": 0.0025,
        }],
    ]
    resp = _get(rec, "/api/performance/26018/creatives")
    assert resp.status_code == 200, resp.text
    cre = resp.json()["creatives"][0]

    assert cre["clicks"] == 900
    assert cre["clicks_all"] == 3100


def test_clicks_all_defaults_none_when_column_missing_from_rows():
    """Pre-backfill resilience: if a row dict lacks the clicks_all key
    entirely (e.g. canned/legacy fixtures), the model defaults to None
    instead of raising."""
    rec = QueryRecorder()
    legacy = _daily_row()
    legacy.pop("clicks_all")
    totals = _totals_row()
    totals.pop("total_clicks_all")
    plat = _platform_row("meta")
    plat.pop("clicks_all")
    camp = _campaign_row("meta")
    camp.pop("clicks_all")
    _project_responses(rec, totals=totals, daily=[legacy],
                       platform_rows=[plat], campaign_rows=[camp])
    resp = _get_performance(rec)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["total_clicks_all"] is None
    assert body["daily"][0]["clicks_all"] is None
    assert body["by_platform"][0]["clicks_all"] is None
    assert body["campaigns"][0]["clicks_all"] is None
