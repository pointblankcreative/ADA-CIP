"""Tests for the StackAdapt reach/frequency direct feed (ADA 1215990005858637).

Background (AI-111 + AI-112): StackAdapt "reach" via Funnel.io is a 1-day
per-creative reach field, not deduplicated multi-day reach (wrong by 7-10x),
and StackAdapt frequency is hardcoded 0.0 upstream. Stage 2 keeps Funnel's SA
R&F excluded from every SQL aggregate (it stays garbage and must never
surface) but adds a SEPARATE Python-side FILL layer that supplies the REAL
StackAdapt reach/frequency from the direct StackAdapt reachFrequency API feed
(`cip_stackadapt.stackadapt_reach_frequency`, current calendar-month bucket).
Funnel stays source of truth for spend / impressions / clicks — those must
NOT change.

These tests mirror the stub pattern in test_projects_router_pacing.py: bq is
patched so no SQL executes. We assert:

  (a) the emitted SQL STILL excludes RF_EXCLUDED_PLATFORMS from every Funnel
      R&F aggregate (totals, daily, adset rollups, platform/campaign
      breakdowns, high-frequency warning) — Stage 2 does not touch the SQL,
  (b) when the SA-direct feed answers, StackAdapt IS listed in
      metric_platforms for reach / frequency and appears in reach_platforms,
  (c) reach_note appends SA_DIRECT_NOTE when the direct feed returns
      current-month numbers, and falls back to RF_EXCLUDED_NOTE (honest "not
      reporting") when StackAdapt is active but not yet synced,
  (d) per-row drilldowns (campaigns, by_platform) carry the SA-direct
      individual + household reach/frequency; a SA campaign with no direct
      row stays reach=None (honest em-dash); /adsets stays campaign-grain
      nulled with the SA_ADSET_NOTE,
  (e) other platforms (Meta etc.) are completely unaffected, and
      StackAdapt spend / impressions / clicks still flow through Funnel.
"""

from __future__ import annotations

import datetime
from unittest.mock import patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.routers import performance as perf_router


# ── Helpers ──────────────────────────────────────────────────────────


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


D1 = datetime.date(2026, 5, 1)
D2 = datetime.date(2026, 5, 31)


def _totals_row(**overrides):
    base = {
        "min_date": D1,
        "max_date": D2,
        "total_spend": 50000.0,
        "total_impressions": 4_000_000,
        "total_clicks": 12000,
        "total_conversions": 100.0,
        "total_reach": 800_000,
        "total_frequency": 2.4,
        "total_video_views": 100_000,
        "total_video_completions": 60_000,
        "total_vcr": 0.6,
        "total_engagements": 5000,
        "total_cpa": 500.0,
        "total_conversion_rate": 0.008,
    }
    base.update(overrides)
    return base


def _platform_row(platform_id, **overrides):
    base = {
        "platform_id": platform_id,
        "spend": 10000.0,
        "impressions": 1_000_000,
        "clicks": 3000,
        "conversions": 20.0,
        "reach": 200_000,
        "frequency": 2.0,
        "video_views": 50_000,
        "video_completions": 30_000,
        "engagements": 2000,
    }
    base.update(overrides)
    return base


def _campaign_row(platform_id, campaign_id="c-1", **overrides):
    base = {
        "campaign_id": campaign_id,
        "campaign_name": f"{platform_id} campaign",
        "platform_id": platform_id,
        "spend": 10000.0,
        "impressions": 1_000_000,
        "clicks": 3000,
        "conversions": 20.0,
        "cpm": 10.0,
        "cpc": 3.33,
        "ctr": 0.003,
        "reach": 200_000,
        "frequency": 2.0,
        "video_views": 50_000,
        "video_completions": 30_000,
        "vcr": 0.6,
        "engagements": 2000,
        "cpa": 500.0,
        "conversion_rate": 0.0066,
    }
    base.update(overrides)
    return base


def _get_performance(rec, code="26018"):
    """Drive GET /api/performance/{code} with the canned response queue.

    Query order in the router:
      1. totals_sql      2. daily_sql        3. adset_daily_sql
      4. sum_sql         5. plat_sql         6. warn_sql
      7. platform_sql    8. campaign_sql     9. sa_direct_sql*
     10. media_plan_objectives

    *sa_direct_sql (ADA 1215990005858637) is issued ONLY when at least one
    campaign row is StackAdapt (non-empty campaign_id). Meta-only fixtures
    therefore skip it and media_plan_objectives becomes call #9.
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


# Canned SA-direct row (ADA 1215990005858637) keyed on campaign_id "c-sa" —
# matches the StackAdapt campaign row below. Individual > household, freq 2-3x
# (the real 26022 CATIE shape validated 2026-07-13).
SA_DIRECT_ROW = {
    "campaign_id": "c-sa",
    "reach": 26_800,
    "frequency": 3.2,
    "reach_household": 8_600,
    "frequency_household": 2.0,
}


def _mixed_project_responses(rec):
    """Meta + StackAdapt project. The SQL stub can't apply the SQL-side
    exclusion, so canned rows simulate what BigQuery returns: StackAdapt
    rows carry NULL reach/frequency in fact_digital_daily rollups (the
    IF(...IN UNNEST(@rf_excluded), NULL, ...) projection) and are absent
    from fact_adset_daily rollups (the NOT IN UNNEST WHERE guard).

    The per-platform / per-campaign Funnel rows intentionally carry NON-NULL
    R&F for StackAdapt so the tests prove the router ignores Funnel's SA reach
    entirely — the surfaced SA numbers come only from the SA-direct feed
    (SA_DIRECT_ROW), issued as an extra query after campaign_sql."""
    rec.responses = [
        [_totals_row()],                                        # totals_sql
        [],                                                     # daily_sql
        [],                                                     # adset_daily_sql
        [{"max_reach": 500_000, "avg_freq": 2.1}],              # sum_sql
        [{"platform_id": "meta"}],                              # plat_sql
        [],                                                     # warn_sql
        [
            _platform_row("meta"),
            _platform_row("stackadapt", reach=999_999, frequency=0.0),
        ],                                                      # platform_sql
        [
            _campaign_row("meta", "c-meta"),
            _campaign_row("stackadapt", "c-sa", reach=999_999, frequency=3.3),
        ],                                                      # campaign_sql
        [SA_DIRECT_ROW],                                        # sa_direct_sql
        [],                                                     # media_plan_objectives
    ]
    return rec


# ── Constant / wiring ────────────────────────────────────────────────


def test_rf_excluded_platforms_constant():
    """The Funnel-side exclusion stays keyed on this set — Stage 2 does NOT
    empty it (Funnel's SA reach is still garbage). The SA-direct feed fills
    the real numbers separately."""
    assert perf_router.RF_EXCLUDED_PLATFORMS == {"stackadapt"}
    # SA_DIRECT_NOTE is what the endpoint appends when the direct feed answers.
    assert "StackAdapt" in perf_router.SA_DIRECT_NOTE
    assert "calendar month" in perf_router.SA_DIRECT_NOTE
    # The honest "not synced yet" fallback note is still available.
    assert "StackAdapt" in perf_router.RF_EXCLUDED_NOTE


# ── SQL exclusion (main endpoint) ────────────────────────────────────


def test_every_rf_aggregate_sql_excludes_stackadapt():
    """Each R&F rollup must reference @rf_excluded:
    totals + daily + platform + campaign use the conditional-NULL
    projection; the fact_adset_daily rollups (per-date series, summary,
    reach platforms, high-frequency warning) use the NOT IN guard."""
    rec = _mixed_project_responses(QueryRecorder())
    resp = _get_performance(rec)
    assert resp.status_code == 200, resp.text

    sqls = [sql for sql, _ in rec.calls]
    (totals_sql, daily_sql, adset_daily_sql, sum_sql, plat_sql,
     warn_sql, platform_sql, campaign_sql) = sqls[:8]

    cond_null = "IF(f.platform_id IN UNNEST(@rf_excluded), NULL"
    for label, sql in [
        ("totals_sql", totals_sql),
        ("daily_sql", daily_sql),
        ("platform_sql", platform_sql),
        ("campaign_sql", campaign_sql),
    ]:
        assert cond_null in sql, (
            f"{label} must NULL out R&F for excluded platforms via the "
            f"conditional projection:\n{sql}"
        )
        # Spend / impressions / clicks must stay unconditional (Funnel is
        # still source of truth for them).
        assert "SUM(f.spend)" in sql or "COALESCE(SUM(f.spend), 0)" in sql
        assert "IF(f.platform_id IN UNNEST(@rf_excluded), NULL, f.spend" not in sql
        assert "IF(f.platform_id IN UNNEST(@rf_excluded), NULL, f.impressions" not in sql
        assert "IF(f.platform_id IN UNNEST(@rf_excluded), NULL, f.clicks" not in sql

    not_in_guard = "platform_id NOT IN UNNEST(@rf_excluded)"
    for label, sql in [
        ("adset_daily_sql", adset_daily_sql),
        ("sum_sql", sum_sql),
        ("plat_sql", plat_sql),
        ("warn_sql", warn_sql),
    ]:
        assert "fact_adset_daily" in sql, f"{label} should hit fact_adset_daily"
        assert not_in_guard in sql, (
            f"{label} must exclude RF_EXCLUDED_PLATFORMS:\n{sql}"
        )

    # And the @rf_excluded array param actually rides along.
    flat_params = [p for _, params in rec.calls[:8] for p in params]
    assert ("array", "rf_excluded", "STRING", ["stackadapt"]) in flat_params


# ── metric_platforms ────────────────────────────────────────────────


def test_metric_platforms_lists_stackadapt_when_direct_feed_present():
    """metric_platforms drives the AI-026 tile subtitle. With the SA-direct
    feed answering, StackAdapt IS a reach/frequency contributor (its numbers
    come from the direct feed, not Funnel), alongside Meta from the adset
    rollup."""
    rec = _mixed_project_responses(QueryRecorder())
    resp = _get_performance(rec)
    assert resp.status_code == 200, resp.text
    body = resp.json()

    mp = body["metric_platforms"]
    assert "StackAdapt" in mp.get("reach", []), mp
    assert "StackAdapt" in mp.get("frequency", []), mp
    assert "Meta" in mp.get("reach", []), mp
    # Non-R&F metrics still list StackAdapt from the Funnel platform rollup.
    assert "StackAdapt" in mp.get("video_views", []), mp


# ── reach_note ──────────────────────────────────────────────────────


def test_reach_note_appends_sa_direct_note_when_feed_present():
    """Existing Meta note prefix stays; the SA-direct sentence is appended
    (not the 'hidden pending' fallback) because the direct feed answered.
    StackAdapt now IS a reach contributor."""
    rec = _mixed_project_responses(QueryRecorder())
    resp = _get_performance(rec)
    assert resp.status_code == 200, resp.text
    body = resp.json()

    note = body["reach_note"]
    assert note is not None
    assert note.startswith("Reach from Meta."), note
    assert note.endswith(perf_router.SA_DIRECT_NOTE), note
    # StackAdapt now contributes reach (from the direct feed).
    assert "stackadapt" in body["reach_platforms"]


def test_stackadapt_only_project_reach_comes_from_direct_feed():
    """StackAdapt-only project: no other reach platform, so the note is just
    the SA-direct sentence — and the Reach/Frequency KPI now populates from the
    direct feed (the whole point of Stage 2). total_reach comes from Funnel
    (still NULL for SA), but total_reach_adset carries the SA-direct number."""
    rec = QueryRecorder()
    rec.responses = [
        [_totals_row(total_reach=None, total_frequency=None)],  # totals_sql
        [],                                                     # daily_sql
        [],                                                     # adset_daily_sql
        [{"max_reach": None, "avg_freq": None}],                # sum_sql
        [],                                                     # plat_sql (rf-guarded → empty)
        [],                                                     # warn_sql
        [_platform_row("stackadapt", reach=None, frequency=None)],  # platform_sql
        [_campaign_row("stackadapt", "c-sa", reach=None, frequency=None)],  # campaign_sql
        [SA_DIRECT_ROW],                                        # sa_direct_sql
        [],                                                     # media_plan_objectives
    ]
    resp = _get_performance(rec)
    assert resp.status_code == 200, resp.text
    body = resp.json()

    assert body["reach_note"] == perf_router.SA_DIRECT_NOTE
    assert body["reach_platforms"] == ["stackadapt"]
    # Funnel totals stay NULL for SA (never surface the garbage column)…
    assert body["total_reach"] is None
    assert body["total_frequency"] is None
    # …but the headline Reach/Frequency KPI is now filled from the direct feed.
    assert body["total_reach_adset"] == 26_800
    assert body["avg_frequency_adset"] == pytest.approx(3.2)
    assert body["total_reach_household"] == 8_600
    assert body["avg_frequency_household"] == pytest.approx(2.0)
    # metric_platforms now declares reach/frequency, attributed to StackAdapt.
    assert body["metric_platforms"].get("reach") == ["StackAdapt"]
    assert body["metric_platforms"].get("frequency") == ["StackAdapt"]
    assert "reach" in body["available_metrics"]
    assert "frequency" in body["available_metrics"]


def test_reach_note_unchanged_without_stackadapt():
    """Meta-only project: note text identical to pre-stopgap behaviour."""
    rec = QueryRecorder()
    rec.responses = [
        [_totals_row()],                                        # totals_sql
        [],                                                     # daily_sql
        [],                                                     # adset_daily_sql
        [{"max_reach": 500_000, "avg_freq": 2.1}],              # sum_sql
        [{"platform_id": "meta"}],                              # plat_sql
        [],                                                     # warn_sql
        [_platform_row("meta")],                                # platform_sql
        [_campaign_row("meta", "c-meta")],                      # campaign_sql
        [],                                                     # media_plan_objectives
    ]
    resp = _get_performance(rec)
    assert resp.status_code == 200, resp.text
    body = resp.json()

    assert body["reach_note"] == "Reach from Meta."
    assert "StackAdapt" not in body["reach_note"]


# ── per-row null-out + other platforms unaffected ───────────────────


def test_stackadapt_rows_filled_from_direct_feed_other_platforms_untouched():
    """by_platform + campaigns: StackAdapt rows now carry the SA-direct
    individual + household reach/frequency (NOT Funnel's 999_999 garbage);
    Meta keeps its Funnel numbers. StackAdapt spend / impressions / clicks
    still flow through Funnel untouched."""
    rec = _mixed_project_responses(QueryRecorder())
    resp = _get_performance(rec)
    assert resp.status_code == 200, resp.text
    body = resp.json()

    plats = {p["platform_id"]: p for p in body["by_platform"]}
    # SA platform row carries the platform-level SA-direct rollup, not Funnel's.
    assert plats["stackadapt"]["reach"] == 26_800
    assert plats["stackadapt"]["frequency"] == pytest.approx(3.2)
    assert plats["stackadapt"]["reach_household"] == 8_600
    assert plats["stackadapt"]["frequency_household"] == pytest.approx(2.0)
    assert plats["meta"]["reach"] == 200_000
    assert plats["meta"]["frequency"] == 2.0
    # Household is SA-only — Meta never carries it.
    assert plats["meta"]["reach_household"] is None
    # Funnel-sourced metrics for StackAdapt are NOT touched.
    assert plats["stackadapt"]["spend"] == 10000.0
    assert plats["stackadapt"]["impressions"] == 1_000_000
    assert plats["stackadapt"]["clicks"] == 3000

    camps = {c["platform_id"]: c for c in body["campaigns"]}
    # SA campaign row filled from the direct feed keyed on campaign_id "c-sa".
    assert camps["stackadapt"]["reach"] == 26_800
    assert camps["stackadapt"]["frequency"] == pytest.approx(3.2)
    assert camps["stackadapt"]["reach_household"] == 8_600
    assert camps["meta"]["reach"] == 200_000
    assert camps["meta"]["frequency"] == 2.0
    assert camps["stackadapt"]["spend"] == 10000.0

    # Meta's adset reach (500k) still dominates the headline (reach is
    # non-additive → MAX); SA (26.8k) doesn't lower it.
    assert body["total_reach_adset"] == 500_000
    assert body["avg_frequency_adset"] == 2.1
    # Household headline comes from SA.
    assert body["total_reach_household"] == 8_600


# ── /adsets drilldown ────────────────────────────────────────────────


def _adset_row(platform_id, **overrides):
    base = {
        "ad_set_id": f"as-{platform_id}",
        "ad_set_name": f"{platform_id} audience",
        "platform_id": platform_id,
        "campaign_name": f"{platform_id} campaign",
        "spend": 5000.0,
        "impressions": 400_000,
        "clicks": 1200,
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
    }
    base.update(overrides)
    return base


def test_adsets_endpoint_nulls_stackadapt_rf_and_appends_note():
    rec = QueryRecorder()
    rec.responses = [[
        _adset_row("meta"),
        # Simulates a regression where the reach CTE leaked values through —
        # the Python guard must still null them.
        _adset_row("stackadapt", reach=77_777, frequency=0.0),
    ]]

    patches = _bq_patches(rec)
    for p in patches:
        p.start()
    try:
        client = TestClient(_make_app())
        resp = client.get("/api/performance/26018/adsets")
    finally:
        for p in patches:
            p.stop()

    assert resp.status_code == 200, resp.text
    body = resp.json()

    rows = {r["platform_id"]: r for r in body["ad_sets"]}
    assert rows["stackadapt"]["reach"] is None
    assert rows["stackadapt"]["frequency"] is None
    assert rows["stackadapt"]["reach_window"] is None
    assert rows["stackadapt"]["cost_per_reach"] is None
    # Meta untouched.
    assert rows["meta"]["reach"] == 100_000
    assert rows["meta"]["frequency"] == 2.5
    # Funnel-sourced metrics untouched on the StackAdapt row.
    assert rows["stackadapt"]["spend"] == 5000.0
    assert rows["stackadapt"]["impressions"] == 400_000

    # Note: existing text preserved; the campaign-grain SA note is appended
    # (adset grain can't carry per-audience SA reach — that's on Summary).
    note = body["total_reach_note"]
    assert note is not None
    assert note.startswith("Reach from Meta. Not additive across audiences."), note
    assert note.endswith(perf_router.SA_ADSET_NOTE), note

    # And the reach CTE SQL carries the exclusion guard.
    sql = rec.calls[0][0]
    assert "platform_id NOT IN UNNEST(@rf_excluded)" in sql
    flat_params = list(rec.calls[0][1])
    assert ("array", "rf_excluded", "STRING", ["stackadapt"]) in flat_params


def test_adsets_endpoint_no_note_change_without_stackadapt():
    rec = QueryRecorder()
    rec.responses = [[_adset_row("meta")]]

    patches = _bq_patches(rec)
    for p in patches:
        p.start()
    try:
        client = TestClient(_make_app())
        resp = client.get("/api/performance/26018/adsets")
    finally:
        for p in patches:
            p.stop()

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["total_reach_note"] == (
        "Reach from Meta. Not additive across audiences."
    )
    assert "StackAdapt" not in body["total_reach_note"]
