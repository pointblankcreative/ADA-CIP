"""Tests for the Phase 14 Creative + Audiences endpoints
(backend/routers/creative.py) and the additive PB-history benchmarks
extension (backend/routers/benchmarks.py).

Coverage:

  * Rotation — variant aggregation across platforms with coverage-aware
    rate denominators (engagement_rate divides by reporting-platform
    impressions only), adset-grain weighted frequency, spend_share,
    platform metric coverage lists, and window totals.
  * Volume guard — rate metrics (hook/completion/engagement/ctr) are
    nulled under 1,000 window impressions while spend / impressions /
    cpm / cpa survive (the F1_PER_PLATFORM_MIN_IMPRESSIONS philosophy).
  * window=7d — the date clause + @window_start param anchor at as_of
    (latest data date), not today; flight emits no date restriction.
  * Trend arrays — last 8 daily points, oldest → newest, None days
    dropped, empty when fewer than 2 usable points.
  * Creative matrix — absent cells where a variant doesn't run, platform
    spend shares, per-cell volume guard.
  * Audiences matrix — media-plan role join (exact + containment match,
    None when unmatched), stable audience id slugs, AI-120 frequency
    nulling, frequency trend ordering, audience × creative cells.
  * Benchmarks — hook_rate / engagement_rate quartiles computed from PB
    campaign history, additive only (seeded rows win; existing fields
    untouched), objective + volume filtered, min-sample gated.

All tests use the QueryRecorder stub pattern from
test_performance_adset_reach.py: bq is patched so no SQL executes, canned
rows model what the (BigQuery-side) SQL returns, and SQL-shape assertions
pin the clauses the canned rows assume.
"""

from __future__ import annotations

from datetime import date, timedelta
from unittest.mock import patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.routers import benchmarks as bench_router
from backend.routers import creative as creative_router


# ── Helpers (mirroring test_performance_adset_reach.py) ───────────────


def _make_app() -> FastAPI:
    app = FastAPI()
    app.include_router(creative_router.router)
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


def _scalar_param(name, type_, value):
    return ("scalar", name, type_, value)


def _table(name):
    return f"`dummy.{name}`"


def _bq_patches(module, rec):
    return [
        patch.object(module.bq, "run_query", side_effect=rec),
        patch.object(module.bq, "string_param", _string_param),
        patch.object(module.bq, "date_param", _date_param),
        patch.object(module.bq, "array_param", _array_param),
        patch.object(module.bq, "scalar_param", _scalar_param),
        patch.object(module.bq, "table", _table),
    ]


def _get(module, rec, path):
    patches = _bq_patches(module, rec)
    for p in patches:
        p.start()
    try:
        if module is creative_router:
            client = TestClient(_make_app())
        else:
            app = FastAPI()
            app.include_router(bench_router.router)
            client = TestClient(app)
        return client.get(path)
    finally:
        for p in patches:
            p.stop()


# ── Canned rows ───────────────────────────────────────────────────────

AS_OF = date(2026, 6, 8)

# Per-(variant, platform) rows as the rotation/matrix cells query returns
# them. "Hero Video" runs on Meta + StackAdapt; StackAdapt reports 3s
# starts but its engagements column is hardcoded 0 and its frequency is
# AI-120-excluded (freq_* NULL). "Static Banner" is Meta-only, no video.
HERO_META = {
    "creative_variant": "Hero Video", "platform_id": "meta",
    "spend": 6000.0, "impressions": 500_000, "clicks": 1500,
    "conversions": 50.0, "engagements": 8000,
    "video_views": 200_000, "video_completions": 80_000,
    "video_views_3s": 150_000,
    "freq_weighted": 1_000_000.0, "freq_impressions": 500_000,
}
HERO_SA = {
    "creative_variant": "Hero Video", "platform_id": "stackadapt",
    "spend": 4000.0, "impressions": 800_000, "clicks": 960,
    "conversions": 0.0, "engagements": 0,
    "video_views": 100_000, "video_completions": 60_000,
    "video_views_3s": 90_000,
    "freq_weighted": None, "freq_impressions": None,
}
BANNER_META = {
    "creative_variant": "Static Banner", "platform_id": "meta",
    "spend": 2000.0, "impressions": 250_000, "clicks": 500,
    "conversions": 10.0, "engagements": 3000,
    "video_views": 0, "video_completions": 0, "video_views_3s": 0,
    "freq_weighted": 375_000.0, "freq_impressions": 250_000,
}


def _hero_daily_rows():
    """10 days of Hero Video dailies (2026-05-30 → 06-08), returned in
    REVERSE order to prove the router sorts by date, not input order.
    Frequency is None on the first two days (no joinable snapshot)."""
    rows = []
    for i in range(1, 11):
        rows.append({
            "creative_variant": "Hero Video",
            "date": date(2026, 5, 29) + timedelta(days=i),
            "ctr": i / 1000,
            "completion_rate": 0.30 + i / 100,
            "cpa": None,
            "frequency": None if i <= 2 else 1.0 + i / 10,
        })
    rows.append({
        "creative_variant": "Static Banner",
        "date": date(2026, 6, 8),
        "ctr": 0.002, "completion_rate": None, "cpa": None, "frequency": None,
    })
    return list(reversed(rows))


def _rotation_responses(cells, daily):
    """Canned responses in the rotation endpoint's BQ call order:
    (1) MAX(date), (2) media-plan objectives, (3) distinct campaigns,
    (4) alias-table probe, (5) variant×platform cells, (6) daily trend."""
    return [
        [{"max_date": AS_OF}],
        [{"platform_id": "meta", "objective": "Awareness"}],
        [{"campaign_name": "Awareness Video", "platform_id": "meta"}],
        [],
        cells,
        daily,
    ]


# ── Rotation: variant aggregation across platforms ────────────────────


def test_rotation_aggregates_variants_across_platforms():
    rec = QueryRecorder()
    rec.responses = _rotation_responses(
        [HERO_META, HERO_SA, BANNER_META], _hero_daily_rows()
    )
    resp = _get(creative_router, rec, "/api/projects/26018/creative/rotation")
    assert resp.status_code == 200, resp.text
    body = resp.json()

    assert body["objective"] == "awareness"
    assert body["window"] == "flight"
    assert body["as_of"] == "2026-06-08"

    rows = {c["variant"]: c for c in body["creatives"]}
    assert list(rows) == ["Hero Video", "Static Banner"]  # spend DESC rank

    hero = rows["Hero Video"]
    assert hero["type"] == "video"
    assert hero["platforms"] == ["meta", "stackadapt"]
    assert hero["spend"] == 10_000.0
    assert hero["spend_share"] == pytest.approx(10_000 / 12_000)
    assert hero["impressions"] == 1_300_000
    assert hero["clicks"] == 2460
    assert hero["ctr"] == pytest.approx(2460 / 1_300_000)
    # Hook: both platforms report 3s in-window, so the denominator spans both.
    assert hero["hook_rate"] == pytest.approx(240_000 / 1_300_000)
    # Completion reuses the vcr definition (completions / views).
    assert hero["completion_rate"] == pytest.approx(140_000 / 300_000)
    # Engagement: StackAdapt's hardcoded-zero engagements keep it out of
    # coverage, so the rate divides by META impressions only — not diluted
    # by the 800k StackAdapt impressions.
    assert hero["engagement_rate"] == pytest.approx(8000 / 500_000)
    # Frequency: impressions-weighted over joinable adset snapshots only.
    assert hero["frequency"] == pytest.approx(2.0)
    assert hero["cpm"] == pytest.approx(10_000 / 1_300_000 * 1000)
    assert hero["conversions"] == 50.0
    assert hero["cpa"] == pytest.approx(200.0)

    banner = rows["Static Banner"]
    assert banner["type"] == "static"
    assert banner["hook_rate"] is None        # no video activity
    assert banner["completion_rate"] is None  # no video views
    assert banner["engagement_rate"] == pytest.approx(3000 / 250_000)

    # Coverage lists are platform_ids, sorted.
    assert body["coverage"] == {
        "hook": ["meta", "stackadapt"],
        "completion": ["meta", "stackadapt"],
        "engagement": ["meta"],
    }

    totals = body["totals"]
    assert totals["spend"] == 12_000.0
    assert totals["impressions"] == 1_550_000
    assert totals["clicks"] == 2960
    assert totals["conversions"] == 60.0
    assert totals["engagement_rate"] == pytest.approx(11_000 / 750_000)
    assert totals["frequency"] == pytest.approx(1_375_000 / 750_000)
    assert totals["hook_rate"] == pytest.approx(240_000 / 1_550_000)


def test_rotation_trend_arrays_last_8_oldest_to_newest():
    rec = QueryRecorder()
    rec.responses = _rotation_responses(
        [HERO_META, HERO_SA, BANNER_META], _hero_daily_rows()
    )
    resp = _get(creative_router, rec, "/api/projects/26018/creative/rotation")
    assert resp.status_code == 200, resp.text
    rows = {c["variant"]: c for c in resp.json()["creatives"]}

    hero_trend = rows["Hero Video"]["trend"]
    # 10 days supplied (reverse order) → last 8, sorted oldest → newest.
    assert hero_trend["ctr"] == pytest.approx([i / 1000 for i in range(3, 11)])
    # Awareness project → primary is completion_rate.
    assert hero_trend["primary"] == pytest.approx(
        [0.30 + i / 100 for i in range(3, 11)]
    )
    # Frequency None on days 1-2 (no snapshot) → dropped, 8 points remain.
    assert hero_trend["frequency"] == pytest.approx(
        [1.0 + i / 10 for i in range(3, 11)]
    )

    # A single daily point isn't a trend → empty arrays.
    banner_trend = rows["Static Banner"]["trend"]
    assert banner_trend == {"ctr": [], "frequency": [], "primary": []}


def test_rotation_primary_trend_is_cpa_for_conversion_projects():
    rec = QueryRecorder()
    daily = [
        {"creative_variant": "Hero Video", "date": date(2026, 6, d),
         "ctr": 0.001, "completion_rate": 0.5, "cpa": float(d), "frequency": None}
        for d in range(1, 6)
    ]
    rec.responses = [
        [{"max_date": AS_OF}],
        [{"platform_id": "meta", "objective": "Conversions"}],
        [{"campaign_name": "Lead Gen Conversion", "platform_id": "meta"}],
        [],
        [HERO_META],
        daily,
    ]
    resp = _get(creative_router, rec, "/api/projects/26018/creative/rotation")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["objective"] == "conversion"
    assert body["creatives"][0]["trend"]["primary"] == pytest.approx(
        [1.0, 2.0, 3.0, 4.0, 5.0]
    )


# ── Rotation: volume guard ────────────────────────────────────────────


def test_rotation_volume_guard_nulls_rates_under_1000_impressions():
    tiny = {
        "creative_variant": "Tiny", "platform_id": "meta",
        "spend": 50.0, "impressions": 500, "clicks": 50,
        "conversions": 2.0, "engagements": 10,
        "video_views": 400, "video_completions": 200, "video_views_3s": 100,
        "freq_weighted": None, "freq_impressions": None,
    }
    rec = QueryRecorder()
    rec.responses = _rotation_responses([tiny], [])
    resp = _get(creative_router, rec, "/api/projects/26018/creative/rotation")
    assert resp.status_code == 200, resp.text
    row = resp.json()["creatives"][0]

    # Rate metrics nulled — too noisy under 1,000 impressions.
    assert row["ctr"] is None
    assert row["hook_rate"] is None
    assert row["completion_rate"] is None
    assert row["engagement_rate"] is None
    # Volume + cost fields survive the guard.
    assert row["spend"] == 50.0
    assert row["impressions"] == 500
    assert row["clicks"] == 50
    assert row["cpm"] == pytest.approx(100.0)
    assert row["cpa"] == pytest.approx(25.0)
    assert row["type"] == "video"  # guard doesn't reclassify


def test_rotation_404_when_no_data():
    rec = QueryRecorder()
    rec.responses = [[]]
    resp = _get(creative_router, rec, "/api/projects/99999/creative/rotation")
    assert resp.status_code == 404


def test_rotation_rejects_unknown_window():
    rec = QueryRecorder()
    resp = _get(creative_router, rec, "/api/projects/26018/creative/rotation?window=30d")
    assert resp.status_code == 422
    assert rec.calls == []  # rejected before any query


# ── Rotation: window=7d ───────────────────────────────────────────────


def test_rotation_7d_window_anchors_at_as_of():
    rec = QueryRecorder()
    rec.responses = _rotation_responses([HERO_META], [])
    resp = _get(
        creative_router, rec, "/api/projects/26018/creative/rotation?window=7d"
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["window"] == "7d"
    assert body["as_of"] == "2026-06-08"

    # Cells query (call 5) and daily trend query (call 6) both carry the
    # window clause, anchored at as_of − 6 (NOT today).
    for idx in (4, 5):
        sql, params = rec.calls[idx]
        assert "date >= @window_start" in sql
        assert ("date", "window_start", date(2026, 6, 2)) in params


def test_rotation_flight_window_has_no_date_restriction():
    rec = QueryRecorder()
    rec.responses = _rotation_responses([HERO_META], [])
    resp = _get(creative_router, rec, "/api/projects/26018/creative/rotation")
    assert resp.status_code == 200, resp.text

    sql, params = rec.calls[4]
    assert "1=1" in sql
    assert not any(p[1] == "window_start" for p in params)
    # AI-120 exclusion still rides along on the frequency CTE.
    assert "platform_id NOT IN UNNEST(@rf_excluded)" in sql
    assert ("array", "rf_excluded", "STRING", ["stackadapt"]) in params


# ── Creative matrix ───────────────────────────────────────────────────


def test_matrix_cells_absent_where_variant_does_not_run():
    banner_small = dict(
        BANNER_META,
        spend=100.0, impressions=800, clicks=8, conversions=0.0, engagements=12,
    )
    rec = QueryRecorder()
    rec.responses = [
        [],  # alias-table probe
        [HERO_META, HERO_SA, banner_small],
    ]
    resp = _get(creative_router, rec, "/api/projects/26018/creative/matrix")
    assert resp.status_code == 200, resp.text
    body = resp.json()

    # Platforms ranked by spend with 0-1 shares.
    assert [p["platform_id"] for p in body["platforms"]] == ["meta", "stackadapt"]
    by_pid = {p["platform_id"]: p for p in body["platforms"]}
    assert by_pid["meta"]["spend"] == pytest.approx(6100.0)
    assert by_pid["meta"]["share"] == pytest.approx(6100 / 10_100)
    assert by_pid["stackadapt"]["share"] == pytest.approx(4000 / 10_100)

    # Rotation rank order (spend DESC).
    assert body["creatives"] == ["Hero Video", "Static Banner"]

    # Cell absent where the variant doesn't run on the platform.
    assert set(body["cells"]["Hero Video"]) == {"meta", "stackadapt"}
    assert set(body["cells"]["Static Banner"]) == {"meta"}

    hero_meta = body["cells"]["Hero Video"]["meta"]
    assert hero_meta["hook_rate"] == pytest.approx(0.3)
    assert hero_meta["completion_rate"] == pytest.approx(0.4)
    assert hero_meta["engagement_rate"] == pytest.approx(0.016)
    assert hero_meta["ctr"] == pytest.approx(0.003)
    assert hero_meta["cpm"] == pytest.approx(12.0)
    assert hero_meta["cpa"] == pytest.approx(120.0)

    # StackAdapt doesn't report engagements → None, but 3s starts → hook.
    hero_sa = body["cells"]["Hero Video"]["stackadapt"]
    assert hero_sa["engagement_rate"] is None
    assert hero_sa["hook_rate"] == pytest.approx(90_000 / 800_000)


def test_matrix_volume_guard_nulls_cell_rates_keeps_volume():
    banner_small = dict(
        BANNER_META,
        spend=100.0, impressions=800, clicks=8, conversions=0.0, engagements=12,
    )
    rec = QueryRecorder()
    rec.responses = [[], [HERO_META, banner_small]]
    resp = _get(creative_router, rec, "/api/projects/26018/creative/matrix")
    assert resp.status_code == 200, resp.text

    cell = resp.json()["cells"]["Static Banner"]["meta"]
    assert cell["ctr"] is None
    assert cell["engagement_rate"] is None
    assert cell["hook_rate"] is None
    assert cell["completion_rate"] is None
    # Spend / impressions / cost fields survive.
    assert cell["spend"] == 100.0
    assert cell["impressions"] == 800
    assert cell["cpm"] == pytest.approx(125.0)
    assert cell["cpa"] is None  # no conversions, not the guard


# ── Audiences matrix ──────────────────────────────────────────────────


AUD_MEMBERS = {
    "platform_id": "meta", "ad_set_name": "Members EN",
    "spend": 5000.0, "impressions": 400_000, "clicks": 1200,
    "conversions": 20.0, "engagements": 5000,
    "video_views": 100_000, "video_completions": 50_000,
    "video_views_3s": 80_000,
    "freq_weighted": 1_000_000.0, "freq_impressions": 400_000,
}
AUD_RETARGET = {
    "platform_id": "meta", "ad_set_name": "03 Retargeting Warm",
    "spend": 3000.0, "impressions": 200_000, "clicks": 900,
    "conversions": 30.0, "engagements": 2500,
    "video_views": 50_000, "video_completions": 20_000,
    "video_views_3s": 30_000,
    "freq_weighted": 700_000.0, "freq_impressions": 200_000,
}
AUD_PROSPECT_SA = {
    "platform_id": "stackadapt", "ad_set_name": "Prospecting Display",
    "spend": 2000.0, "impressions": 600_000, "clicks": 700,
    "conversions": 0.0, "engagements": 0,
    "video_views": 0, "video_completions": 0, "video_views_3s": 0,
    "freq_weighted": None, "freq_impressions": None,
}


def _audience_responses():
    """Canned responses in the audiences endpoint's BQ call order:
    (1) media-plan audience roles, (2) audience rollup, (3) frequency
    trend dailies, (4) alias-table probe, (5) audience × creative cells."""
    freq_trend = [
        {"date": date(2026, 6, 3), "platform_id": "meta",
         "ad_set_name": "Members EN", "frequency": 2.5},
        {"date": date(2026, 6, 1), "platform_id": "meta",
         "ad_set_name": "Members EN", "frequency": 2.0},
        {"date": date(2026, 6, 2), "platform_id": "meta",
         "ad_set_name": "Members EN", "frequency": 2.2},
    ]
    cells = [
        {"creative_variant": "Hero Video", "platform_id": "meta",
         "ad_set_name": "Members EN", "spend": 4000.0, "impressions": 300_000,
         "clicks": 1000, "conversions": 15.0, "engagements": 4000,
         "video_views": 80_000, "video_completions": 40_000,
         "video_views_3s": 60_000},
        {"creative_variant": "Hero Video", "platform_id": "stackadapt",
         "ad_set_name": "Prospecting Display", "spend": 2000.0,
         "impressions": 600_000, "clicks": 700, "conversions": 0.0,
         "engagements": 0, "video_views": 0, "video_completions": 0,
         "video_views_3s": 0},
        {"creative_variant": "Static Banner", "platform_id": "meta",
         "ad_set_name": "Members EN", "spend": 1000.0, "impressions": 100_000,
         "clicks": 200, "conversions": 5.0, "engagements": 1000,
         "video_views": 0, "video_completions": 0, "video_views_3s": 0},
    ]
    return [
        [{"audience_name": "Members EN", "audience_type": "member_list"},
         {"audience_name": "Retargeting", "audience_type": "retargeting"}],
        [AUD_MEMBERS, AUD_RETARGET, AUD_PROSPECT_SA],
        freq_trend,
        [],
        cells,
    ]


def test_audiences_role_join_exact_containment_and_none():
    rec = QueryRecorder()
    rec.responses = _audience_responses()
    resp = _get(creative_router, rec, "/api/projects/26018/audiences/matrix")
    assert resp.status_code == 200, resp.text
    rows = {a["name"]: a for a in resp.json()["audiences"]}

    # Exact (normalized) match.
    assert rows["Members EN"]["role"] == "member_list"
    # Containment match — plan name "Retargeting" inside the adset name.
    assert rows["03 Retargeting Warm"]["role"] == "retargeting"
    # No media-plan line matches → None, never a guess.
    assert rows["Prospecting Display"]["role"] is None


def test_audiences_rows_metrics_ids_and_frequency():
    rec = QueryRecorder()
    rec.responses = _audience_responses()
    resp = _get(creative_router, rec, "/api/projects/26018/audiences/matrix")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    rows = {a["name"]: a for a in body["audiences"]}

    members = rows["Members EN"]
    assert members["id"] == "members-en-meta"  # stable slug
    assert members["platform_id"] == "meta"
    assert members["frequency"] == pytest.approx(2.5)
    # Daily points sorted oldest → newest regardless of input order.
    assert members["frequency_trend"] == pytest.approx([2.0, 2.2, 2.5])
    assert members["ctr"] == pytest.approx(1200 / 400_000)
    assert members["completion_rate"] == pytest.approx(0.5)
    assert members["engagement_rate"] == pytest.approx(5000 / 400_000)
    assert members["cpa"] == pytest.approx(250.0)

    # AI-120: StackAdapt frequency stays None; its hardcoded-zero
    # engagements keep engagement_rate at None (not 0).
    prospecting = rows["Prospecting Display"]
    assert prospecting["id"] == "prospecting-display-stackadapt"
    assert prospecting["frequency"] is None
    assert prospecting["frequency_trend"] == []
    assert prospecting["engagement_rate"] is None
    assert prospecting["completion_rate"] is None
    assert prospecting["ctr"] == pytest.approx(700 / 600_000)

    # Audiences ranked by spend DESC.
    assert [a["name"] for a in body["audiences"]] == [
        "Members EN", "03 Retargeting Warm", "Prospecting Display",
    ]


def test_audiences_cells_keyed_by_audience_id_and_variant():
    rec = QueryRecorder()
    rec.responses = _audience_responses()
    resp = _get(creative_router, rec, "/api/projects/26018/audiences/matrix")
    assert resp.status_code == 200, resp.text
    body = resp.json()

    # Creatives in rotation rank order (spend DESC across cells).
    assert body["creatives"] == ["Hero Video", "Static Banner"]

    members_cells = body["cells"]["members-en-meta"]
    assert set(members_cells) == {"Hero Video", "Static Banner"}
    hero = members_cells["Hero Video"]
    assert hero["spend"] == 4000.0
    assert hero["hook_rate"] == pytest.approx(60_000 / 300_000)
    assert hero["completion_rate"] == pytest.approx(0.5)
    assert hero["ctr"] == pytest.approx(1000 / 300_000)
    assert hero["cpa"] == pytest.approx(4000 / 15)

    # Pairings that never ran are absent entirely.
    sa_cells = body["cells"]["prospecting-display-stackadapt"]
    assert set(sa_cells) == {"Hero Video"}
    # StackAdapt cell: no 3s starts in the cells data → hook None.
    assert sa_cells["Hero Video"]["hook_rate"] is None


def test_audiences_sql_keeps_ai103_and_ai120_shapes():
    """The audience rollup must reuse the /adsets reach semantics: latest
    snapshot at adset grain, campaign-grain fallback gated on the adset
    join missing, AI-120 exclusion in both CTEs."""
    rec = QueryRecorder()
    rec.responses = _audience_responses()
    resp = _get(creative_router, rec, "/api/projects/26018/audiences/matrix")
    assert resp.status_code == 200, resp.text

    sql, params = rec.calls[1]  # audience rollup
    assert "ORDER BY date DESC, loaded_at DESC" in sql
    assert "AND ad_set_id IS NOT NULL" in sql
    assert "AND ad_set_id IS NULL" in sql
    assert "AND ar.ad_set_id IS NULL" in sql
    assert sql.count("platform_id NOT IN UNNEST(@rf_excluded)") == 2
    assert ("array", "rf_excluded", "STRING", ["stackadapt"]) in params


# ── Benchmarks extension ──────────────────────────────────────────────


EXISTING_CTR_ROW = {
    "benchmark_id": "ind_xplat_awr_ctr", "scope": "canadian_political",
    "platform_id": None, "metric_name": "ctr", "metric_unit": "percentage",
    "p25": 0.003, "p50": 0.005, "p75": 0.008,
    "sample_size": None, "source": "industry_research", "notes": None,
}


def _history_campaign(name, impressions, video_views_3s, engagements, spend=500.0):
    return {
        "campaign_id": f"c-{name}", "campaign_name": name, "spend": spend,
        "impressions": impressions, "video_views_3s": video_views_3s,
        "engagements": engagements,
    }


def _benchmark_responses(table_rows, history_rows):
    """Canned responses in the benchmarks endpoint's BQ call order:
    (1) media-plan objectives, (2) distinct campaigns, (3) benchmarks
    table select, (4) PB-history per-campaign aggregates."""
    return [
        [],
        [{"campaign_name": "Awareness Video", "platform_id": "meta"}],
        table_rows,
        history_rows,
    ]


def test_benchmarks_extended_with_hook_and_engagement_quartiles():
    history = [
        _history_campaign("Awareness A", 100_000, 10_000, 1_000),
        _history_campaign("Awareness B", 100_000, 20_000, 2_000),
        _history_campaign("Awareness C", 100_000, 30_000, 3_000),
        _history_campaign("Awareness D", 100_000, 40_000, 4_000),
        # Wrong objective → excluded from both metrics.
        _history_campaign("Lead Gen Conversion", 100_000, 50_000, 9_000),
        # Under the volume guard → excluded.
        _history_campaign("Awareness Tiny", 500, 200, 100),
        # No 3s data → excluded from hook, included in engagement.
        _history_campaign("Awareness NoVid", 100_000, 0, 5_000),
    ]
    rec = QueryRecorder()
    rec.responses = _benchmark_responses([EXISTING_CTR_ROW], history)
    resp = _get(bench_router, rec, "/api/benchmarks/26018")
    assert resp.status_code == 200, resp.text
    body = resp.json()

    assert body["objective_type"] == "awareness"

    # Existing fields untouched.
    assert body["benchmarks"]["ctr"]["p50"] == 0.005
    assert body["benchmarks"]["ctr"]["source"] == "industry_research"

    # hook_rate quartiles over [0.1, 0.2, 0.3, 0.4] (inclusive method).
    hook = body["benchmarks"]["hook_rate"]
    assert hook["p25"] == pytest.approx(0.175)
    assert hook["p50"] == pytest.approx(0.25)
    assert hook["p75"] == pytest.approx(0.325)
    assert hook["sample_size"] == 4
    assert hook["source"] == "pb_history"

    # engagement_rate over [0.01 .. 0.05] (NoVid contributes here).
    eng = body["benchmarks"]["engagement_rate"]
    assert eng["p25"] == pytest.approx(0.02)
    assert eng["p50"] == pytest.approx(0.03)
    assert eng["p75"] == pytest.approx(0.04)
    assert eng["sample_size"] == 5

    # History query shape: campaign grain, parameterized spend floor.
    sql, params = rec.calls[3]
    assert "GROUP BY campaign_id" in sql
    assert ("scalar", "min_spend", "FLOAT64", 50.0) in params


def test_benchmarks_seeded_rows_win_over_computed():
    seeded_hook = dict(
        EXISTING_CTR_ROW,
        benchmark_id="xc_meta_awr_hook", metric_name="hook_rate",
        p25=0.5, p50=0.6, p75=0.7, source="cross_client",
    )
    history = [
        _history_campaign(f"Awareness {i}", 100_000, 10_000 * i, 1_000 * i)
        for i in range(1, 5)
    ]
    rec = QueryRecorder()
    rec.responses = _benchmark_responses([EXISTING_CTR_ROW, seeded_hook], history)
    resp = _get(bench_router, rec, "/api/benchmarks/26018")
    assert resp.status_code == 200, resp.text
    hook = resp.json()["benchmarks"]["hook_rate"]
    # The seeded table row is authoritative; the computed value never
    # overwrites it (setdefault semantics).
    assert hook["p50"] == 0.6
    assert hook["source"] == "cross_client"


def test_benchmarks_min_sample_gate_omits_thin_quartiles():
    history = [
        _history_campaign("Awareness A", 100_000, 10_000, 0),
        _history_campaign("Awareness B", 100_000, 20_000, 0),
    ]
    rec = QueryRecorder()
    rec.responses = _benchmark_responses([EXISTING_CTR_ROW], history)
    resp = _get(bench_router, rec, "/api/benchmarks/26018")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    # Two campaigns aren't a benchmark — no hook_rate / engagement_rate.
    assert "hook_rate" not in body["benchmarks"]
    assert "engagement_rate" not in body["benchmarks"]
    # Existing fields still intact.
    assert body["benchmarks"]["ctr"]["p50"] == 0.005
