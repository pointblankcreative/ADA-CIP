"""Tests for the AI-120 Option D StackAdapt reach/frequency stopgap.

Background (AI-111 + AI-112): StackAdapt "reach" via Funnel.io is a 1-day
per-creative reach field, not deduplicated multi-day reach (wrong by 7-10x),
and StackAdapt frequency is hardcoded 0.0 upstream. Decision (2026-05-20,
AI-120): v1 hides StackAdapt reach + frequency in the UI and excludes
StackAdapt from every reach/frequency aggregate, until the post-launch
StackAdapt direct-API supplement restores them. Funnel stays source of truth
for spend / impressions / clicks — those must NOT change.

These tests mirror the stub pattern in test_projects_router_pacing.py: bq is
patched so no SQL executes. We assert:

  (a) the emitted SQL excludes RF_EXCLUDED_PLATFORMS from every R&F
      aggregate (totals, daily, adset rollups, platform/campaign breakdowns,
      high-frequency warning),
  (b) metric_platforms never lists StackAdapt for reach / frequency (the
      AI-026 subtitle then says "Not reported by StackAdapt." automatically),
  (c) the stopgap sentence is appended to reach_note when StackAdapt is
      active, and existing note text is preserved,
  (d) per-row drilldowns (campaigns, by_platform, ad_sets) return
      reach=None / frequency=None for StackAdapt rows so the frontend
      renders an em-dash (AI-029 unsupported-platform pattern),
  (e) other platforms (Meta etc.) are completely unaffected, and
      StackAdapt spend / impressions / clicks still flow through.

When the direct-API supplement ships and RF_EXCLUDED_PLATFORMS is emptied,
delete this file (or flip the assertions) alongside that change.
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


def _mixed_project_responses(rec):
    """Meta + StackAdapt project. The SQL stub can't apply the SQL-side
    exclusion, so canned rows simulate what BigQuery returns: StackAdapt
    rows carry NULL reach/frequency in fact_digital_daily rollups (the
    IF(...IN UNNEST(@rf_excluded), NULL, ...) projection) and are absent
    from fact_adset_daily rollups (the NOT IN UNNEST WHERE guard).

    The per-platform / per-campaign rows intentionally carry NON-NULL
    R&F for StackAdapt so the tests prove the Python-side guard nulls
    them even if the SQL-side exclusion were to regress."""
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
        [],                                                     # media_plan_objectives
    ]
    return rec


# ── Constant / wiring ────────────────────────────────────────────────


def test_rf_excluded_platforms_constant():
    """The stopgap is keyed on this set; the future direct-API supplement
    removes the platform from it (one-line revert)."""
    assert perf_router.RF_EXCLUDED_PLATFORMS == {"stackadapt"}
    assert "StackAdapt" in perf_router.RF_EXCLUDED_NOTE
    assert "direct API" in perf_router.RF_EXCLUDED_NOTE


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


def test_metric_platforms_excludes_stackadapt_for_reach_and_frequency():
    """metric_platforms drives the AI-026 tile subtitle: with StackAdapt
    absent from the reach/frequency lists, the frontend renders
    "From Meta. Not reported by StackAdapt." automatically."""
    rec = _mixed_project_responses(QueryRecorder())
    # Make the regression case explicit: even if SQL returned non-null R&F
    # for stackadapt (responses already do for platform_sql), the Python
    # guard must keep it out of the lists.
    resp = _get_performance(rec)
    assert resp.status_code == 200, resp.text
    body = resp.json()

    mp = body["metric_platforms"]
    assert "StackAdapt" not in mp.get("reach", []), mp
    assert "StackAdapt" not in mp.get("frequency", []), mp
    assert "Meta" in mp.get("reach", []), mp
    # Non-R&F metrics keep listing StackAdapt — only R&F is hidden.
    assert "StackAdapt" in mp.get("video_views", []), mp


# ── reach_note ──────────────────────────────────────────────────────


def test_reach_note_appended_when_stackadapt_active():
    """Existing note text stays; the stopgap sentence is appended."""
    rec = _mixed_project_responses(QueryRecorder())
    resp = _get_performance(rec)
    assert resp.status_code == 200, resp.text
    body = resp.json()

    note = body["reach_note"]
    assert note is not None
    assert note.startswith("Reach from Meta."), note
    assert note.endswith(
        "StackAdapt reach/frequency hidden pending direct API integration."
    ), note
    # StackAdapt never appears as a reach-contributing platform.
    assert "stackadapt" not in body["reach_platforms"]


def test_reach_note_is_only_stopgap_sentence_when_no_other_reach_platform():
    """StackAdapt-only project: no contributing platforms, so the note is
    just the explanation for why R&F is missing."""
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
        [],                                                     # media_plan_objectives
    ]
    resp = _get_performance(rec)
    assert resp.status_code == 200, resp.text
    body = resp.json()

    assert body["reach_note"] == (
        "StackAdapt reach/frequency hidden pending direct API integration."
    )
    assert body["reach_platforms"] == []
    # The KPI rollups carry no phantom StackAdapt reach.
    assert body["total_reach"] is None
    assert body["total_frequency"] is None
    assert body["total_reach_adset"] is None
    assert body["avg_frequency_adset"] is None
    # metric_platforms must not declare reach/frequency at all.
    assert "reach" not in body["metric_platforms"]
    assert "frequency" not in body["metric_platforms"]
    assert "reach" not in body["available_metrics"]
    assert "frequency" not in body["available_metrics"]


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


def test_stackadapt_rows_nulled_in_breakdowns_other_platforms_untouched():
    """by_platform + campaigns: StackAdapt rows get reach=None /
    frequency=None (em-dash via AI-029 in the UI) even when the canned SQL
    rows carry values; Meta keeps its numbers. StackAdapt spend /
    impressions / clicks flow through untouched."""
    rec = _mixed_project_responses(QueryRecorder())
    resp = _get_performance(rec)
    assert resp.status_code == 200, resp.text
    body = resp.json()

    plats = {p["platform_id"]: p for p in body["by_platform"]}
    assert plats["stackadapt"]["reach"] is None
    assert plats["stackadapt"]["frequency"] is None
    assert plats["meta"]["reach"] == 200_000
    assert plats["meta"]["frequency"] == 2.0
    # Funnel-sourced metrics for StackAdapt are NOT touched.
    assert plats["stackadapt"]["spend"] == 10000.0
    assert plats["stackadapt"]["impressions"] == 1_000_000
    assert plats["stackadapt"]["clicks"] == 3000

    camps = {c["platform_id"]: c for c in body["campaigns"]}
    assert camps["stackadapt"]["reach"] is None
    assert camps["stackadapt"]["frequency"] is None
    assert camps["meta"]["reach"] == 200_000
    assert camps["meta"]["frequency"] == 2.0
    assert camps["stackadapt"]["spend"] == 10000.0

    # KPI rollups (fact_adset_daily, rf-guarded) flow through unchanged.
    assert body["total_reach_adset"] == 500_000
    assert body["avg_frequency_adset"] == 2.1


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

    # Note: existing text preserved, stopgap sentence appended.
    note = body["total_reach_note"]
    assert note is not None
    assert note.startswith("Reach from Meta. Not additive across audiences."), note
    assert note.endswith(
        "StackAdapt reach/frequency hidden pending direct API integration."
    ), note

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
