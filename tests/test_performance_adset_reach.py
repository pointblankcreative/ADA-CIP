"""Tests for the AI-103 per-adset reach/frequency fix on /adsets.

Background (AI-103, Critical · related AI-023, AI-024): the `/adsets`
endpoint's reach CTE grouped fact_adset_daily by (platform_id, campaign_id)
WITHOUT ad_set_id and joined on the same two columns, broadcasting the
campaign-wide MAX(reach) / MAX(frequency) onto every adset row. EN/FR
audience pairs in the same Meta campaign showed identical reach AND
frequency, and because reach and frequency were MAXed independently the
displayed pair could be physically impossible (EN's reach with FR's
frequency, from different dates), corrupting cost_per_reach.

Fix: two CTEs replace the campaign-grain rollup —

  * adset_reach   — latest snapshot per (platform, campaign, ad_set), via
                    ROW_NUMBER ... ORDER BY date DESC, loaded_at DESC,
                    QUALIFY rn = 1, joined on ad_set_id. Reach + frequency
                    come from the SAME physical row (same snapshot date).
  * campaign_reach — fallback ONLY for platforms that report reach at
                    campaign level (Snapchat / LinkedIn → ad_set_id IS NULL
                    in fact_adset_daily). Attached only when the adset-grain
                    join found nothing (ar.ad_set_id IS NULL), so it cannot
                    re-introduce the broadcast.

Both CTEs carry the AI-120 `platform_id NOT IN UNNEST(@rf_excluded)` guard
so StackAdapt stays excluded (its Funnel reach is 1-day per-creative and
frequency is hardcoded 0.0 — see test_performance_rf_stopgap.py). Meta is
NOT in RF_EXCLUDED_PLATFORMS: its source data is verified-correct and this
fix is what makes it render correctly.

These tests mirror the QueryRecorder stub pattern in
test_performance_rf_stopgap.py: bq is patched so no SQL executes. The SQL
semantics themselves run in BigQuery, so the guarantees are pinned in two
layers:

  (a) SQL-shape assertions on the emitted query — adset grain in the
      PARTITION BY and the join, latest-snapshot ordering, same-row
      selection, NULL-ad_set_id fallback, AI-120 exclusion in BOTH CTEs;
  (b) response-passthrough assertions with canned rows — distinct EN/FR
      values per row survive to the API response, NULL-ad_set_id platform
      rows carry the campaign-grain fallback values, StackAdapt rows are
      still nulled by the Python-side guard, spend/impressions/clicks are
      untouched.

Real-world check (26018, as of 2026-06-02 snapshots): Awareness FR drops
365,405 → 112,837; Retargeting FR 59,337 → 1,866; EN rows keep their true
values; cost_per_reach corrects accordingly.
"""

from __future__ import annotations

import re
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


def _get_adsets(rec, code="26018", query=""):
    patches = _bq_patches(rec)
    for p in patches:
        p.start()
    try:
        client = TestClient(_make_app())
        return client.get(f"/api/performance/{code}/adsets{query}")
    finally:
        for p in patches:
            p.stop()


def _adset_row(platform_id, ad_set_id, ad_set_name, **overrides):
    base = {
        "ad_set_id": ad_set_id,
        "ad_set_name": ad_set_name,
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


def _normalize_ws(sql: str) -> str:
    return re.sub(r"\s+", " ", sql)


# ── (a) SQL shape — adset grain, latest snapshot, fallback, AI-120 ───


def test_adsets_sql_joins_reach_at_adset_grain():
    """AI-103 core guarantee: the adset-grain CTE partitions AND joins on
    ad_set_id, so a campaign value can never be broadcast onto its adsets.
    The old broadcast shape (GROUP BY platform_id, campaign_id over
    fact_adset_daily) must be gone."""
    rec = QueryRecorder()
    rec.responses = [[]]
    resp = _get_adsets(rec)
    # Empty result is fine for a shape test; the call still emits SQL.
    assert resp.status_code == 200, resp.text

    sql = rec.calls[0][0]
    flat = _normalize_ws(sql)

    # Latest-snapshot-per-adset CTE: adset in the partition, date DESC
    # ordering with loaded_at tiebreak, single row selected.
    assert (
        "PARTITION BY platform_id, campaign_id, ad_set_id "
        "ORDER BY date DESC, loaded_at DESC ) = 1"
    ) in flat, flat
    # Join carries ad_set_id (the column the old version dropped).
    assert "AND a.ad_set_id = ar.ad_set_id" in flat, flat
    # Reach and frequency are selected from the SAME row (no per-column
    # MAX aggregation anywhere over fact_adset_daily in this query —
    # match the projection form, not the explanatory SQL comment).
    assert "MAX(reach) AS" not in sql
    assert "MAX(frequency) AS" not in sql
    # The old campaign-grain GROUP BY over fact_adset_daily is gone.
    assert "GROUP BY platform_id, campaign_id" not in flat, flat


def test_adsets_sql_has_campaign_grain_fallback_for_null_adset_platforms():
    """Snapchat/LinkedIn write fact_adset_daily rows with ad_set_id IS NULL
    (campaign-level reach). The fallback CTE must select only those rows,
    pick the latest snapshot per campaign, and attach only when the
    adset-grain join missed (ar.ad_set_id IS NULL)."""
    rec = QueryRecorder()
    rec.responses = [[]]
    resp = _get_adsets(rec)
    assert resp.status_code == 200, resp.text

    sql = rec.calls[0][0]
    flat = _normalize_ws(sql)

    # adset CTE only sees adset-grain rows; fallback only NULL-grain rows.
    assert "AND ad_set_id IS NOT NULL" in flat, flat
    assert "AND ad_set_id IS NULL" in flat, flat
    # Fallback latest-snapshot at campaign grain.
    assert (
        "PARTITION BY platform_id, campaign_id "
        "ORDER BY date DESC, loaded_at DESC ) = 1"
    ) in flat, flat
    # Fallback join is gated on the adset-grain join having found nothing.
    assert "AND ar.ad_set_id IS NULL" in flat, flat
    # Projections prefer adset-grain, fall back to campaign-grain — and
    # cost_per_reach divides by the same COALESCEd reach.
    assert "COALESCE(ar.reach, cr.reach) AS reach" in flat
    assert "COALESCE(ar.frequency, cr.frequency) AS frequency" in flat
    assert "COALESCE(ar.reach_window, cr.reach_window) AS reach_window" in flat
    assert "NULLIF(COALESCE(ar.reach, cr.reach), 0)" in flat


def test_adsets_sql_keeps_rf_exclusion_in_both_ctes():
    """AI-120 must survive the AI-103 rewrite: BOTH the adset-grain CTE and
    the campaign-grain fallback exclude RF_EXCLUDED_PLATFORMS, and the
    @rf_excluded param still rides along. Meta must NOT be excluded — its
    source data is verified-correct."""
    rec = QueryRecorder()
    rec.responses = [[]]
    resp = _get_adsets(rec)
    assert resp.status_code == 200, resp.text

    sql, params = rec.calls[0]
    guard = "platform_id NOT IN UNNEST(@rf_excluded)"
    assert sql.count(guard) == 2, (
        f"Expected the AI-120 guard in both reach CTEs:\n{sql}"
    )
    assert ("array", "rf_excluded", "STRING", ["stackadapt"]) in params
    assert "meta" not in perf_router.RF_EXCLUDED_PLATFORMS


def test_adsets_sql_platform_filter_applies_to_both_reach_ctes():
    """?platform= must constrain fact_adset_daily in the adset CTE and the
    fallback CTE alike (the old single-CTE version had one occurrence)."""
    rec = QueryRecorder()
    rec.responses = [[]]
    resp = _get_adsets(rec, query="?platform=meta")
    assert resp.status_code == 200, resp.text

    sql, params = rec.calls[0]
    assert sql.count("AND platform_id = @platform") == 2, sql
    assert ("string", "platform", "meta") in params


# ── (b) Response passthrough — distinct EN/FR, fallback, stopgap ─────


def test_en_fr_adset_pairs_return_distinct_reach_and_frequency():
    """The AI-103 headline: two adsets in the same Meta campaign carry their
    own reach/frequency. Canned rows model what the fixed SQL returns for
    26018 (2026-06-02 snapshots) — the response must preserve per-row
    values instead of one campaign-wide pair."""
    rec = QueryRecorder()
    rec.responses = [[
        _adset_row(
            "meta", "as-aw-en", "03 Awareness EN",
            campaign_name="Awareness",
            reach=365_405, frequency=1.795348,
            spend=9_000.0,
            cost_per_reach=9_000.0 / 365_405 * 1000,
        ),
        _adset_row(
            "meta", "as-aw-fr", "04 Awareness FR",
            campaign_name="Awareness",
            reach=112_837, frequency=1.588814,
            spend=3_000.0,
            cost_per_reach=3_000.0 / 112_837 * 1000,
        ),
        _adset_row(
            "meta", "as-rt-en", "01 Conversion Retargeting EN",
            campaign_name="Retargeting",
            reach=59_337, frequency=1.767683,
            spend=4_000.0,
            cost_per_reach=4_000.0 / 59_337 * 1000,
        ),
        _adset_row(
            "meta", "as-rt-fr", "02 Conversion Retargeting FR",
            campaign_name="Retargeting",
            reach=1_866, frequency=4.951768,
            spend=1_000.0,
            cost_per_reach=1_000.0 / 1_866 * 1000,
        ),
    ]]
    resp = _get_adsets(rec)
    assert resp.status_code == 200, resp.text
    rows = {r["ad_set_id"]: r for r in resp.json()["ad_sets"]}

    # Four rows, four distinct reach values — no campaign-wide broadcast.
    assert len(rows) == 4
    assert len({r["reach"] for r in rows.values()}) == 4

    # FR rows show their OWN values, not the EN sibling's.
    assert rows["as-aw-fr"]["reach"] == 112_837
    assert rows["as-aw-fr"]["frequency"] == 1.588814
    assert rows["as-rt-fr"]["reach"] == 1_866
    assert rows["as-rt-fr"]["frequency"] == 4.951768
    # EN rows keep their true values.
    assert rows["as-aw-en"]["reach"] == 365_405
    assert rows["as-rt-en"]["reach"] == 59_337

    # cost_per_reach is computed from the row's OWN reach.
    assert rows["as-rt-fr"]["cost_per_reach"] == 1_000.0 / 1_866 * 1000
    assert rows["as-aw-en"]["cost_per_reach"] == 9_000.0 / 365_405 * 1000


def test_reach_and_frequency_come_from_same_snapshot_row():
    """AI-023 residue killer: the SQL has no per-column aggregation over
    fact_adset_daily (asserted on the emitted query), so reach + frequency
    on a row can only originate from the same physical snapshot row — the
    latest one in range (date DESC, loaded_at DESC). The canned row models
    that pairing: FR retargeting frequency is the LATEST snapshot (4.95),
    not the historical max (7.66 on 5/20)."""
    rec = QueryRecorder()
    rec.responses = [[
        _adset_row(
            "meta", "as-rt-fr", "02 Conversion Retargeting FR",
            reach=1_866, frequency=4.951768,
        ),
    ]]
    resp = _get_adsets(rec)
    assert resp.status_code == 200, resp.text

    # SQL-level guarantee of same-row pairing: reach and frequency are bare
    # column selections inside QUALIFY-filtered CTEs — never MAX'd apart.
    sql = rec.calls[0][0]
    assert "MAX(reach) AS" not in sql
    assert "MAX(frequency) AS" not in sql
    assert sql.count("QUALIFY ROW_NUMBER() OVER (") == 2

    row = resp.json()["ad_sets"][0]
    assert (row["reach"], row["frequency"]) == (1_866, 4.951768)


def test_null_adset_platform_receives_campaign_grain_fallback():
    """Snapchat reports reach at campaign grain (fact_adset_daily rows with
    NULL ad_set_id) → the fixed SQL COALESCEs the campaign_reach fallback
    onto its adset rows. Canned rows model that output; the response must
    carry the fallback values through (this is honest campaign-grain
    sharing, not the AI-103 broadcast)."""
    rec = QueryRecorder()
    rec.responses = [[
        _adset_row(
            "snapchat", "sc-as-1", "Snap audience A",
            campaign_name="Snap campaign",
            reach=42_000, frequency=3.1, reach_window="lifetime",
            spend=2_100.0, cost_per_reach=2_100.0 / 42_000 * 1000,
        ),
        _adset_row(
            "snapchat", "sc-as-2", "Snap audience B",
            campaign_name="Snap campaign",
            reach=42_000, frequency=3.1, reach_window="lifetime",
            spend=900.0, cost_per_reach=900.0 / 42_000 * 1000,
        ),
        _adset_row(
            "meta", "as-aw-fr", "04 Awareness FR",
            reach=112_837, frequency=1.588814,
        ),
    ]]
    resp = _get_adsets(rec)
    assert resp.status_code == 200, resp.text
    rows = {r["ad_set_id"]: r for r in resp.json()["ad_sets"]}

    # Both Snapchat adset rows share the campaign-level value (the platform
    # only reports campaign-grain reach) — values flow through un-nulled.
    assert rows["sc-as-1"]["reach"] == 42_000
    assert rows["sc-as-2"]["reach"] == 42_000
    assert rows["sc-as-1"]["frequency"] == 3.1
    assert rows["sc-as-1"]["reach_window"] == "lifetime"
    # Meta adset-grain row unaffected by the fallback.
    assert rows["as-aw-fr"]["reach"] == 112_837
    # Snapchat is a reach contributor in the note (not an excluded platform).
    note = resp.json()["total_reach_note"]
    assert "Snapchat" in note and "Meta" in note


def test_stackadapt_rows_still_nulled_after_rewrite():
    """AI-120 stopgap behaviour preserved end-to-end: even if the SQL leaked
    StackAdapt values (canned regression rows), the Python guard nulls
    reach / frequency / reach_window / cost_per_reach, appends the stopgap
    note, and leaves Funnel-sourced spend/impressions/clicks untouched."""
    rec = QueryRecorder()
    rec.responses = [[
        _adset_row("meta", "as-aw-en", "03 Awareness EN",
                   reach=365_405, frequency=1.795348),
        _adset_row("stackadapt", "sa-creative-1", "SA creative",
                   reach=77_777, frequency=0.0),
    ]]
    resp = _get_adsets(rec)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    rows = {r["platform_id"]: r for r in body["ad_sets"]}

    assert rows["stackadapt"]["reach"] is None
    assert rows["stackadapt"]["frequency"] is None
    assert rows["stackadapt"]["reach_window"] is None
    assert rows["stackadapt"]["cost_per_reach"] is None
    # Funnel-sourced metrics untouched on the StackAdapt row.
    assert rows["stackadapt"]["spend"] == 5000.0
    assert rows["stackadapt"]["impressions"] == 400_000
    assert rows["stackadapt"]["clicks"] == 1200
    # Meta untouched.
    assert rows["meta"]["reach"] == 365_405

    note = body["total_reach_note"]
    assert note.startswith("Reach from Meta. Not additive across audiences.")
    assert note.endswith(
        "StackAdapt reach/frequency hidden pending direct API integration."
    )


def test_spend_impressions_clicks_grain_unchanged():
    """The ad_metrics CTE (fact_digital_daily) is untouched by AI-103: same
    SUM aggregations, same GROUP BY grain. Only the reach join changed."""
    rec = QueryRecorder()
    rec.responses = [[]]
    resp = _get_adsets(rec)
    assert resp.status_code == 200, resp.text

    flat = _normalize_ws(rec.calls[0][0])
    assert "SUM(f.spend) AS spend" in flat
    assert "SUM(f.impressions) AS impressions" in flat
    assert "SUM(f.clicks) AS clicks" in flat
    assert (
        "GROUP BY f.campaign_id, f.ad_set_id, f.ad_set_name, f.platform_id"
    ) in flat
