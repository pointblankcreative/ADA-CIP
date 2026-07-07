"""Meta ad-funnel metrics on the performance drilldowns — ADA 1215990005805822.

The ingestion + schema work for the full Meta ad-level metric set already
shipped: `outbound_clicks` / `landing_page_views` live in fact_digital_daily,
are populated by the daily transform (Meta + Pinterest for outbound clicks,
Meta-only for landing-page views; every other platform casts them NULL), and
are already consumed by the diagnostics engine. The only genuine gap was the
read surface — the per-ad view the ticket complains about never selected or
returned them.

This pins that surface. It mirrors the fuller QueryRecorder passthrough
pattern of the AI-102 `test_performance_clicks_all.py` rollout — bq is patched
so no SQL executes — and the stub is kept SELF-CONTAINED here (no cross-import
from the drifted top-level tests/ tree). We assert, at ad / creative / adset
grain:

  (a) the SQL SUMs both new columns (and the creative rollup carries them in
      BOTH the inner ad_agg CTE and the outer rollup), while the sibling
      SUM(f.clicks_all) stays untouched;
  (b) canned non-null values flow through to the response rows unchanged;
  (c) a row missing the keys entirely (non-reporting / pre-backfill) defaults
      to None — an honest em-dash in the UI — instead of raising.
"""

from __future__ import annotations

import datetime
from unittest.mock import patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.routers import performance as perf_router


# ── Self-contained stub (mirrors test_performance_clicks_all.py) ──────


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


D1 = datetime.date(2026, 4, 27)
D2 = datetime.date(2026, 6, 2)


# ── Canned rows (full field set so the models serialize cleanly) ──────


def _ad_row(**overrides):
    base = {
        "ad_id": "ad-1",
        "ad_name": "Hero Video EN",
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
        "outbound_clicks": 980,
        "landing_page_views": 640,
        "cpm": 10.0,
        "cpc": 0.81,
        "ctr": 0.0124,
        "vcr": 0.6,
        "engagement_rate": 0.007,
    }
    base.update(overrides)
    return base


def _adset_row(**overrides):
    base = {
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
        "outbound_clicks": 3_500,
        "landing_page_views": 2_100,
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


def _creative_row(**overrides):
    base = {
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
        "outbound_clicks": 1_450,
        "landing_page_views": 910,
        "cpm": 10.0,
        "cpc": 2.22,
        "ctr": 0.0045,
        "vcr": 0.6,
        "engagement_rate": 0.0025,
    }
    base.update(overrides)
    return base


# ── SQL shape: both columns summed, clicks_all untouched ─────────────


def test_ads_sql_sums_outbound_clicks_and_landing_page_views():
    rec = QueryRecorder()
    rec.responses = [[]]
    resp = _get(rec, "/api/performance/26018/ads")
    assert resp.status_code == 200, resp.text
    sql = rec.calls[0][0]
    assert "SUM(f.outbound_clicks) AS outbound_clicks" in sql
    assert "SUM(f.landing_page_views) AS landing_page_views" in sql
    # Sibling clicks_all aggregate must remain untouched.
    assert "SUM(f.clicks_all) AS clicks_all" in sql


def test_adsets_sql_sums_outbound_clicks_and_landing_page_views():
    rec = QueryRecorder()
    rec.responses = [[]]
    resp = _get(rec, "/api/performance/26018/adsets")
    assert resp.status_code == 200, resp.text
    sql = rec.calls[0][0]
    # Aggregated in the ad_metrics CTE and carried through the outer SELECT.
    assert "SUM(f.outbound_clicks) AS outbound_clicks" in sql
    assert "SUM(f.landing_page_views) AS landing_page_views" in sql
    assert "a.outbound_clicks" in sql
    assert "a.landing_page_views" in sql
    assert "SUM(f.clicks_all) AS clicks_all" in sql


def test_creatives_sql_sums_outbound_clicks_at_inner_and_outer_grain():
    rec = QueryRecorder()
    # First call is the alias-table probe, second the real query.
    rec.responses = [[], []]
    resp = _get(rec, "/api/performance/26018/creatives")
    assert resp.status_code == 200, resp.text
    sql = rec.calls[1][0]
    # Inner ad_agg CTE and the outer rollup both carry the two columns.
    assert "SUM(f.outbound_clicks) AS outbound_clicks" in sql
    assert "SUM(outbound_clicks) AS outbound_clicks" in sql
    assert "SUM(f.landing_page_views) AS landing_page_views" in sql
    assert "SUM(landing_page_views) AS landing_page_views" in sql
    assert "SUM(f.clicks_all) AS clicks_all" in sql


# ── Passthrough: canned non-null values reach the response ───────────


def test_ads_rows_carry_outbound_clicks_and_landing_page_views():
    rec = QueryRecorder()
    rec.responses = [[_ad_row(outbound_clicks=980, landing_page_views=640)]]
    resp = _get(rec, "/api/performance/26018/ads")
    assert resp.status_code == 200, resp.text
    ad = resp.json()["ads"][0]
    assert ad["outbound_clicks"] == 980
    assert ad["landing_page_views"] == 640


def test_adsets_rows_carry_outbound_clicks_and_landing_page_views():
    rec = QueryRecorder()
    rec.responses = [[_adset_row(outbound_clicks=3_500, landing_page_views=2_100)]]
    resp = _get(rec, "/api/performance/26018/adsets")
    assert resp.status_code == 200, resp.text
    row = resp.json()["ad_sets"][0]
    assert row["outbound_clicks"] == 3_500
    assert row["landing_page_views"] == 2_100


def test_creatives_rows_carry_outbound_clicks_and_landing_page_views():
    rec = QueryRecorder()
    rec.responses = [
        [],  # alias-table probe
        [_creative_row(outbound_clicks=1_450, landing_page_views=910)],
    ]
    resp = _get(rec, "/api/performance/26018/creatives")
    assert resp.status_code == 200, resp.text
    cre = resp.json()["creatives"][0]
    assert cre["outbound_clicks"] == 1_450
    assert cre["landing_page_views"] == 910


# ── Honest nulls: explicit None and missing-key both → None ──────────


def test_non_reporting_platform_values_pass_through_as_none():
    """StackAdapt etc. cast both columns NULL in the transform → the row
    carries explicit None, which must survive to the response (em-dash in
    the UI), not collapse to 0."""
    rec = QueryRecorder()
    rec.responses = [[
        _ad_row(
            ad_id="sa-1",
            platform_id="stackadapt",
            outbound_clicks=None,
            landing_page_views=None,
        )
    ]]
    resp = _get(rec, "/api/performance/26018/ads")
    assert resp.status_code == 200, resp.text
    ad = resp.json()["ads"][0]
    assert ad["outbound_clicks"] is None
    assert ad["landing_page_views"] is None


def test_missing_keys_default_to_none_at_each_grain():
    """Pre-backfill / legacy fixtures may lack the keys entirely — the
    int | None = None model default must apply instead of raising."""
    # /ads
    ad = _ad_row()
    ad.pop("outbound_clicks")
    ad.pop("landing_page_views")
    rec = QueryRecorder()
    rec.responses = [[ad]]
    resp = _get(rec, "/api/performance/26018/ads")
    assert resp.status_code == 200, resp.text
    a = resp.json()["ads"][0]
    assert a["outbound_clicks"] is None
    assert a["landing_page_views"] is None

    # /adsets
    adset = _adset_row()
    adset.pop("outbound_clicks")
    adset.pop("landing_page_views")
    rec = QueryRecorder()
    rec.responses = [[adset]]
    resp = _get(rec, "/api/performance/26018/adsets")
    assert resp.status_code == 200, resp.text
    s = resp.json()["ad_sets"][0]
    assert s["outbound_clicks"] is None
    assert s["landing_page_views"] is None

    # /creatives
    creative = _creative_row()
    creative.pop("outbound_clicks")
    creative.pop("landing_page_views")
    rec = QueryRecorder()
    rec.responses = [[], [creative]]
    resp = _get(rec, "/api/performance/26018/creatives")
    assert resp.status_code == 200, resp.text
    c = resp.json()["creatives"][0]
    assert c["outbound_clicks"] is None
    assert c["landing_page_views"] is None
