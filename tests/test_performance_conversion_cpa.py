"""Tests for the Conversion CPA rollup (2026-06-05).

PB's default reporting KPI is CPA over conversion-objective spend only.
`total_cpa` (all spend ÷ all conversions) is the *effective* CPA — on
mixed projects it counts awareness spend in the numerator, overstating
acquisition cost (26018: $12 effective vs ~$3.50 conversion CPA).

The rollup reuses the per-campaign objective classification that the
Campaigns table renders, so the KPI tile and the table can't disagree.

Mirrors the QueryRecorder stub pattern in test_performance_clicks_all.py:
bq is patched so no SQL executes.
"""

from __future__ import annotations

import datetime
from unittest.mock import patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.routers import performance as perf_router


def _make_app() -> FastAPI:
    app = FastAPI()
    app.include_router(perf_router.router)
    return app


class QueryRecorder:
    def __init__(self) -> None:
        self.calls: list[tuple[str, list]] = []
        self.responses: list[list[dict]] = []

    def __call__(self, sql: str, params=None):
        self.calls.append((sql, list(params or [])))
        if self.responses:
            return self.responses.pop(0)
        return []


def _bq_patches(rec):
    return [
        patch.object(perf_router.bq, "run_query", side_effect=rec),
        patch.object(perf_router.bq, "string_param", lambda n, v: ("string", n, v)),
        patch.object(perf_router.bq, "date_param", lambda n, v: ("date", n, v)),
        patch.object(perf_router.bq, "array_param", lambda n, t, v: ("array", n, t, list(v))),
        patch.object(perf_router.bq, "table", lambda n: f"`dummy.{n}`"),
    ]


D1 = datetime.date(2026, 4, 27)
D2 = datetime.date(2026, 6, 2)


def _totals_row(**overrides):
    base = {
        "min_date": D1,
        "max_date": D2,
        "total_spend": 12314.0,
        "total_impressions": 2_030_000,
        "total_clicks": 7363,
        "total_clicks_all": 26012,
        "total_conversions": 1026.0,
        "total_reach": None,
        "total_frequency": None,
        "total_video_views": 100_000,
        "total_video_completions": 60_000,
        "total_vcr": 0.6,
        "total_engagements": 12736,
        "total_cpa": 12.0,
        "total_conversion_rate": 0.139,
    }
    base.update(overrides)
    return base


def _campaign_row(campaign_name, spend, conversions, platform_id="meta",
                  campaign_id="c-1"):
    return {
        "campaign_id": campaign_id,
        "campaign_name": campaign_name,
        "platform_id": platform_id,
        "spend": spend,
        "impressions": 100_000,
        "clicks": 1_000,
        "clicks_all": 2_000,
        "conversions": conversions,
        "cpm": 10.0,
        "cpc": 1.0,
        "ctr": 0.01,
        "reach": None,
        "frequency": None,
        "video_views": 0,
        "video_completions": 0,
        "vcr": None,
        "engagements": 0,
        "cpa": (spend / conversions) if conversions else None,
        "conversion_rate": 0.1,
    }


def _get_performance(rec, code="26018"):
    """Query order in the router: totals, daily, adset_daily, sum, plat,
    warn, platform, campaign, media_plan_objectives."""
    patches = _bq_patches(rec)
    for p in patches:
        p.start()
    try:
        client = TestClient(_make_app())
        return client.get(f"/api/performance/{code}")
    finally:
        for p in patches:
            p.stop()


def _responses(rec, campaign_rows, totals=None):
    rec.responses = [
        [totals or _totals_row()],   # totals_sql
        [],                          # daily_sql
        [],                          # adset_daily_sql
        [{"max_reach": None, "avg_freq": None}],  # sum_sql
        [],                          # plat_sql
        [],                          # warn_sql
        [{"platform_id": "meta", "spend": 12314.0, "impressions": 2_030_000,
          "clicks": 7363, "clicks_all": 26012, "conversions": 1026.0,
          "reach": None, "frequency": None, "video_views": 0,
          "video_completions": 0, "engagements": 0}],  # platform_sql
        campaign_rows,               # campaign_sql
        [],                          # media_plan_objectives
    ]
    return rec


def test_conversion_cpa_excludes_awareness_spend():
    """26018 shape: awareness + conversion campaigns. Conversion CPA must
    divide conversion-objective spend only; total_cpa stays effective."""
    rec = _responses(QueryRecorder(), [
        _campaign_row("26018 Pre-Bargaining Awareness", 8776.0, 0.0,
                      campaign_id="c-aw"),
        _campaign_row("26018 Retargeting Conversions", 3538.0, 1026.0,
                      campaign_id="c-conv"),
    ])
    resp = _get_performance(rec)
    assert resp.status_code == 200
    body = resp.json()
    assert body["conversion_spend"] == 3538.0
    assert body["conversion_conversions"] == 1026.0
    assert abs(body["conversion_cpa"] - 3538.0 / 1026.0) < 0.001
    # Effective CPA untouched
    assert body["total_cpa"] == 12.0


def test_conversion_cpa_none_when_no_conversion_campaigns():
    rec = _responses(QueryRecorder(), [
        _campaign_row("26018 Awareness Video Views", 8776.0, 0.0),
    ])
    resp = _get_performance(rec)
    assert resp.status_code == 200
    body = resp.json()
    assert body["conversion_cpa"] is None
    assert body["conversion_conversions"] is None


def test_conversion_cpa_none_when_zero_conversions():
    """Conversion campaigns with no conversions yet: no divide-by-zero,
    CPA stays None (frontend renders the awaiting state)."""
    rec = _responses(QueryRecorder(), [
        _campaign_row("26018 Retargeting Conversions", 3538.0, 0.0),
    ], totals=_totals_row(total_conversions=0.0, total_cpa=None))
    resp = _get_performance(rec)
    assert resp.status_code == 200
    body = resp.json()
    assert body["conversion_cpa"] is None
    assert body["conversion_spend"] == 3538.0
