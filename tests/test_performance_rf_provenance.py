"""Regression tests for F1 (2026-06-03): Reach/Frequency tile provenance.

The Reach / Frequency KPI values come from the fact_adset_daily rollup, but
metric_platforms (which drives the AI-026 "From X. Not reported by Y."
subtitle) was derived only from campaign-grain platform rows. In production,
Meta reach lives ONLY at adset grain (campaign-grain reach is NULL) and
StackAdapt — the only platform with campaign-grain reach — is excluded by
the AI-120 stopgap. Net effect: metric_platforms had no "reach"/"frequency"
keys at all and the tiles rendered the biggest number on the page with no
provenance subtitle.

Fix: adset-derived reach_platforms (already stopgap-guarded) are merged into
metric_platforms["reach"/"frequency"].
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
        patch.object(
            perf_router.bq, "array_param", lambda n, t, v: ("array", n, t, list(v))
        ),
        patch.object(perf_router.bq, "table", lambda name: f"`dummy.{name}`"),
    ]


D1 = datetime.date(2026, 5, 1)
D2 = datetime.date(2026, 5, 31)


def _totals_row():
    return {
        "min_date": D1, "max_date": D2,
        "total_spend": 10000.0, "total_impressions": 2_000_000,
        "total_clicks": 7000, "total_conversions": 0.0,
        "total_reach": None, "total_frequency": None,
        "total_video_views": 9000, "total_video_completions": 60,
        "total_vcr": 0.0066, "total_engagements": 12000,
        "total_cpa": None, "total_conversion_rate": 0.0,
    }


def _platform_row(platform_id, **overrides):
    """Realistic production shape: campaign-grain reach/frequency are NULL
    for Meta (adset-grain only) and NULLed for StackAdapt (AI-120)."""
    base = {
        "platform_id": platform_id,
        "spend": 5000.0, "impressions": 1_000_000, "clicks": 3000,
        "conversions": 0.0, "reach": None, "frequency": None,
        "video_views": 4500, "video_completions": 30, "engagements": 6000,
    }
    base.update(overrides)
    return base


def _campaign_row(platform_id, campaign_id):
    return {
        "campaign_id": campaign_id,
        "campaign_name": f"{platform_id} awareness campaign",
        "platform_id": platform_id,
        "spend": 5000.0, "impressions": 1_000_000, "clicks": 3000,
        "conversions": 0.0, "reach": None, "frequency": None,
        "video_views": 4500, "video_completions": 30, "engagements": 6000,
        "cpm": 5.0, "cpc": 1.6, "ctr": 0.003,
    }


def _get_performance(rec, code="26018"):
    patches = _bq_patches(rec)
    for p in patches:
        p.start()
    try:
        client = TestClient(_make_app())
        return client.get(f"/api/performance/{code}")
    finally:
        for p in patches:
            p.stop()


def _realistic_responses(rec):
    """Meta reach exists ONLY in the adset rollup; campaign grain all-NULL."""
    rec.responses = [
        [_totals_row()],                              # totals_sql
        [],                                           # daily_sql
        [],                                           # adset_daily_sql
        [{"max_reach": 365_405, "avg_freq": 2.8}],    # sum_sql
        [{"platform_id": "meta"}],                    # plat_sql (adset-derived)
        [],                                           # warn_sql
        [
            _platform_row("meta"),
            _platform_row("stackadapt"),
            _platform_row("google_ads", video_views=None,
                          video_completions=None, engagements=None),
        ],                                            # platform_sql
        [
            _campaign_row("meta", "c-meta"),
            _campaign_row("stackadapt", "c-sa"),
        ],                                            # campaign_sql
        [],                                           # media_plan_objectives
    ]
    return rec


def test_reach_frequency_provenance_comes_from_adset_rollup():
    """With campaign-grain reach NULL everywhere (production reality),
    metric_platforms must still attribute reach/frequency to the adset
    source platform so the AI-026 subtitle renders."""
    rec = _realistic_responses(QueryRecorder())
    resp = _get_performance(rec)
    assert resp.status_code == 200, resp.text
    body = resp.json()

    mp = body["metric_platforms"]
    assert mp.get("reach") == ["Meta"], mp
    assert mp.get("frequency") == ["Meta"], mp
    assert "reach" in body["available_metrics"]
    assert "frequency" in body["available_metrics"]
    # The KPI value itself still flows from the adset rollup.
    assert body["total_reach_adset"] == 365_405
    # Excluded platform never sneaks in.
    assert "StackAdapt" not in mp.get("reach", [])
    assert "StackAdapt" not in mp.get("frequency", [])


def test_no_adset_reach_means_no_provenance_claim():
    """If nothing reports adset reach (and campaign grain is NULL), the
    metric stays unlisted — no invented provenance."""
    rec = QueryRecorder()
    rec.responses = [
        [_totals_row()],
        [],
        [],
        [{"max_reach": None, "avg_freq": None}],
        [],                                           # plat_sql: empty
        [],
        [_platform_row("google_ads", video_views=None,
                       video_completions=None, engagements=None)],
        [_campaign_row("google_ads", "c-ga")],
        [],
    ]
    resp = _get_performance(rec)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert "reach" not in body["metric_platforms"], body["metric_platforms"]
    assert "frequency" not in body["metric_platforms"]


def test_adset_and_campaign_grain_provenance_merge_dedupes():
    """If a platform reports reach at BOTH grains it appears once."""
    rec = QueryRecorder()
    rec.responses = [
        [_totals_row()],
        [],
        [],
        [{"max_reach": 1000, "avg_freq": 1.5}],
        [{"platform_id": "meta"}],
        [],
        [_platform_row("meta", reach=900, frequency=1.4)],
        [_campaign_row("meta", "c-meta")],
        [],
    ]
    resp = _get_performance(rec)
    assert resp.status_code == 200, resp.text
    mp = resp.json()["metric_platforms"]
    assert mp.get("reach") == ["Meta"], mp
    assert mp.get("frequency") == ["Meta"], mp
