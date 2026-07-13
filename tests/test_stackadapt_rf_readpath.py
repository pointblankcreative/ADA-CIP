"""Read-path tests for the StackAdapt reach/frequency direct feed (Stage 2).

Asana 1215990005858637. Stage 1 (PR #112) landed the ETL +
`cip_stackadapt.stackadapt_reach_frequency` table. Stage 2 wires the READ path:

  * backend/routers/performance.py — a Python-side SA-direct FILL layer that
    replaces Funnel's excluded StackAdapt reach/frequency with the real
    current-calendar-month numbers from the direct feed (individual primary,
    household additive). Funnel stays source of truth for spend/impressions/
    clicks.
  * backend/services/diagnostics/engine.py — `_query_platform_metrics_by_type`
    overrides the StackAdapt entries in `adset_bucket` with the direct feed so
    D1/D2/D3/D4 stop scoring StackAdapt-only campaigns on Funnel's wrong reach.

These tests stub `bq` so no SQL executes; canned rows model what BigQuery
returns. The Funnel-side SQL exclusion (RF_EXCLUDED_PLATFORMS) is unchanged and
covered by tests/test_performance_rf_stopgap.py — here we prove the FILL.
"""

from __future__ import annotations

import datetime
from unittest.mock import MagicMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.routers import performance as perf_router


# ── performance endpoint harness (mirrors test_performance_rf_stopgap) ──


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


D1 = datetime.date(2026, 7, 1)
D2 = datetime.date(2026, 7, 13)


def _totals_row(**overrides):
    base = {
        "min_date": D1, "max_date": D2,
        "total_spend": 20000.0, "total_impressions": 3_000_000,
        "total_clicks": 9000, "total_conversions": 0.0,
        "total_reach": None, "total_frequency": None,
        "total_video_views": 200_000, "total_video_completions": 120_000,
        "total_vcr": 0.6, "total_engagements": 4000,
        "total_cpa": None, "total_conversion_rate": 0.0,
    }
    base.update(overrides)
    return base


def _platform_row(platform_id, **overrides):
    base = {
        "platform_id": platform_id,
        "spend": 20000.0, "impressions": 3_000_000, "clicks": 9000,
        "conversions": 0.0, "reach": None, "frequency": None,
        "video_views": 200_000, "video_completions": 120_000, "engagements": 4000,
    }
    base.update(overrides)
    return base


def _campaign_row(platform_id, campaign_id, **overrides):
    base = {
        "campaign_id": campaign_id,
        "campaign_name": f"{platform_id} {campaign_id}",
        "platform_id": platform_id,
        "spend": 20000.0, "impressions": 3_000_000, "clicks": 9000,
        "conversions": 0.0, "cpm": 6.6, "cpc": 2.2, "ctr": 0.003,
        "reach": None, "frequency": None,
        "video_views": 200_000, "video_completions": 120_000,
        "vcr": 0.6, "engagements": 4000, "cpa": None, "conversion_rate": 0.0,
    }
    base.update(overrides)
    return base


def _get_performance(rec, code="26022"):
    patches = _bq_patches(rec)
    for p in patches:
        p.start()
    try:
        return TestClient(_make_app()).get(f"/api/performance/{code}")
    finally:
        for p in patches:
            p.stop()


# ── SA-direct FILL ──────────────────────────────────────────────────


def test_sa_direct_fills_individual_and_household_when_matched():
    """A StackAdapt-only project: the SA campaign/platform rows carry the
    direct feed's individual + household reach/frequency, and the headline
    Reach/Frequency KPI lights up (total_reach_adset from the direct feed)."""
    rec = QueryRecorder()
    rec.responses = [
        [_totals_row()],                                     # totals_sql
        [],                                                  # daily_sql
        [],                                                  # adset_daily_sql
        [{"max_reach": None, "avg_freq": None}],             # sum_sql
        [],                                                  # plat_sql (SA-guarded)
        [],                                                  # warn_sql
        [_platform_row("stackadapt")],                       # platform_sql
        [_campaign_row("stackadapt", "3272754")],            # campaign_sql
        [{                                                   # sa_direct_sql
            "campaign_id": "3272754",
            "reach": 26_800, "frequency": 3.2,
            "reach_household": 8_600, "frequency_household": 2.0,
        }],
        [],                                                  # media_plan_objectives
    ]
    resp = _get_performance(rec)
    assert resp.status_code == 200, resp.text
    body = resp.json()

    plat = {p["platform_id"]: p for p in body["by_platform"]}["stackadapt"]
    assert plat["reach"] == 26_800
    assert plat["frequency"] == pytest.approx(3.2)
    assert plat["reach_household"] == 8_600
    assert plat["frequency_household"] == pytest.approx(2.0)

    camp = {c["campaign_id"]: c for c in body["campaigns"]}["3272754"]
    assert camp["reach"] == 26_800
    assert camp["reach_household"] == 8_600

    # Headline KPI now populated from the direct feed (Funnel total stays NULL).
    assert body["total_reach"] is None
    assert body["total_reach_adset"] == 26_800
    assert body["avg_frequency_adset"] == pytest.approx(3.2)
    assert body["total_reach_household"] == 8_600
    assert body["avg_frequency_household"] == pytest.approx(2.0)
    assert body["reach_note"] == perf_router.SA_DIRECT_NOTE

    # Individual reach is the primary `reach` — household is strictly additive.
    assert plat["reach"] > (plat["reach_household"] or 0)


def test_sa_campaign_without_direct_match_stays_none():
    """Honest: a StackAdapt campaign with no current-month row in the direct
    feed keeps reach=None (em-dash), and the note falls back to the 'not yet
    synced' wording — no fabricated numbers."""
    rec = QueryRecorder()
    rec.responses = [
        [_totals_row()],                                     # totals_sql
        [],                                                  # daily_sql
        [],                                                  # adset_daily_sql
        [{"max_reach": None, "avg_freq": None}],             # sum_sql
        [],                                                  # plat_sql
        [],                                                  # warn_sql
        [_platform_row("stackadapt")],                       # platform_sql
        [_campaign_row("stackadapt", "unsynced-1")],         # campaign_sql
        [],                                                  # sa_direct_sql (no match)
        [],                                                  # media_plan_objectives
    ]
    resp = _get_performance(rec)
    assert resp.status_code == 200, resp.text
    body = resp.json()

    plat = {p["platform_id"]: p for p in body["by_platform"]}["stackadapt"]
    assert plat["reach"] is None
    assert plat["frequency"] is None
    assert plat["reach_household"] is None

    camp = {c["campaign_id"]: c for c in body["campaigns"]}["unsynced-1"]
    assert camp["reach"] is None
    assert camp["reach_household"] is None

    assert body["total_reach_adset"] is None
    assert body["total_reach_household"] is None
    assert body["reach_note"] == perf_router.RF_EXCLUDED_NOTE
    # Funnel-sourced metrics still flow.
    assert plat["spend"] == 20000.0
    assert plat["impressions"] == 3_000_000


# ── diagnostics engine SA override ──────────────────────────────────


def _sa_daily_row(**overrides):
    """fact_digital_daily row shape for _query_platform_metrics_by_type."""
    row = {
        "platform_id": "stackadapt",
        "campaign_objective": "REACH",
        "campaign_name": "26022 CATIE Video Reach",
        "spend": 12000.0, "impressions": 2_000_000, "clicks": 400,
        "conversions": 0.0,
    }
    for f in ("video_views_3s", "thruplay", "video_q25", "video_q50",
              "video_q75", "video_q100", "post_engagement", "post_reactions",
              "post_comments", "outbound_clicks", "landing_page_views",
              "registrations", "leads", "on_platform_leads", "contacts",
              "donations", "viewability_measured", "viewability_viewed"):
        row.setdefault(f, 0)
    row.update(overrides)
    return row


def test_diagnostics_overrides_stackadapt_reach_from_direct_feed():
    """The engine must score StackAdapt on the direct feed's dedup reach, not
    Funnel's inflated 1-day per-creative reach. fact_adset_daily returns a bogus
    250k reach + 0.0 frequency; the SA-direct feed returns 26.8k / 3.2. The
    resulting PlatformMetrics for stackadapt must use the direct numbers."""
    from backend.services.diagnostics import engine
    from backend.services.diagnostics.models import CampaignType

    def router(sql, params=None):
        if "stackadapt_reach_frequency" in sql:
            return [{
                "campaign_name": "26022 CATIE Video Reach",
                "reach": 26_800, "frequency": 3.2,
            }]
        if "GROUP BY platform_id, campaign_name" in sql:  # adset rollup (Funnel)
            return [{
                "platform_id": "stackadapt",
                "campaign_name": "26022 CATIE Video Reach",
                "reach_window": "1d",
                "reach": 250_000,      # Funnel's inflated garbage
                "frequency": 0.0,      # Funnel hardcodes ~0
                "adset_impressions": 2_000_000,
            }]
        if "GROUP BY platform_id, campaign_objective" in sql:  # daily rollup
            return [_sa_daily_row()]
        raise AssertionError(f"Unmocked SQL: {sql[:120]}")

    with patch("backend.services.diagnostics.engine.bq") as mock_bq:
        mock_bq.table.side_effect = lambda n: f"`point-blank-ada.cip.{n}`"
        mock_bq.string_param = MagicMock(return_value=MagicMock())
        mock_bq.date_param = MagicMock(return_value=MagicMock())
        mock_bq.run_query.side_effect = router
        result = engine._query_platform_metrics_by_type(
            "26022", datetime.date(2026, 7, 1), datetime.date(2026, 7, 13)
        )

    sa = [
        pm for pms in result.values() for pm in pms
        if pm.platform_id == "stackadapt"
    ]
    assert sa, "expected a StackAdapt PlatformMetrics row"
    assert len(sa) == 1
    assert sa[0].reach == 26_800          # direct feed, not 250_000
    assert sa[0].reach != 250_000
    assert sa[0].frequency == pytest.approx(3.2)  # direct feed, not 0.0
    # Spend/impressions stay Funnel-sourced from the daily rollup.
    assert sa[0].spend == pytest.approx(12000.0)
    assert sa[0].impressions == 2_000_000


def test_diagnostics_keeps_funnel_reach_when_direct_feed_empty():
    """If the direct feed returns nothing (not synced), the engine leaves the
    Funnel-derived adset_bucket untouched — no override, graceful degradation."""
    from backend.services.diagnostics import engine

    def router(sql, params=None):
        if "stackadapt_reach_frequency" in sql:
            return []  # nothing synced
        if "GROUP BY platform_id, campaign_name" in sql:
            return [{
                "platform_id": "stackadapt",
                "campaign_name": "26022 CATIE Video Reach",
                "reach_window": "1d", "reach": 250_000,
                "frequency": 1.5, "adset_impressions": 2_000_000,
            }]
        if "GROUP BY platform_id, campaign_objective" in sql:
            return [_sa_daily_row()]
        raise AssertionError(f"Unmocked SQL: {sql[:120]}")

    with patch("backend.services.diagnostics.engine.bq") as mock_bq:
        mock_bq.table.side_effect = lambda n: f"`point-blank-ada.cip.{n}`"
        mock_bq.string_param = MagicMock(return_value=MagicMock())
        mock_bq.date_param = MagicMock(return_value=MagicMock())
        mock_bq.run_query.side_effect = router
        result = engine._query_platform_metrics_by_type(
            "26022", datetime.date(2026, 7, 1), datetime.date(2026, 7, 13)
        )

    sa = [
        pm for pms in result.values() for pm in pms
        if pm.platform_id == "stackadapt"
    ]
    assert sa
    # No direct data → Funnel adset reach is preserved (not zeroed).
    assert sa[0].reach == 250_000
