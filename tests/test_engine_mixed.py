"""Integration tests for the mixed-campaign diagnostic engine.

Covers Build Plan §12: per-line classification, per-type data partitioning,
and dual DiagnosticOutput return for mixed projects.

Canonical fixtures model OSSTF 25042 — 3 persuasion lines (engagement F1,
awareness F2, CTV reach/frequency) plus 1 conversion line (retargeting).
That was the real-world campaign that surfaced the single-type bug.

The tests mock `bq.run_query` (dispatching by SQL fingerprint) and mock
`compute_persuasion_health` / `compute_conversion_health` so we assert the
CampaignData handed to each pillar computer, not the signal math itself.
The signal math is already covered by test_diagnostics_*.py.
"""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import pytest

from backend.services.diagnostics.models import (
    CampaignData,
    CampaignType,
    DiagnosticOutput,
    StatusBand,
)


# ── helpers ─────────────────────────────────────────────────────────


def _media_plan_row(
    line_id: str,
    objective: str | None,
    budget: float = 5000,
    impressions: int = 500_000,
    platform_id: str = "facebook",
) -> dict:
    """Shape matching _query_media_plan's SELECT aliases."""
    return {
        "line_id": line_id,
        "platform_id": platform_id,
        "channel_category": "social",
        "audience_name": "Prospects",
        "audience_type": "prospecting",
        "planned_budget": budget,
        "planned_impressions": impressions,
        "frequency_cap": "3/7d",
        "flight_start": date(2026, 4, 1),
        "flight_end": date(2026, 4, 30),
        "ffs_score": 3.5,
        "objective": objective,
    }


def _fact_digital_row(platform_id: str, objective: str | None, spend: float = 1000,
                     impressions: int = 100_000, clicks: int = 500,
                     conversions: float = 0,
                     campaign_name: str | None = None) -> dict:
    """Shape matching fact_digital_daily SELECT in _query_platform_metrics_by_type.

    `campaign_name` is optional — added so tests can exercise the NULL-objective
    fallback path where classify_objective_string falls back to campaign_name
    keyword matching.
    """
    row = {
        "platform_id": platform_id,
        "campaign_objective": objective,
        "campaign_name": campaign_name,
        "spend": spend,
        "impressions": impressions,
        "clicks": clicks,
        "conversions": conversions,
    }
    # Zero-fill all the numeric fields the query selects
    for f in ("video_views_3s", "thruplay", "video_q25", "video_q50",
              "video_q75", "video_q100", "post_engagement", "post_reactions",
              "post_comments", "outbound_clicks", "landing_page_views",
              "registrations", "leads", "on_platform_leads", "contacts",
              "donations", "viewability_measured", "viewability_viewed"):
        row.setdefault(f, 0)
    return row


def _fact_adset_row(platform_id: str, campaign_name: str, reach: int = 50_000,
                    frequency: float = 2.5, impressions: int = 100_000,
                    reach_window: str | None = None) -> dict:
    return {
        "platform_id": platform_id,
        "campaign_name": campaign_name,
        "reach_window": reach_window,
        "reach": reach,
        "frequency": frequency,
        "adset_impressions": impressions,
    }


class _QueryRouter:
    """Dispatch a mocked `bq.run_query` call by looking at the SQL body.

    We key off fragments in each distinct query (FROM clause or GROUP BY
    structure) rather than call count — easier to read and survives order
    changes. Raises if an unexpected query shape is requested so tests fail
    loudly on engine drift.
    """

    def __init__(self, responses: dict[str, list[dict]]):
        # fragment → rows
        self.responses = responses
        self.calls: list[str] = []

    def __call__(self, sql, params=None):
        self.calls.append(sql)
        for fragment, rows in self.responses.items():
            if fragment in sql:
                return rows
        raise AssertionError(
            f"Unmocked SQL — no fragment matched. Snippet:\n{sql[:300]}..."
        )


# ── Engine context manager ──────────────────────────────────────────


class _EngineContext:
    """Bundle all the patches the engine needs into one context.

    Returns captured CampaignData objects (one per campaign type that ran),
    the query router, and the mocked store/alert functions.
    """

    def __init__(self, media_plan_rows, digital_rows, adset_rows,
                 ga4_url_rows=None, ga4_metric_rows=None, pacing_rows=None):
        self.router = _QueryRouter({
            # _query_media_plan
            "FROM (\n            SELECT *,\n                   ROW_NUMBER()": media_plan_rows,
            # _query_platform_metrics_by_type — digital
            "GROUP BY platform_id, campaign_objective": digital_rows,
            # _query_platform_metrics_by_type — adset
            "GROUP BY platform_id, campaign_name": adset_rows,
            # _query_daily_metrics_by_type
            "GROUP BY date, platform_id, campaign_objective": [],
            # _query_ga4 — URL lookup
            "FROM `point-blank-ada.cip.project_ga4_urls`": ga4_url_rows or [],
            # _query_ga4 — main query (only hit if URLs exist)
            "FROM `point-blank-ada.cip.fact_ga4_daily`": ga4_metric_rows or [],
            # _query_budget_pacing
            "FROM `point-blank-ada.cip.budget_tracking`": pacing_rows or [],
        })
        self.captured_data: dict[CampaignType, CampaignData] = {}
        self.stored_outputs: list[list[DiagnosticOutput]] = []
        self.fired_outputs: list[DiagnosticOutput] = []

    def _fake_compute(self, campaign_type: CampaignType):
        """Return a compute_* stub that captures CampaignData and returns a
        minimal valid DiagnosticOutput."""
        def _compute(data: CampaignData) -> DiagnosticOutput:
            self.captured_data[campaign_type] = data
            return DiagnosticOutput(
                project_code=data.project_code,
                campaign_type=campaign_type,
                evaluation_date=data.flight.evaluation_date,
                flight_day=data.flight.elapsed_days,
                flight_total_days=data.flight.total_days,
                health_score=75.0,
                health_status=StatusBand.STRONG,
                platforms=sorted({p.platform_id for p in data.platform_metrics}),
                line_ids=[l.line_id for l in data.media_plan],
            )
        return _compute

    def __enter__(self):
        # Patch the full bq module used by engine.py, plus the two health
        # computers and the two BQ load-job clients. The `bq.table()` return
        # must include the fully-qualified path since we key the router off
        # it (e.g. "FROM `point-blank-ada.cip.project_ga4_urls`").
        self.mock_bq = patch("backend.services.diagnostics.engine.bq").start()
        self.mock_bq.table.side_effect = lambda n: f"`point-blank-ada.cip.{n}`"
        self.mock_bq.string_param = MagicMock(return_value=MagicMock())
        self.mock_bq.date_param = MagicMock(return_value=MagicMock())
        self.mock_bq.scalar_param = MagicMock(return_value=MagicMock())
        self.mock_bq.array_param = MagicMock(return_value=MagicMock())
        self.mock_bq.run_query.side_effect = self.router

        self.mock_persuasion = patch(
            "backend.services.diagnostics.engine.compute_persuasion_health",
            side_effect=self._fake_compute(CampaignType.PERSUASION),
        ).start()
        self.mock_conversion = patch(
            "backend.services.diagnostics.engine.compute_conversion_health",
            side_effect=self._fake_compute(CampaignType.CONVERSION),
        ).start()

        # Swallow the BQ load-job writes (no real IO)
        def _store(outputs):
            self.stored_outputs.append(list(outputs))
        def _fire(output):
            self.fired_outputs.append(output)

        self.mock_store = patch(
            "backend.services.diagnostics.engine._store_results",
            side_effect=_store,
        ).start()
        self.mock_fire = patch(
            "backend.services.diagnostics.engine._fire_alerts",
            side_effect=_fire,
        ).start()

        return self

    def __exit__(self, exc_type, exc, tb):
        patch.stopall()
        return False


# ── Fixtures ────────────────────────────────────────────────────────


@pytest.fixture
def osstf_media_plan():
    """OSSTF 25042 — mixed shape. 3 persuasion lines + 1 conversion line."""
    return [
        _media_plan_row("eng-f1", "Engagement (Comments & Likes)", budget=6000, impressions=600_000),
        _media_plan_row("aware-f2", "Awareness (Video Views)", budget=8000, impressions=800_000),
        _media_plan_row("retarget", "Retargeting", budget=3000, impressions=200_000, platform_id="stackadapt"),
        _media_plan_row("ctv", "Reach & Frequency (CTV)", budget=12000, impressions=1_200_000, platform_id="stackadapt"),
    ]


@pytest.fixture
def osstf_digital_rows():
    """fact_digital_daily rows: one per (platform, campaign_objective) pair.

    Mirrors what would come back for a mixed campaign — awareness on FB,
    engagement on FB, awareness on StackAdapt (CTV), and conversion
    (retargeting) on StackAdapt.
    """
    return [
        _fact_digital_row("facebook", "OUTCOME_ENGAGEMENT", spend=5500, impressions=500_000, clicks=800),
        _fact_digital_row("facebook", "OUTCOME_AWARENESS", spend=7000, impressions=750_000, clicks=200),
        _fact_digital_row("stackadapt", "REACH", spend=10000, impressions=1_000_000, clicks=400),
        _fact_digital_row("stackadapt", "CONVERSIONS", spend=2500, impressions=150_000, clicks=350, conversions=42),
    ]


@pytest.fixture
def osstf_adset_rows():
    """fact_adset_daily rows: classified by campaign_name keywords."""
    return [
        _fact_adset_row("facebook", "25042 F1 Engagement Comments", reach=120_000, frequency=4.2),
        _fact_adset_row("facebook", "25042 F2 Awareness Video Views", reach=200_000, frequency=3.8),
        _fact_adset_row("stackadapt", "25042 CTV Reach Frequency", reach=400_000, frequency=2.5),
        _fact_adset_row("stackadapt", "25042 Retargeting Conversions", reach=25_000, frequency=6.0),
    ]


# ── Tests ───────────────────────────────────────────────────────────


class TestMixedCampaign:
    """OSSTF 25042 — the canonical mixed campaign."""

    def test_mixed_returns_two_outputs(self, osstf_media_plan, osstf_digital_rows, osstf_adset_rows):
        from backend.services.diagnostics.engine import run_diagnostics_for_project
        with _EngineContext(osstf_media_plan, osstf_digital_rows, osstf_adset_rows) as ctx:
            outputs = run_diagnostics_for_project("25042", date(2026, 4, 15))

        assert len(outputs) == 2
        types = {o.campaign_type for o in outputs}
        assert types == {CampaignType.PERSUASION, CampaignType.CONVERSION}

    def test_mixed_persuasion_subset_gets_only_persuasion_lines(
        self, osstf_media_plan, osstf_digital_rows, osstf_adset_rows
    ):
        from backend.services.diagnostics.engine import run_diagnostics_for_project
        with _EngineContext(osstf_media_plan, osstf_digital_rows, osstf_adset_rows) as ctx:
            run_diagnostics_for_project("25042", date(2026, 4, 15))

        persuasion_data = ctx.captured_data[CampaignType.PERSUASION]
        line_ids = {l.line_id for l in persuasion_data.media_plan}
        assert line_ids == {"eng-f1", "aware-f2", "ctv"}

    def test_mixed_conversion_subset_gets_only_conversion_lines(
        self, osstf_media_plan, osstf_digital_rows, osstf_adset_rows
    ):
        from backend.services.diagnostics.engine import run_diagnostics_for_project
        with _EngineContext(osstf_media_plan, osstf_digital_rows, osstf_adset_rows) as ctx:
            run_diagnostics_for_project("25042", date(2026, 4, 15))

        conversion_data = ctx.captured_data[CampaignType.CONVERSION]
        line_ids = {l.line_id for l in conversion_data.media_plan}
        assert line_ids == {"retarget"}

    def test_platform_metrics_partitioned_by_campaign_type(
        self, osstf_media_plan, osstf_digital_rows, osstf_adset_rows
    ):
        """The conversion subset must NOT see spend/impressions from the
        awareness campaigns — that was the OSSTF 25042 bug."""
        from backend.services.diagnostics.engine import run_diagnostics_for_project
        with _EngineContext(osstf_media_plan, osstf_digital_rows, osstf_adset_rows) as ctx:
            run_diagnostics_for_project("25042", date(2026, 4, 15))

        # Conversion subset should have exactly one row (stackadapt conversion)
        # with spend 2500, NOT summed with the awareness stackadapt row (10000).
        conversion_data = ctx.captured_data[CampaignType.CONVERSION]
        assert conversion_data.total_spend == 2500
        assert conversion_data.total_conversions == 42

        # Persuasion subset should sum the three awareness/engagement rows
        persuasion_data = ctx.captured_data[CampaignType.PERSUASION]
        # 5500 + 7000 + 10000 = 22500
        assert persuasion_data.total_spend == 22500

    def test_ga4_feeds_both_subsets(
        self, osstf_media_plan, osstf_digital_rows, osstf_adset_rows
    ):
        """Build Plan §12: GA4 session data is shared between both subsets."""
        from backend.services.diagnostics.engine import run_diagnostics_for_project
        ga4_urls = [{"ga4_property_id": "GA4-123", "url_pattern": "/25042"}]
        ga4_rows = [{
            "sessions": 1000, "scrolls": 300, "engaged_sessions": 500,
            "form_starts": 50, "form_submits": 25, "key_events": 20,
        }]
        with _EngineContext(
            osstf_media_plan, osstf_digital_rows, osstf_adset_rows,
            ga4_url_rows=ga4_urls, ga4_metric_rows=ga4_rows,
        ) as ctx:
            run_diagnostics_for_project("25042", date(2026, 4, 15))

        for ctype in (CampaignType.PERSUASION, CampaignType.CONVERSION):
            assert ctx.captured_data[ctype].ga4.sessions == 1000
            assert ctx.captured_data[ctype].ga4.form_submits == 25

    def test_both_outputs_stored_in_one_load(
        self, osstf_media_plan, osstf_digital_rows, osstf_adset_rows
    ):
        """_store_results is called once with both DiagnosticOutput objects."""
        from backend.services.diagnostics.engine import run_diagnostics_for_project
        with _EngineContext(osstf_media_plan, osstf_digital_rows, osstf_adset_rows) as ctx:
            run_diagnostics_for_project("25042", date(2026, 4, 15))

        assert len(ctx.stored_outputs) == 1
        assert len(ctx.stored_outputs[0]) == 2

    def test_alerts_fired_per_output(
        self, osstf_media_plan, osstf_digital_rows, osstf_adset_rows
    ):
        """_fire_alerts is called once per DiagnosticOutput (not once total)."""
        from backend.services.diagnostics.engine import run_diagnostics_for_project
        with _EngineContext(osstf_media_plan, osstf_digital_rows, osstf_adset_rows) as ctx:
            run_diagnostics_for_project("25042", date(2026, 4, 15))

        assert len(ctx.fired_outputs) == 2
        types_fired = {o.campaign_type for o in ctx.fired_outputs}
        assert types_fired == {CampaignType.PERSUASION, CampaignType.CONVERSION}


# ── Regression: pure projects still work ─────────────────────────


class TestPureCampaignsRegression:
    """Pre-refactor behaviour must be preserved for single-type projects."""

    def test_pure_persuasion_returns_single_output(self):
        from backend.services.diagnostics.engine import run_diagnostics_for_project
        media_plan = [
            _media_plan_row("f1", "Awareness (Video Views)"),
            _media_plan_row("f2", "Engagement (Comments & Likes)"),
        ]
        digital = [
            _fact_digital_row("facebook", "OUTCOME_AWARENESS", spend=5000),
            _fact_digital_row("facebook", "OUTCOME_ENGAGEMENT", spend=3000),
        ]
        adset = [
            _fact_adset_row("facebook", "25042 Awareness Video", reach=100_000, frequency=3.0),
        ]
        with _EngineContext(media_plan, digital, adset) as ctx:
            outputs = run_diagnostics_for_project("25013", date(2026, 4, 15))

        assert len(outputs) == 1
        assert outputs[0].campaign_type == CampaignType.PERSUASION
        assert CampaignType.PERSUASION in ctx.captured_data
        assert CampaignType.CONVERSION not in ctx.captured_data

    def test_pure_conversion_returns_single_output(self):
        from backend.services.diagnostics.engine import run_diagnostics_for_project
        media_plan = [
            _media_plan_row("c1", "Conversion"),
            _media_plan_row("c2", "Lead Gen - Website Forms"),
        ]
        digital = [
            _fact_digital_row("facebook", "CONVERSIONS", spend=4000, conversions=50),
            _fact_digital_row("facebook", "LEAD_GENERATION", spend=3000, conversions=40),
        ]
        adset = [
            _fact_adset_row("facebook", "25013 Conversion Leads", reach=80_000, frequency=2.2),
        ]
        with _EngineContext(media_plan, digital, adset) as ctx:
            outputs = run_diagnostics_for_project("25013", date(2026, 4, 15))

        assert len(outputs) == 1
        assert outputs[0].campaign_type == CampaignType.CONVERSION
        assert CampaignType.CONVERSION in ctx.captured_data
        assert CampaignType.PERSUASION not in ctx.captured_data


# ── Edge cases ──────────────────────────────────────────────────────


class TestEdgeCases:
    def test_no_media_plan_returns_empty_list(self):
        from backend.services.diagnostics.engine import run_diagnostics_for_project
        with _EngineContext(
            media_plan_rows=[], digital_rows=[], adset_rows=[],
        ) as ctx:
            outputs = run_diagnostics_for_project("99999", date(2026, 4, 15))

        assert outputs == []
        # Should not have called compute, store, or fire
        assert ctx.captured_data == {}
        assert ctx.stored_outputs == []
        assert ctx.fired_outputs == []

    def test_ambiguous_objective_defaults_to_persuasion(self):
        """Conservative default: null / unknown objectives go to persuasion."""
        from backend.services.diagnostics.engine import run_diagnostics_for_project
        media_plan = [
            _media_plan_row("unknown", None),
            _media_plan_row("weird", "Xyzzy"),
        ]
        digital = [
            _fact_digital_row("facebook", "OUTCOME_AWARENESS", spend=1000),
        ]
        adset = [_fact_adset_row("facebook", "25099 Campaign", reach=10_000)]

        with _EngineContext(media_plan, digital, adset) as ctx:
            outputs = run_diagnostics_for_project("25099", date(2026, 4, 15))

        assert len(outputs) == 1
        assert outputs[0].campaign_type == CampaignType.PERSUASION

    def test_null_campaign_objective_falls_back_to_campaign_name(self):
        """Daily rows with NULL campaign_objective must classify via campaign_name.

        Regression: the OSSTF 25042 Meta conversion card went blank starting
        2026-04-07 because the upstream transformation stopped writing
        campaign_objective, and the engine's daily path classified every row
        as PERSUASION (the conservative default for NULL). With the fallback
        in place, a NULL-objective row whose campaign_name contains a
        conversion keyword ("Retargeting", "Leads", etc.) routes into the
        conversion bucket."""
        from backend.services.diagnostics.engine import run_diagnostics_for_project
        media_plan = [
            _media_plan_row("awareness", "Awareness (Video Views)"),
            _media_plan_row("retarget", "Retargeting", platform_id="facebook"),
        ]
        # Both rows have campaign_objective=None — simulating the regression.
        # They should classify from campaign_name: the retargeting name must
        # route to CONVERSION, not lump back in with PERSUASION.
        digital = [
            _fact_digital_row(
                "facebook", None,
                spend=5000, impressions=500_000, clicks=400,
                campaign_name="25042 F1 Awareness Video Views",
            ),
            _fact_digital_row(
                "facebook", None,
                spend=2000, impressions=100_000, clicks=300, conversions=30,
                campaign_name="25042 Retargeting Conversions",
            ),
        ]
        adset = [
            _fact_adset_row("facebook", "25042 F1 Awareness Video Views",
                            reach=120_000, frequency=3.0),
            _fact_adset_row("facebook", "25042 Retargeting Conversions",
                            reach=20_000, frequency=5.0),
        ]
        with _EngineContext(media_plan, digital, adset) as ctx:
            outputs = run_diagnostics_for_project("25042", date(2026, 4, 15))

        # Must produce BOTH outputs — the conversion card must not be blank.
        types = {o.campaign_type for o in outputs}
        assert types == {CampaignType.PERSUASION, CampaignType.CONVERSION}

        # Persuasion bucket sees the awareness spend, not the retargeting spend.
        persuasion_data = ctx.captured_data[CampaignType.PERSUASION]
        assert persuasion_data.total_spend == 5000

        # Conversion bucket sees the retargeting spend + conversions — proof
        # the fallback routed the NULL-objective row correctly.
        conversion_data = ctx.captured_data[CampaignType.CONVERSION]
        assert conversion_data.total_spend == 2000
        assert conversion_data.total_conversions == 30

    def test_per_subset_flight_derivation(self):
        """Persuasion and conversion subsets derive their own flight dates.

        Use a media plan where the conversion line starts/ends later than
        the persuasion lines — each subset's flight should reflect its own
        min(start) / max(end).
        """
        from backend.services.diagnostics.engine import run_diagnostics_for_project
        media_plan = [
            {**_media_plan_row("aware", "Awareness (Video Views)"),
             "flight_start": date(2026, 4, 1), "flight_end": date(2026, 4, 15)},
            {**_media_plan_row("retarget", "Retargeting"),
             "flight_start": date(2026, 4, 10), "flight_end": date(2026, 4, 30)},
        ]
        digital = [
            _fact_digital_row("facebook", "OUTCOME_AWARENESS", spend=1000),
            _fact_digital_row("facebook", "CONVERSIONS", spend=500, conversions=10),
        ]
        adset = [
            _fact_adset_row("facebook", "Awareness Video", reach=50_000),
            _fact_adset_row("facebook", "Retargeting Conv", reach=10_000),
        ]
        with _EngineContext(media_plan, digital, adset) as ctx:
            run_diagnostics_for_project("25088", date(2026, 4, 15))

        p_flight = ctx.captured_data[CampaignType.PERSUASION].flight
        c_flight = ctx.captured_data[CampaignType.CONVERSION].flight
        assert p_flight.flight_start == date(2026, 4, 1)
        assert p_flight.flight_end == date(2026, 4, 15)
        assert c_flight.flight_start == date(2026, 4, 10)
        assert c_flight.flight_end == date(2026, 4, 30)


# ── Budget pacing filter ────────────────────────────────────────────


class TestBudgetPacingFilter:
    """_query_budget_pacing must restrict rollup to the subset's line_ids."""

    def test_pacing_query_uses_line_ids_when_provided(self):
        from backend.services.diagnostics.engine import _query_budget_pacing
        with patch("backend.services.diagnostics.engine.bq") as mock_bq:
            mock_bq.table.side_effect = lambda n: f"`point-blank-ada.cip.{n}`"
            mock_bq.string_param = MagicMock(return_value=MagicMock())
            mock_bq.date_param = MagicMock(return_value=MagicMock())
            mock_bq.array_param = MagicMock(return_value=MagicMock())
            mock_bq.run_query.return_value = [{"pacing_percentage": 87.5}]

            result = _query_budget_pacing(
                "25042", date(2026, 4, 15), line_ids={"line-a", "line-b"}
            )

        assert result == 87.5
        # array_param should have been called once with deterministic ordering
        mock_bq.array_param.assert_called_once()
        args = mock_bq.array_param.call_args
        assert args.args[0] == "line_ids"
        assert args.args[1] == "STRING"
        assert args.args[2] == ["line-a", "line-b"]  # sorted
        # SQL must include the UNNEST clause
        called_sql = mock_bq.run_query.call_args.args[0]
        assert "IN UNNEST(@line_ids)" in called_sql

    def test_pacing_query_without_line_ids_omits_filter(self):
        from backend.services.diagnostics.engine import _query_budget_pacing
        with patch("backend.services.diagnostics.engine.bq") as mock_bq:
            mock_bq.table.side_effect = lambda n: f"`point-blank-ada.cip.{n}`"
            mock_bq.string_param = MagicMock(return_value=MagicMock())
            mock_bq.date_param = MagicMock(return_value=MagicMock())
            mock_bq.array_param = MagicMock(return_value=MagicMock())
            mock_bq.run_query.return_value = [{"pacing_percentage": 100.0}]

            _query_budget_pacing("25042", date(2026, 4, 15), line_ids=None)

        mock_bq.array_param.assert_not_called()
        called_sql = mock_bq.run_query.call_args.args[0]
        assert "IN UNNEST(@line_ids)" not in called_sql


# ── Reach window handling ────────────────────────────────────────────


class TestReachWindowSelection:
    """Adset rows carry a reach_window (1d / 7d / null) that's not comparable
    across windows. The engine must pick one window per (platform, campaign).

    Preference order: 7d > 1d > null/other. This mirrors how platforms report
    frequency natively — a 7d reach lookback is the most common and yields
    the most representative frequency signal."""

    def test_prefers_7d_window_over_1d_same_campaign(self):
        """Same (platform, campaign) with both 7d and 1d rows: only 7d counts."""
        from backend.services.diagnostics.engine import run_diagnostics_for_project
        media_plan = [
            _media_plan_row("aware", "Awareness (Video Views)"),
        ]
        digital = [
            _fact_digital_row("facebook", "OUTCOME_AWARENESS",
                              spend=5000, impressions=500_000, clicks=400),
        ]
        # Same campaign reported against both windows — 1d row has smaller
        # reach and higher frequency than the 7d row. If the engine
        # accidentally mixes them, the final frequency will be a garbage
        # weighted blend.
        adset = [
            _fact_adset_row("facebook", "25042 Awareness Video",
                            reach=100_000, frequency=5.0, impressions=500_000,
                            reach_window="1d"),
            _fact_adset_row("facebook", "25042 Awareness Video",
                            reach=250_000, frequency=2.0, impressions=500_000,
                            reach_window="7d"),
        ]
        with _EngineContext(media_plan, digital, adset) as ctx:
            run_diagnostics_for_project("25042", date(2026, 4, 15))

        pm = ctx.captured_data[CampaignType.PERSUASION].platform_metrics
        assert len(pm) == 1
        assert pm[0].platform_id == "facebook"
        # Only the 7d row should have been picked — reach=250k, freq=2.0.
        assert pm[0].reach == 250_000
        assert pm[0].frequency == pytest.approx(2.0)

    def test_falls_back_to_1d_when_only_1d_available(self):
        """If a (platform, campaign) only has 1d rows, we still use them."""
        from backend.services.diagnostics.engine import run_diagnostics_for_project
        media_plan = [_media_plan_row("aware", "Awareness (Video Views)")]
        digital = [
            _fact_digital_row("facebook", "OUTCOME_AWARENESS", spend=2000),
        ]
        adset = [
            _fact_adset_row("facebook", "25042 Awareness Video",
                            reach=80_000, frequency=3.5, impressions=280_000,
                            reach_window="1d"),
        ]
        with _EngineContext(media_plan, digital, adset) as ctx:
            run_diagnostics_for_project("25042", date(2026, 4, 15))

        pm = ctx.captured_data[CampaignType.PERSUASION].platform_metrics
        assert len(pm) == 1
        assert pm[0].reach == 80_000
        assert pm[0].frequency == pytest.approx(3.5)

    def test_null_reach_window_is_preserved_as_fallback(self):
        """Some platforms don't populate reach_window — treat null as lowest
        priority but use it when nothing better exists."""
        from backend.services.diagnostics.engine import run_diagnostics_for_project
        media_plan = [_media_plan_row("aware", "Awareness (Video Views)")]
        digital = [
            _fact_digital_row("facebook", "OUTCOME_AWARENESS", spend=2000),
        ]
        adset = [
            # Only row: reach_window is None (default kwarg)
            _fact_adset_row("facebook", "25042 Awareness Video",
                            reach=60_000, frequency=3.0, impressions=180_000),
        ]
        with _EngineContext(media_plan, digital, adset) as ctx:
            run_diagnostics_for_project("25042", date(2026, 4, 15))

        pm = ctx.captured_data[CampaignType.PERSUASION].platform_metrics
        assert len(pm) == 1
        assert pm[0].reach == 60_000
        assert pm[0].frequency == pytest.approx(3.0)


# ── Reach aggregation across multiple same-type campaigns ───────────


class TestReachAggregationMultiCampaign:
    """When a single platform runs 3+ campaigns of the same type, reach is
    aggregated via MAX (conservative floor — avoids double-counting overlap)
    and frequency is an impression-weighted average across campaigns.

    MAX is documented as understating reach for non-overlapping audiences
    but was chosen over SUM to avoid overstating overlap. This test locks
    in that behaviour."""

    def test_three_awareness_campaigns_same_platform(self):
        from backend.services.diagnostics.engine import run_diagnostics_for_project
        media_plan = [
            _media_plan_row("f1", "Awareness (Video Views)"),
            _media_plan_row("f2", "Engagement (Comments & Likes)"),
            _media_plan_row("f3", "Reach & Frequency (CTV)"),
        ]
        digital = [
            _fact_digital_row("facebook", "OUTCOME_AWARENESS",
                              spend=3000, impressions=300_000, clicks=200),
            _fact_digital_row("facebook", "OUTCOME_ENGAGEMENT",
                              spend=4000, impressions=400_000, clicks=600),
            _fact_digital_row("facebook", "REACH",
                              spend=2000, impressions=200_000, clicks=100),
        ]
        # Three campaigns, all persuasion, same platform. Reach maxes:
        #   180k, 220k, 150k  → MAX = 220k
        # Frequencies (impression-weighted):
        #   (2.0 * 300k) + (3.0 * 400k) + (1.5 * 200k)   = 600k+1.2M+300k = 2.1M
        #   (300k + 400k + 200k)                         = 900k
        #   → 2.1M / 900k ≈ 2.333
        adset = [
            _fact_adset_row("facebook", "Awareness Video",
                            reach=180_000, frequency=2.0, impressions=300_000,
                            reach_window="7d"),
            _fact_adset_row("facebook", "Engagement Post",
                            reach=220_000, frequency=3.0, impressions=400_000,
                            reach_window="7d"),
            _fact_adset_row("facebook", "CTV Reach Frequency",
                            reach=150_000, frequency=1.5, impressions=200_000,
                            reach_window="7d"),
        ]
        with _EngineContext(media_plan, digital, adset) as ctx:
            run_diagnostics_for_project("25042", date(2026, 4, 15))

        pm = ctx.captured_data[CampaignType.PERSUASION].platform_metrics
        assert len(pm) == 1
        fb = pm[0]
        assert fb.platform_id == "facebook"
        # Spend/impressions SUM across campaigns (daily bucket)
        assert fb.spend == pytest.approx(9000)
        assert fb.impressions == 900_000
        # Reach: MAX, not SUM
        assert fb.reach == 220_000
        # Frequency: impression-weighted avg ≈ 2.333
        assert fb.frequency == pytest.approx(2.1e6 / 9e5, rel=1e-3)
