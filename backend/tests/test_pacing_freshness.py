"""P-FRESH-PACE: freshness-aware pacing tests.

Covers the engine's not_reporting + estimate + platform-pool-over-budget paths
and the router's exclusion of not_reporting / estimate lines from the pacing
ratio (both numerator and denominator).
"""

import asyncio
from datetime import date
from unittest.mock import patch

import pytest

from backend.services.pacing import run_pacing_for_project
from backend.routers import pacing as pacing_router


def _make_line(line_id, platform_id, budget, flight_start, flight_end,
               line_code=None, audience_name=None):
    return {
        "line_id": line_id, "line_code": line_code, "platform_id": platform_id,
        "channel_category": None, "site_network": None, "budget": budget,
        "flight_start": flight_start, "flight_end": flight_end,
        "audience_name": audience_name,
    }


def _stale(*platform_ids):
    return [{"platform_id": pid, "is_stale": True} for pid in platform_ids]


# ── Engine: not_reporting ───────────────────────────────────────────


class TestNotReporting:
    @patch("backend.services.pacing.compute_platform_freshness")
    @patch("backend.services.pacing.date")
    @patch("backend.services.pacing.bq")
    @patch("backend.services.pacing._write_budget_tracking")
    @patch("backend.services.pacing._write_alerts")
    def test_stale_inflight_line_reads_not_reporting(
        self, mock_alerts, mock_tracking, mock_bq, mock_date, mock_fresh
    ):
        """A stale platform's in-flight line → not_reporting: planned=0 (out of
        the denominator), frozen actual KEPT, no pacing over/under alert."""
        mock_date.today.return_value = date(2026, 7, 17)
        mock_date.fromisoformat.side_effect = lambda s: date.fromisoformat(s)
        mock_bq.table.side_effect = lambda n: f"`ds.{n}`"
        mock_bq.string_param.side_effect = lambda n, v: (n, v)
        mock_bq.date_param.side_effect = lambda n, v: (n, v)
        mock_fresh.return_value = _stale("stackadapt")

        lines = [_make_line("D1", "stackadapt", 10000,
                            date(2026, 7, 1), date(2026, 7, 31))]

        def qr(sql, params=None):
            if "media_plan_lines" in sql:
                return lines
            if "blocking_chart_weeks" in sql:
                return []
            if "SUM(spend)" in sql:
                return [{"total_spend": 500.0}]
            return []

        mock_bq.run_query.side_effect = qr
        result = run_pacing_for_project("26023", date(2026, 7, 17))

        row = mock_tracking.call_args[0][2][0]
        assert row["line_status"] == "not_reporting"
        assert row["is_not_reporting"] is True
        # frozen actual kept, planned dropped out of the ratio
        assert row["actual_spend_to_date"] == 500.0
        assert row["planned_spend_to_date"] == 0.0
        assert row["pacing_percentage"] == 0.0
        # a dead feed must not fire a pacing alert
        assert result["alerts"] == 0
        assert not mock_alerts.called

    @patch("backend.services.pacing.compute_platform_freshness")
    @patch("backend.services.pacing.date")
    @patch("backend.services.pacing.bq")
    @patch("backend.services.pacing._write_budget_tracking")
    @patch("backend.services.pacing._write_alerts")
    def test_fresh_platform_line_stays_active(
        self, mock_alerts, mock_tracking, mock_bq, mock_date, mock_fresh
    ):
        """Control: same line, platform NOT stale → normal active pacing."""
        mock_date.today.return_value = date(2026, 7, 17)
        mock_date.fromisoformat.side_effect = lambda s: date.fromisoformat(s)
        mock_bq.table.side_effect = lambda n: f"`ds.{n}`"
        mock_bq.string_param.side_effect = lambda n, v: (n, v)
        mock_bq.date_param.side_effect = lambda n, v: (n, v)
        mock_fresh.return_value = []  # nothing stale

        lines = [_make_line("D1", "stackadapt", 10000,
                            date(2026, 7, 1), date(2026, 7, 31))]

        def qr(sql, params=None):
            if "media_plan_lines" in sql:
                return lines
            if "blocking_chart_weeks" in sql:
                return []
            if "SUM(spend)" in sql:
                return [{"total_spend": 500.0}]
            return []

        mock_bq.run_query.side_effect = qr
        run_pacing_for_project("26023", date(2026, 7, 17))

        row = mock_tracking.call_args[0][2][0]
        assert row["line_status"] == "active"
        assert row["is_not_reporting"] is False
        assert row["planned_spend_to_date"] > 0

    @patch("backend.services.pacing.compute_platform_freshness")
    @patch("backend.services.pacing.date")
    @patch("backend.services.pacing.bq")
    @patch("backend.services.pacing._write_budget_tracking")
    @patch("backend.services.pacing._write_alerts")
    def test_stale_but_flight_ended_stays_completed(
        self, mock_alerts, mock_tracking, mock_bq, mock_date, mock_fresh
    ):
        """A stale platform whose flight has ENDED is 'completed', not
        'not_reporting' (the not_reporting swap only applies in-flight)."""
        mock_date.today.return_value = date(2026, 7, 17)
        mock_date.fromisoformat.side_effect = lambda s: date.fromisoformat(s)
        mock_bq.table.side_effect = lambda n: f"`ds.{n}`"
        mock_bq.string_param.side_effect = lambda n, v: (n, v)
        mock_bq.date_param.side_effect = lambda n, v: (n, v)
        mock_fresh.return_value = _stale("stackadapt")

        lines = [_make_line("D1", "stackadapt", 10000,
                            date(2026, 6, 1), date(2026, 6, 30))]

        def qr(sql, params=None):
            if "media_plan_lines" in sql:
                return lines
            if "blocking_chart_weeks" in sql:
                return []
            if "SUM(spend)" in sql:
                return [{"total_spend": 9800.0}]
            return []

        mock_bq.run_query.side_effect = qr
        run_pacing_for_project("26023", date(2026, 7, 17))

        row = mock_tracking.call_args[0][2][0]
        assert row["line_status"] == "completed"
        assert row["is_not_reporting"] is False


# ── Engine: residual estimate flag ──────────────────────────────────


class TestEstimateFlag:
    @patch("backend.services.pacing.compute_platform_freshness")
    @patch("backend.services.pacing.date")
    @patch("backend.services.pacing.bq")
    @patch("backend.services.pacing._write_budget_tracking")
    @patch("backend.services.pacing._write_alerts")
    def test_residual_group_split_line_is_estimate(
        self, mock_alerts, mock_tracking, mock_bq, mock_date, mock_fresh
    ):
        """A line attributed ONLY via the budget-weight residual split carries
        is_estimate=True; a line_code-measured line does not."""
        mock_date.today.return_value = date(2026, 7, 17)
        mock_date.fromisoformat.side_effect = lambda s: date.fromisoformat(s)
        mock_bq.table.side_effect = lambda n: f"`ds.{n}`"
        mock_bq.string_param.side_effect = lambda n, v: (n, v)
        mock_bq.date_param.side_effect = lambda n, v: (n, v)
        mock_fresh.return_value = []

        # A: measured by line_code; B: no line_code → residual estimate.
        lines = [
            _make_line("A", "meta", 5000, date(2026, 7, 1), date(2026, 7, 31),
                       line_code="MA"),
            _make_line("B", "meta", 5000, date(2026, 7, 1), date(2026, 7, 31)),
        ]

        def qr(sql, params=None):
            if "media_plan_lines" in sql:
                return lines
            if "blocking_chart_weeks" in sql:
                return []
            if "SUM(spend)" in sql and "line_code" in sql:
                return [{"total_spend": 600.0, "first_spend_date": date(2026, 7, 5)}]
            if "SUM(spend)" in sql:
                return [{"total_spend": 1000.0}]
            return []

        mock_bq.run_query.side_effect = qr
        run_pacing_for_project("TEST", date(2026, 7, 17))

        by_id = {r["line_id"]: r for r in mock_tracking.call_args[0][2]}
        assert by_id["A"]["is_estimate"] is False   # measured
        assert by_id["B"]["is_estimate"] is True    # residual estimate
        assert by_id["B"]["actual_spend_to_date"] == pytest.approx(400.0)


# ── Engine: platform-pool over-budget safety net ────────────────────


class TestPlatformPoolOverBudget:
    @patch("backend.services.pacing.compute_platform_freshness")
    @patch("backend.services.pacing.date")
    @patch("backend.services.pacing.bq")
    @patch("backend.services.pacing._write_budget_tracking")
    @patch("backend.services.pacing._write_alerts")
    def test_not_reporting_platform_still_flags_real_overspend(
        self, mock_alerts, mock_tracking, mock_bq, mock_date, mock_fresh
    ):
        """Even when the line is not_reporting (no per-line alert), a platform
        whose total REAL spend exceeds its total budget still fires a
        budget_exceeded alert — real overspend is never silenced."""
        mock_date.today.return_value = date(2026, 7, 17)
        mock_date.fromisoformat.side_effect = lambda s: date.fromisoformat(s)
        mock_bq.table.side_effect = lambda n: f"`ds.{n}`"
        mock_bq.string_param.side_effect = lambda n, v: (n, v)
        mock_bq.date_param.side_effect = lambda n, v: (n, v)
        mock_fresh.return_value = _stale("stackadapt")

        lines = [_make_line("D1", "stackadapt", 1000,
                            date(2026, 7, 1), date(2026, 7, 31))]

        def qr(sql, params=None):
            if "media_plan_lines" in sql:
                return lines
            if "blocking_chart_weeks" in sql:
                return []
            if "SUM(spend)" in sql:
                return [{"total_spend": 1500.0}]  # over the $1000 pool
            return []

        mock_bq.run_query.side_effect = qr
        result = run_pacing_for_project("26023", date(2026, 7, 17))

        row = mock_tracking.call_args[0][2][0]
        assert row["line_status"] == "not_reporting"
        # pool overspend still fired
        assert result["alerts"] == 1
        alerts = mock_alerts.call_args[0][0]
        assert any(a["alert_type"] == "budget_exceeded" for a in alerts)

    @patch("backend.services.pacing.compute_platform_freshness")
    @patch("backend.services.pacing.date")
    @patch("backend.services.pacing.bq")
    @patch("backend.services.pacing._write_budget_tracking")
    @patch("backend.services.pacing._write_alerts")
    def test_not_reporting_within_budget_fires_no_alert(
        self, mock_alerts, mock_tracking, mock_bq, mock_date, mock_fresh
    ):
        """A not_reporting platform still within its pool budget fires nothing."""
        mock_date.today.return_value = date(2026, 7, 17)
        mock_date.fromisoformat.side_effect = lambda s: date.fromisoformat(s)
        mock_bq.table.side_effect = lambda n: f"`ds.{n}`"
        mock_bq.string_param.side_effect = lambda n, v: (n, v)
        mock_bq.date_param.side_effect = lambda n, v: (n, v)
        mock_fresh.return_value = _stale("stackadapt")

        lines = [_make_line("D1", "stackadapt", 1000,
                            date(2026, 7, 1), date(2026, 7, 31))]

        def qr(sql, params=None):
            if "media_plan_lines" in sql:
                return lines
            if "blocking_chart_weeks" in sql:
                return []
            if "SUM(spend)" in sql:
                return [{"total_spend": 400.0}]  # under budget
            return []

        mock_bq.run_query.side_effect = qr
        result = run_pacing_for_project("26023", date(2026, 7, 17))

        assert result["alerts"] == 0
        assert not mock_alerts.called


# ── Router: exclude not_reporting + estimate from the pacing ratio ──


class TestRouterRatioExclusion:
    def _brow(self, **over):
        base = {
            "date": date(2026, 7, 17),
            "line_id": "L", "line_code": "#01", "platform_id": "meta",
            "channel_category": "Digital", "line_status": "active",
            "planned_budget": 1000.0, "planned_spend_to_date": 1000.0,
            "actual_spend_to_date": 1000.0, "remaining_budget": 0.0,
            "remaining_days": 10, "pacing_percentage": 100.0,
            "daily_budget_required": 0.0, "is_over_pacing": False,
            "is_under_pacing": False, "bundle_id": None, "bundle_role": None,
            "is_not_reporting": False, "is_estimate": False,
            "audience_name": "A", "flight_start": date(2026, 7, 1),
            "flight_end": date(2026, 7, 31), "sheet_id": None,
            "phase_label": None, "phase_display_order": None,
        }
        base.update(over)
        return base

    def _bq(self, rows):
        class _BQ:
            @staticmethod
            def table(name):
                return f"`ds.{name}`"

            @staticmethod
            def string_param(n, v):
                return (n, v)

            @staticmethod
            def date_param(n, v):
                return (n, v)

            @staticmethod
            def array_param(n, t, v):
                return (n, v)

            @staticmethod
            def run_query(sql, params=None):
                if "dim_projects" in sql:
                    return [{"project_code": "26023", "net_budget": 100000.0}]
                if "budget_tracking" in sql:
                    return rows
                return []
        return _BQ

    def test_not_reporting_line_excluded_from_ratio_kept_in_total(self):
        rows = [
            self._brow(line_id="healthy",
                       planned_spend_to_date=1000.0, actual_spend_to_date=1000.0),
            # not_reporting: frozen actual present, planned 0
            self._brow(line_id="dooh", line_status="not_reporting",
                       is_not_reporting=True,
                       planned_spend_to_date=0.0, actual_spend_to_date=800.0),
        ]
        with patch.object(pacing_router, "bq", self._bq(rows)), \
                patch.object(pacing_router, "_query_untracked_platform_spend",
                             return_value=[]):
            resp = asyncio.run(pacing_router.get_pacing("26023"))

        # ratio is 1000/1000 = 100, NOT 1800/1000 = 180 (the false overspend)
        assert resp.overall_pacing_percentage == 100.0
        assert resp.total_planned_to_date == 1000.0
        # frozen not_reporting spend still shown in the displayed total
        assert resp.total_actual_to_date == 1800.0
        # and it is NOT counted as "spending with no baseline"
        assert resp.lines_without_baseline == 0

    def test_estimate_line_excluded_from_ratio_kept_in_total(self):
        rows = [
            self._brow(line_id="measured",
                       planned_spend_to_date=1000.0, actual_spend_to_date=1000.0),
            # residual estimate: has a baseline + spend, but excluded from ratio
            self._brow(line_id="estimate", is_estimate=True,
                       planned_spend_to_date=1000.0, actual_spend_to_date=2000.0),
        ]
        with patch.object(pacing_router, "bq", self._bq(rows)), \
                patch.object(pacing_router, "_query_untracked_platform_spend",
                             return_value=[]):
            resp = asyncio.run(pacing_router.get_pacing("26023"))

        # estimate line dropped from BOTH numerator and denominator:
        # 1000/1000 = 100, not 3000/2000 = 150
        assert resp.overall_pacing_percentage == 100.0
        assert resp.total_planned_to_date == 1000.0
        # its spend still appears in the displayed total
        assert resp.total_actual_to_date == 3000.0

    def test_line_flags_flow_through_to_pacing_line(self):
        rows = [
            self._brow(line_id="dooh", line_status="not_reporting",
                       is_not_reporting=True, is_estimate=True,
                       planned_spend_to_date=0.0, actual_spend_to_date=800.0),
        ]
        with patch.object(pacing_router, "bq", self._bq(rows)), \
                patch.object(pacing_router, "_query_untracked_platform_spend",
                             return_value=[]):
            resp = asyncio.run(pacing_router.get_pacing("26023"))

        line = resp.lines[0]
        assert line.is_not_reporting is True
        assert line.is_estimate is True

    def test_phase_pacing_matches_overall_excludes_estimate(self):
        """Review #2: the phase-level pacing % must apply the SAME _in_ratio
        hold-out as Overall Pacing, so a phase pill never diverges from the KPI
        for identical data (26023's Outdoor DOOH is is_estimate)."""
        rows = [
            self._brow(line_id="measured", sheet_id="S1", phase_label="Phase 1",
                       phase_display_order=1,
                       planned_spend_to_date=1000.0, actual_spend_to_date=1000.0),
            self._brow(line_id="estimate", sheet_id="S1", phase_label="Phase 1",
                       phase_display_order=1, is_estimate=True,
                       planned_spend_to_date=1000.0, actual_spend_to_date=2000.0),
        ]
        with patch.object(pacing_router, "bq", self._bq(rows)), \
                patch.object(pacing_router, "_query_untracked_platform_spend",
                             return_value=[]):
            resp = asyncio.run(pacing_router.get_pacing("26023"))

        assert resp.overall_pacing_percentage == 100.0
        assert len(resp.phases) == 1
        # phase pill agrees with the Overall Pacing KPI (both exclude estimate)
        assert resp.phases[0].pacing_percentage == 100.0

    def test_ratio_excluded_all_set_when_every_inflight_line_held_out(self):
        """Review #3: all in-flight lines held out → total_planned collapses to
        0 for a freshness reason, so ratio_excluded_all=True lets the UI render
        neutrally instead of a red 0.0%."""
        rows = [
            self._brow(line_id="dooh", line_status="not_reporting",
                       is_not_reporting=True,
                       planned_spend_to_date=0.0, actual_spend_to_date=800.0),
            self._brow(line_id="est", is_estimate=True,
                       planned_spend_to_date=500.0, actual_spend_to_date=500.0),
        ]
        with patch.object(pacing_router, "bq", self._bq(rows)), \
                patch.object(pacing_router, "_query_untracked_platform_spend",
                             return_value=[]):
            resp = asyncio.run(pacing_router.get_pacing("26023"))

        assert resp.ratio_excluded_all is True
        assert resp.overall_pacing_percentage == 0  # neutral, not a real 0%
        # spend still shown in the total
        assert resp.total_actual_to_date == 1300.0

    def test_ratio_excluded_all_false_when_a_healthy_line_present(self):
        rows = [
            self._brow(line_id="healthy",
                       planned_spend_to_date=1000.0, actual_spend_to_date=1000.0),
            self._brow(line_id="dooh", line_status="not_reporting",
                       is_not_reporting=True,
                       planned_spend_to_date=0.0, actual_spend_to_date=800.0),
        ]
        with patch.object(pacing_router, "bq", self._bq(rows)), \
                patch.object(pacing_router, "_query_untracked_platform_spend",
                             return_value=[]):
            resp = asyncio.run(pacing_router.get_pacing("26023"))

        assert resp.ratio_excluded_all is False
        assert resp.overall_pacing_percentage == 100.0


# ── Engine: platform-pool alert dedup vs per-line budget_exceeded ───


class TestPoolAlertDedup:
    @patch("backend.services.pacing.compute_platform_freshness")
    @patch("backend.services.pacing.date")
    @patch("backend.services.pacing.bq")
    @patch("backend.services.pacing._write_budget_tracking")
    @patch("backend.services.pacing._write_alerts")
    def test_pool_alert_deduped_when_per_line_budget_exceeded_fired(
        self, mock_alerts, mock_tracking, mock_bq, mock_date, mock_fresh
    ):
        """Review #4a: a per-line budget_exceeded (a COMPLETED, measured line on
        a stale platform) already surfaces the overspend — the platform-pool
        safety net must NOT emit a second budget_exceeded for the same platform."""
        mock_date.today.return_value = date(2026, 7, 17)
        mock_date.fromisoformat.side_effect = lambda s: date.fromisoformat(s)
        mock_bq.table.side_effect = lambda n: f"`ds.{n}`"
        mock_bq.string_param.side_effect = lambda n, v: (n, v)
        mock_bq.date_param.side_effect = lambda n, v: (n, v)
        mock_fresh.return_value = _stale("meta")

        # A: COMPLETED (June), line_code-measured, over its $1000 budget → fires
        #    a per-line budget_exceeded (completed lines still alert).
        # B: in-flight (July) on the same stale platform → not_reporting.
        lines = [
            _make_line("A", "meta", 1000, date(2026, 6, 1), date(2026, 6, 30),
                       line_code="MA"),
            _make_line("B", "meta", 1000, date(2026, 7, 1), date(2026, 7, 31)),
        ]

        def qr(sql, params=None):
            if "media_plan_lines" in sql:
                return lines
            if "blocking_chart_weeks" in sql:
                return []
            if "SUM(spend)" in sql and "line_code" in sql:
                return [{"total_spend": 1500.0, "first_spend_date": date(2026, 6, 5)}]
            if "SUM(spend)" in sql:
                pd = {p[0]: p[1] for p in (params or [])}
                if pd.get("flight_start") == date(2026, 7, 1):
                    return [{"total_spend": 800.0}]  # B's group (July)
                return [{"total_spend": 1500.0}]      # A's group (June)
            return []

        mock_bq.run_query.side_effect = qr
        result = run_pacing_for_project("26023", date(2026, 7, 17))

        by_id = {r["line_id"]: r for r in mock_tracking.call_args[0][2]}
        assert by_id["A"]["line_status"] == "completed"
        assert by_id["B"]["line_status"] == "not_reporting"

        alerts = mock_alerts.call_args[0][0]
        budget_alerts = [a for a in alerts if a["alert_type"] == "budget_exceeded"]
        # exactly ONE — the per-line one; the pool alert is deduped away.
        assert len(budget_alerts) == 1
        assert result["alerts"] == 1
