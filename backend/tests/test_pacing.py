"""Tests for pacing spend attribution by flight date (ADAC-27).

Tests cover:
  - Flight-date-filtered spend attribution
  - Proportional split within same flight window
  - Different flight windows on the same platform
  - NULL flight dates fallback (no date filter)
  - line_code match with date filtering
"""

from datetime import date
from unittest.mock import patch, MagicMock

import pytest

import asyncio

from backend.services.pacing import (
    run_pacing_for_project,
    _float,
    _audience_tokens,
    _match_adset_to_line_id,
)
from backend.routers import pacing as pacing_router


# ── Helpers ────────────────────────────────────────────────────────


def _make_line(
    line_id, platform_id, budget, flight_start, flight_end,
    line_code=None, channel_category=None, site_network=None,
):
    return {
        "line_id": line_id,
        "line_code": line_code,
        "platform_id": platform_id,
        "channel_category": channel_category,
        "site_network": site_network,
        "budget": budget,
        "flight_start": flight_start,
        "flight_end": flight_end,
    }


# J1 fix: Removed brittle _mock_run_query helper. Tests now use explicit inline
# query_router() functions with deterministic dispatch logic that checks both SQL
# content AND parameter values together, eliminating fragile substring matching.


# ── Test: flight-date-filtered spend ──────────────────────────────


class TestFlightDateFiltering:
    """Spend should only be attributed within a line's flight window."""

    @patch("backend.services.pacing.date")
    @patch("backend.services.pacing.bq")
    @patch("backend.services.pacing._write_budget_tracking")
    @patch("backend.services.pacing._write_alerts")
    def test_f2_line_gets_zero_spend_before_flight_starts(
        self, mock_alerts, mock_tracking, mock_bq, mock_date
    ):
        """A line with flight Apr 9–30 should get $0 spend when all spend
        occurred Feb 25–Apr 2 (before the flight started)."""
        # J2 fix: Proper date mocking that handles both .today() and .fromisoformat()
        mock_date.today.return_value = date(2026, 4, 9)
        mock_date.fromisoformat.side_effect = lambda iso_str: date.fromisoformat(iso_str)
        mock_bq.table.side_effect = lambda name: f"`proj.ds.{name}`"
        mock_bq.string_param.side_effect = lambda n, v: (n, v)
        mock_bq.date_param.side_effect = lambda n, v: (n, v)

        lines = [
            _make_line("L1", "meta", 10613, date(2026, 2, 26), date(2026, 3, 19)),
            _make_line("L2", "meta", 12742, date(2026, 4, 9), date(2026, 4, 30)),
            _make_line("L3", "meta", 5500, date(2026, 4, 9), date(2026, 4, 30)),
        ]

        def query_router(sql, params=None):
            if "media_plan_lines" in sql:
                return lines
            if "blocking_chart_weeks" in sql:
                return []
            if "SUM(spend)" in sql:
                # Check which flight window is being queried
                if params:
                    param_dict = {p[0]: p[1] for p in params}
                    fs = param_dict.get("flight_start")
                    fe = param_dict.get("flight_end")
                    if fs == date(2026, 2, 26) and fe == date(2026, 3, 19):
                        return [{"total_spend": 21609.0}]
                    if fs == date(2026, 4, 9) and fe == date(2026, 4, 30):
                        return [{"total_spend": 0.0}]
                return [{"total_spend": 0.0}]
            return []

        mock_bq.run_query.side_effect = query_router

        result = run_pacing_for_project("25042", date(2026, 4, 9))

        # Verify tracking rows were written
        assert mock_tracking.called
        tracking_rows = mock_tracking.call_args[0][2]

        # Find the tracking row for each line
        by_id = {r["line_id"]: r for r in tracking_rows}

        # F1 line (Feb 26 – Mar 19) should get all the spend
        assert by_id["L1"]["actual_spend_to_date"] == 21609.0

        # F2 lines (Apr 9 – Apr 30) should get $0 — flight just started
        assert by_id["L2"]["actual_spend_to_date"] == 0.0
        assert by_id["L3"]["actual_spend_to_date"] == 0.0

    @patch("backend.services.pacing.date")
    @patch("backend.services.pacing.bq")
    @patch("backend.services.pacing._write_budget_tracking")
    @patch("backend.services.pacing._write_alerts")
    def test_proportional_split_within_same_flight_window(
        self, mock_alerts, mock_tracking, mock_bq, mock_date
    ):
        """Two lines on the same platform with the same flight dates should
        split spend proportionally by budget."""
        mock_date.today.return_value = date(2026, 4, 15)
        mock_date.fromisoformat.side_effect = lambda iso_str: date.fromisoformat(iso_str)
        mock_bq.table.side_effect = lambda name: f"`proj.ds.{name}`"
        mock_bq.string_param.side_effect = lambda n, v: (n, v)
        mock_bq.date_param.side_effect = lambda n, v: (n, v)

        lines = [
            _make_line("L1", "meta", 12742, date(2026, 4, 9), date(2026, 4, 30)),
            _make_line("L2", "meta", 5500, date(2026, 4, 9), date(2026, 4, 30)),
        ]

        def query_router(sql, params=None):
            if "media_plan_lines" in sql:
                return lines
            if "blocking_chart_weeks" in sql:
                return []
            if "SUM(spend)" in sql:
                return [{"total_spend": 3000.0}]
            return []

        mock_bq.run_query.side_effect = query_router

        result = run_pacing_for_project("25042", date(2026, 4, 15))

        tracking_rows = mock_tracking.call_args[0][2]
        by_id = {r["line_id"]: r for r in tracking_rows}

        total_budget = 12742 + 5500
        expected_l1 = round(3000.0 * (12742 / total_budget), 2)
        expected_l2 = round(3000.0 * (5500 / total_budget), 2)

        assert by_id["L1"]["actual_spend_to_date"] == expected_l1
        assert by_id["L2"]["actual_spend_to_date"] == expected_l2

        # J4 fix: Assert conservative-estimate requirement — planned_spend_to_date
        # must never exceed prorated budget based on time elapsed.
        flight_start = date(2026, 4, 9)
        flight_end = date(2026, 4, 30)
        days_elapsed = (date(2026, 4, 15) - flight_start).days + 1
        total_flight_days = (flight_end - flight_start).days + 1

        for line_data in [by_id["L1"], by_id["L2"]]:
            # The tracking-row dict uses `planned_budget` (not `budget`) as
            # the column name in budget_tracking — see pacing._write_budget_tracking.
            assert line_data["planned_spend_to_date"] <= line_data["planned_budget"] * (
                days_elapsed / total_flight_days
            ), (
                f"planned_spend_to_date {line_data['planned_spend_to_date']} must not exceed "
                f"prorated budget {line_data['planned_budget']} * ({days_elapsed}/{total_flight_days})"
            )

    @patch("backend.services.pacing.date")
    @patch("backend.services.pacing.bq")
    @patch("backend.services.pacing._write_budget_tracking")
    @patch("backend.services.pacing._write_alerts")
    def test_different_flight_windows_same_platform(
        self, mock_alerts, mock_tracking, mock_bq, mock_date
    ):
        """Lines on the same platform but different flights should get spend
        only from their own flight window."""
        mock_date.today.return_value = date(2026, 4, 1)
        mock_date.fromisoformat.side_effect = lambda iso_str: date.fromisoformat(iso_str)
        mock_bq.table.side_effect = lambda name: f"`proj.ds.{name}`"
        mock_bq.string_param.side_effect = lambda n, v: (n, v)
        mock_bq.date_param.side_effect = lambda n, v: (n, v)

        lines = [
            _make_line("L1", "google_ads", 8000, date(2026, 3, 1), date(2026, 3, 31)),
            _make_line("L2", "google_ads", 6000, date(2026, 4, 1), date(2026, 4, 30)),
        ]

        def query_router(sql, params=None):
            if "media_plan_lines" in sql:
                return lines
            if "blocking_chart_weeks" in sql:
                return []
            if "SUM(spend)" in sql:
                if params:
                    param_dict = {p[0]: p[1] for p in params}
                    fs = param_dict.get("flight_start")
                    if fs == date(2026, 3, 1):
                        return [{"total_spend": 7500.0}]
                    if fs == date(2026, 4, 1):
                        return [{"total_spend": 200.0}]
                return [{"total_spend": 0.0}]
            return []

        mock_bq.run_query.side_effect = query_router

        result = run_pacing_for_project("TEST", date(2026, 4, 1))

        tracking_rows = mock_tracking.call_args[0][2]
        by_id = {r["line_id"]: r for r in tracking_rows}

        assert by_id["L1"]["actual_spend_to_date"] == 7500.0
        assert by_id["L2"]["actual_spend_to_date"] == 200.0

    @patch("backend.services.pacing.date")
    @patch("backend.services.pacing.bq")
    @patch("backend.services.pacing._write_budget_tracking")
    @patch("backend.services.pacing._write_alerts")
    def test_null_flight_dates_fallback(
        self, mock_alerts, mock_tracking, mock_bq, mock_date
    ):
        """Lines with NULL flight dates should still query spend without
        date filtering (backward compat)."""
        mock_date.today.return_value = date(2026, 4, 1)
        mock_date.fromisoformat.side_effect = lambda iso_str: date.fromisoformat(iso_str)
        mock_bq.table.side_effect = lambda name: f"`proj.ds.{name}`"
        mock_bq.string_param.side_effect = lambda n, v: (n, v)
        mock_bq.date_param.side_effect = lambda n, v: (n, v)

        # Lines with NULL flight dates get skipped in the pacing loop
        # (budget <= 0 or no flight_start/flight_end → continue), so the
        # spend query for their group fires but no tracking row is written.
        # The backward-compat path matters for the *spend query* not having
        # date params — we verify the query doesn't include date filters.
        lines = [
            _make_line("L1", "meta", 5000, None, None),
            _make_line("L2", "meta", 3000, date(2026, 4, 1), date(2026, 4, 30)),
        ]

        queries_executed = []

        def query_router(sql, params=None):
            queries_executed.append((sql, params))
            if "media_plan_lines" in sql:
                return lines
            if "blocking_chart_weeks" in sql:
                return []
            if "SUM(spend)" in sql:
                return [{"total_spend": 1000.0}]
            return []

        mock_bq.run_query.side_effect = query_router

        run_pacing_for_project("TEST", date(2026, 4, 1))

        # Find the spend queries
        spend_queries = [
            (sql, params) for sql, params in queries_executed
            if "SUM(spend)" in sql and "line_code" not in sql
        ]

        # Should have two spend queries: one for NULL-flight group, one for dated group
        assert len(spend_queries) == 2

        # The NULL-flight query should NOT have date params
        null_query = [
            (sql, params) for sql, params in spend_queries
            if "flight_start" not in sql
        ]
        assert len(null_query) == 1

        # The dated query SHOULD have date params
        dated_query = [
            (sql, params) for sql, params in spend_queries
            if "flight_start" in sql
        ]
        assert len(dated_query) == 1


class TestLineCodeWithDateFilter:
    """line_code matches should also be filtered by flight dates."""

    @patch("backend.services.pacing.date")
    @patch("backend.services.pacing.bq")
    @patch("backend.services.pacing._write_budget_tracking")
    @patch("backend.services.pacing._write_alerts")
    def test_line_code_spend_filtered_by_flight(
        self, mock_alerts, mock_tracking, mock_bq, mock_date
    ):
        """A line_code match should only count spend within the flight window."""
        mock_date.today.return_value = date(2026, 4, 15)
        mock_date.fromisoformat.side_effect = lambda iso_str: date.fromisoformat(iso_str)
        mock_bq.table.side_effect = lambda name: f"`proj.ds.{name}`"
        mock_bq.string_param.side_effect = lambda n, v: (n, v)
        mock_bq.date_param.side_effect = lambda n, v: (n, v)

        lines = [
            _make_line(
                "L1", "meta", 10000,
                date(2026, 4, 1), date(2026, 4, 30),
                line_code="META-001",
            ),
        ]

        def query_router(sql, params=None):
            if "media_plan_lines" in sql:
                return lines
            if "blocking_chart_weeks" in sql:
                return []
            if "SUM(spend)" in sql and "line_code" in sql:
                # line_code query should include date filters
                assert "flight_start" in sql, "line_code query must filter by flight dates"
                return [{"total_spend": 4500.0}]
            if "SUM(spend)" in sql:
                return [{"total_spend": 8000.0}]
            return []

        mock_bq.run_query.side_effect = query_router

        result = run_pacing_for_project("TEST", date(2026, 4, 15))

        tracking_rows = mock_tracking.call_args[0][2]
        # Should use the line_code spend ($4500), not platform group ($8000)
        assert tracking_rows[0]["actual_spend_to_date"] == 4500.0


class TestGracePeriodSpendDetection:
    """Regression (26023): a spend-bearing in-flight line attributed via the
    platform group-split fallback (no line_code match yet) must NOT be held in
    the 'pending' grace state. Staying pending zeroed planned_spend_to_date,
    dropped the line from the project pacing aggregate, and rendered the whole
    project 'DARK / no data' on the Summary tab despite real spend.
    """

    @patch("backend.services.pacing.date")
    @patch("backend.services.pacing.bq")
    @patch("backend.services.pacing._write_budget_tracking")
    @patch("backend.services.pacing._write_alerts")
    def test_group_split_spend_in_first_two_days_is_active(
        self, mock_alerts, mock_tracking, mock_bq, mock_date
    ):
        """Day 2 of flight, no line_code attribution, but the platform group
        has real spend → the line is 'active' with planned_spend_to_date > 0."""
        mock_date.today.return_value = date(2026, 6, 13)
        mock_date.fromisoformat.side_effect = lambda s: date.fromisoformat(s)
        mock_bq.table.side_effect = lambda name: f"`proj.ds.{name}`"
        mock_bq.string_param.side_effect = lambda n, v: (n, v)
        mock_bq.date_param.side_effect = lambda n, v: (n, v)

        # line_code=None → no line_code spend query fires; spend lands only via
        # the platform group-split fallback (the 26023 condition).
        lines = [
            _make_line("L1", "meta", 99362.06, date(2026, 6, 11), date(2026, 7, 19)),
        ]

        def query_router(sql, params=None):
            if "media_plan_lines" in sql:
                return lines
            if "blocking_chart_weeks" in sql:
                return []
            if "SUM(spend)" in sql:
                return [{"total_spend": 794.16}]
            return []

        mock_bq.run_query.side_effect = query_router

        run_pacing_for_project("26023", date(2026, 6, 13))

        row = mock_tracking.call_args[0][2][0]
        assert row["actual_spend_to_date"] == 794.16
        # The fix: spend present → active (not 'pending'), planned computed.
        assert row["line_status"] == "active"
        assert row["planned_spend_to_date"] > 0
        assert row["pacing_percentage"] > 0

    @patch("backend.services.pacing.date")
    @patch("backend.services.pacing.bq")
    @patch("backend.services.pacing._write_budget_tracking")
    @patch("backend.services.pacing._write_alerts")
    def test_zero_spend_in_grace_window_stays_pending(
        self, mock_alerts, mock_tracking, mock_bq, mock_date
    ):
        """Grace period preserved: a flight that started within 2 days with NO
        spend on any attribution path stays 'pending' with zero planned."""
        mock_date.today.return_value = date(2026, 6, 12)
        mock_date.fromisoformat.side_effect = lambda s: date.fromisoformat(s)
        mock_bq.table.side_effect = lambda name: f"`proj.ds.{name}`"
        mock_bq.string_param.side_effect = lambda n, v: (n, v)
        mock_bq.date_param.side_effect = lambda n, v: (n, v)

        lines = [
            _make_line("L1", "meta", 50000, date(2026, 6, 11), date(2026, 7, 19)),
        ]

        def query_router(sql, params=None):
            if "media_plan_lines" in sql:
                return lines
            if "blocking_chart_weeks" in sql:
                return []
            if "SUM(spend)" in sql:
                return [{"total_spend": 0.0}]
            return []

        mock_bq.run_query.side_effect = query_router

        run_pacing_for_project("TESTGRACE", date(2026, 6, 12))

        row = mock_tracking.call_args[0][2][0]
        assert row["actual_spend_to_date"] == 0.0
        assert row["line_status"] == "pending"
        assert row["planned_spend_to_date"] == 0.0

    @patch("backend.services.pacing.date")
    @patch("backend.services.pacing.bq")
    @patch("backend.services.pacing._write_budget_tracking")
    @patch("backend.services.pacing._write_alerts")
    def test_no_early_alert_for_group_split_line_without_linecode_spend(
        self, mock_alerts, mock_tracking, mock_bq, mock_date
    ):
        """A line now 'active' via group-split in its first 2 days must still
        suppress pacing alerts (data-lag) when there is no line_code spend —
        preserving the original grace-period alert behavior so the fix doesn't
        introduce day-1/2 underpacing noise on every new flight."""
        mock_date.today.return_value = date(2026, 6, 13)
        mock_date.fromisoformat.side_effect = lambda s: date.fromisoformat(s)
        mock_bq.table.side_effect = lambda name: f"`proj.ds.{name}`"
        mock_bq.string_param.side_effect = lambda n, v: (n, v)
        mock_bq.date_param.side_effect = lambda n, v: (n, v)

        # Tiny spend vs large planned → would be a critical underpace if alerted.
        lines = [
            _make_line("L1", "meta", 99362.06, date(2026, 6, 11), date(2026, 7, 19)),
        ]

        def query_router(sql, params=None):
            if "media_plan_lines" in sql:
                return lines
            if "blocking_chart_weeks" in sql:
                return []
            if "SUM(spend)" in sql:
                return [{"total_spend": 10.0}]
            return []

        mock_bq.run_query.side_effect = query_router

        result = run_pacing_for_project("26023", date(2026, 6, 13))

        # Active + low pacing, but inside the 2-day data-lag window with no
        # line_code spend → no alerts fired.
        assert result["alerts"] == 0


class TestGridIgnoredForDayCount:
    """Regression (26023 Meta): day counts come from the authoritative flight
    dates, NOT the blocking chart. A Monday-aligned grid whose active weeks lag
    the flight start must not change how many days a line has been active — doing
    so understated the baseline and produced false overpacing (read 250% on day
    5). The blocking chart is a within-week weighting input only.
    """

    # Blocking grid whose first ACTIVE week (Jun 15) starts the day AFTER the
    # as-of date (Jun 14), while the flight itself started Jun 11.
    _WEEKS = [
        {"line_id": "L1", "week_start": date(2026, 6, 1), "is_active": False},
        {"line_id": "L1", "week_start": date(2026, 6, 8), "is_active": False},
        {"line_id": "L1", "week_start": date(2026, 6, 15), "is_active": True},
        {"line_id": "L1", "week_start": date(2026, 6, 22), "is_active": True},
        {"line_id": "L1", "week_start": date(2026, 6, 29), "is_active": True},
        {"line_id": "L1", "week_start": date(2026, 7, 6), "is_active": True},
        {"line_id": "L1", "week_start": date(2026, 7, 13), "is_active": True},
    ]

    @patch("backend.services.pacing.date")
    @patch("backend.services.pacing.bq")
    @patch("backend.services.pacing._write_budget_tracking")
    @patch("backend.services.pacing._write_alerts")
    def test_grid_lag_with_spend_uses_flight_dates(
        self, mock_alerts, mock_tracking, mock_bq, mock_date
    ):
        """Grid active weeks start after flight_start, but the day count comes
        from the flight dates regardless: 4 of 39 days elapsed →
        19500 / 39 * 4 = 2000, so the line paces against a real baseline."""
        mock_date.today.return_value = date(2026, 6, 14)
        mock_date.fromisoformat.side_effect = lambda s: date.fromisoformat(s)
        mock_bq.table.side_effect = lambda name: f"`proj.ds.{name}`"
        mock_bq.string_param.side_effect = lambda n, v: (n, v)
        mock_bq.date_param.side_effect = lambda n, v: (n, v)

        lines = [
            _make_line("L1", "meta", 19500, date(2026, 6, 11), date(2026, 7, 19)),
        ]

        def query_router(sql, params=None):
            if "media_plan_lines" in sql:
                return lines
            if "blocking_chart_weeks" in sql:
                return self._WEEKS
            if "SUM(spend)" in sql:
                return [{"total_spend": 802.39}]
            return []

        mock_bq.run_query.side_effect = query_router

        run_pacing_for_project("26023", date(2026, 6, 14))

        row = mock_tracking.call_args[0][2][0]
        assert row["line_status"] == "active"
        assert row["actual_spend_to_date"] == 802.39
        # 4 of 39 flight days elapsed → 19500 / 39 * 4 = 2000.0
        assert row["planned_spend_to_date"] == pytest.approx(19500 / 39 * 4)
        assert row["pacing_percentage"] > 0

    @patch("backend.services.pacing.date")
    @patch("backend.services.pacing.bq")
    @patch("backend.services.pacing._write_budget_tracking")
    @patch("backend.services.pacing._write_alerts")
    def test_grid_lag_without_spend_still_uses_flight_baseline(
        self, mock_alerts, mock_tracking, mock_bq, mock_date
    ):
        """Same lagging grid, NO spend: the baseline still comes from the flight
        dates (4 of 39 → 2000), so an in-flight line that isn't delivering reads
        as honestly under-pacing (0% of a real baseline) rather than being hidden
        by the grid. Flight dates, not the grid, decide the line is live."""
        mock_date.today.return_value = date(2026, 6, 14)
        mock_date.fromisoformat.side_effect = lambda s: date.fromisoformat(s)
        mock_bq.table.side_effect = lambda name: f"`proj.ds.{name}`"
        mock_bq.string_param.side_effect = lambda n, v: (n, v)
        mock_bq.date_param.side_effect = lambda n, v: (n, v)

        lines = [
            _make_line("L1", "meta", 19500, date(2026, 6, 11), date(2026, 7, 19)),
        ]

        def query_router(sql, params=None):
            if "media_plan_lines" in sql:
                return lines
            if "blocking_chart_weeks" in sql:
                return self._WEEKS
            if "SUM(spend)" in sql:
                return [{"total_spend": 0.0}]
            return []

        mock_bq.run_query.side_effect = query_router

        run_pacing_for_project("26023", date(2026, 6, 14))

        row = mock_tracking.call_args[0][2][0]
        assert row["actual_spend_to_date"] == 0.0
        # 4 of 39 flight days → a real baseline, NOT 0 (the grid no longer
        # suppresses it). actual 0 against a real baseline = honest 0% under-pace.
        assert row["planned_spend_to_date"] == pytest.approx(19500 / 39 * 4)
        assert row["pacing_percentage"] == 0.0

    @patch("backend.services.pacing.date")
    @patch("backend.services.pacing.bq")
    @patch("backend.services.pacing._write_budget_tracking")
    @patch("backend.services.pacing._write_alerts")
    def test_grid_partial_lag_uses_flight_dates_not_grid(
        self, mock_alerts, mock_tracking, mock_bq, mock_date
    ):
        """The exact 26023 Meta bug. As-of Jun 15 the lagging grid yields
        elapsed=1 of 35 active days, so the OLD grid-based baseline was
        19500/35*1 = 557.14, reading ~250% over. Day counts now come from the
        flight dates: 5 of 39 → 19500/39*5 = 2500, a realistic baseline (~56%)."""
        mock_date.today.return_value = date(2026, 6, 15)
        mock_date.fromisoformat.side_effect = lambda s: date.fromisoformat(s)
        mock_bq.table.side_effect = lambda name: f"`proj.ds.{name}`"
        mock_bq.string_param.side_effect = lambda n, v: (n, v)
        mock_bq.date_param.side_effect = lambda n, v: (n, v)

        lines = [
            _make_line("L1", "meta", 19500, date(2026, 6, 11), date(2026, 7, 19)),
        ]

        def query_router(sql, params=None):
            if "media_plan_lines" in sql:
                return lines
            if "blocking_chart_weeks" in sql:
                return self._WEEKS
            if "SUM(spend)" in sql:
                return [{"total_spend": 1391.47}]
            return []

        mock_bq.run_query.side_effect = query_router

        run_pacing_for_project("26023", date(2026, 6, 15))

        row = mock_tracking.call_args[0][2][0]
        # Flight-date baseline (5 of 39 → 2500), NOT the grid's 19500/35*1=557.14.
        assert row["planned_spend_to_date"] == pytest.approx(19500 / 39 * 5)
        assert row["planned_spend_to_date"] != pytest.approx(19500 / 35 * 1)
        # Realistic pacing (~55.7%), not the false ~250%.
        assert row["pacing_percentage"] == pytest.approx(
            1391.47 / (19500 / 39 * 5) * 100, abs=0.1
        )
        assert row["pacing_percentage"] < 100


# ── Test: _float helper ──────────────────────────────────────────


class TestFloatHelper:
    def test_none_returns_default(self):
        assert _float(None) == 0.0

    def test_float_passthrough(self):
        assert _float(3.14) == 3.14

    def test_string_numeric(self):
        assert _float("42.5") == 42.5

    def test_decimal(self):
        from decimal import Decimal
        assert _float(Decimal("99.9")) == 99.9


# ── Test: rollup denominator guard (Finding 2) ────────────────────


class TestRollupDenominatorGuard:
    """Finding 2: a line with spend but planned_spend_to_date == 0 must not
    inflate overall_pacing_percentage. The line's spend still appears in the
    displayed total_actual_to_date; only the pacing ratio excludes it."""

    def _brow(self, **over):
        base = {
            "date": date(2026, 6, 14),
            "line_id": "L", "line_code": "#01", "platform_id": "meta",
            "channel_category": "Digital", "line_status": "active",
            "planned_budget": 1000.0, "planned_spend_to_date": 1000.0,
            "actual_spend_to_date": 1000.0, "remaining_budget": 0.0,
            "remaining_days": 10, "pacing_percentage": 100.0,
            "daily_budget_required": 0.0, "is_over_pacing": False,
            "is_under_pacing": False, "bundle_id": None, "bundle_role": None,
            "audience_name": "A", "flight_start": date(2026, 6, 1),
            "flight_end": date(2026, 6, 30), "sheet_id": None,
            "phase_label": None, "phase_display_order": None,
        }
        base.update(over)
        return base

    def _bq(self, rows):
        class _BQ:
            @staticmethod
            def table(name):
                return f"`proj.ds.{name}`"

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
                    return [{"project_code": "TEST", "net_budget": 100000.0}]
                if "budget_tracking" in sql:
                    return rows
                return []
        return _BQ

    def test_zero_baseline_line_excluded_from_pct_but_kept_in_total(self):
        rows = [
            self._brow(line_id="healthy",
                       planned_spend_to_date=1000.0, actual_spend_to_date=1000.0),
            # spend but no baseline (e.g. degenerate dates the floor can't fix)
            self._brow(line_id="zerobase",
                       planned_spend_to_date=0.0, actual_spend_to_date=500.0),
        ]
        with patch.object(pacing_router, "bq", self._bq(rows)), \
                patch.object(pacing_router, "_query_untracked_platform_spend",
                             return_value=[]):
            resp = asyncio.run(pacing_router.get_pacing("TEST"))

        # 1000 / 1000 = 100%, NOT 1500 / 1000 = 150% (the inflated, pre-fix value)
        assert resp.overall_pacing_percentage == 100.0
        # the zero-baseline line's spend is still surfaced in the total
        assert resp.total_actual_to_date == 1500.0
        assert resp.total_planned_to_date == 1000.0


# ── Test: grid-outside-window fallback (Finding 1, consumer) ──────


class TestGridOutsideWindowFallback:
    """Finding 1 (consumer side): when a line's blocking-chart weeks fall
    entirely outside its flight window (0 active days), pacing falls back to the
    flight-span split instead of collapsing the baseline to 0 — even with no
    spend yet (which is what distinguishes this from the spend-only floor)."""

    _WEEKS_IN_MAY = [
        {"line_id": "L1", "week_start": date(2026, 5, 4), "is_active": True},
        {"line_id": "L1", "week_start": date(2026, 5, 11), "is_active": True},
    ]

    @patch("backend.services.pacing.date")
    @patch("backend.services.pacing.bq")
    @patch("backend.services.pacing._write_budget_tracking")
    @patch("backend.services.pacing._write_alerts")
    def test_grid_outside_window_uses_flight_span(
        self, mock_alerts, mock_tracking, mock_bq, mock_date
    ):
        mock_date.today.return_value = date(2026, 6, 14)
        mock_date.fromisoformat.side_effect = lambda s: date.fromisoformat(s)
        mock_bq.table.side_effect = lambda name: f"`proj.ds.{name}`"
        mock_bq.string_param.side_effect = lambda n, v: (n, v)
        mock_bq.date_param.side_effect = lambda n, v: (n, v)

        lines = [
            _make_line("L1", "meta", 3000, date(2026, 6, 1), date(2026, 6, 30)),
        ]

        def query_router(sql, params=None):
            if "media_plan_lines" in sql:
                return lines
            if "blocking_chart_weeks" in sql:
                return self._WEEKS_IN_MAY
            if "SUM(spend)" in sql:
                return [{"total_spend": 0.0}]
            return []

        mock_bq.run_query.side_effect = query_router
        run_pacing_for_project("TEST", date(2026, 6, 14))

        row = mock_tracking.call_args[0][2][0]
        assert row["line_status"] == "active"
        assert row["actual_spend_to_date"] == 0.0
        # 14 of 30 flight days elapsed → 3000 / 30 * 14 = 1400 (NOT 0)
        assert row["planned_spend_to_date"] == pytest.approx(3000 / 30 * 14)


# ── Test: pacing spend correctness (Findings 5, 6, 7) ─────────────


class TestPacingSpendCorrectness:
    @patch("backend.services.pacing.date")
    @patch("backend.services.pacing.bq")
    @patch("backend.services.pacing._write_budget_tracking")
    @patch("backend.services.pacing._write_alerts")
    def test_group_split_excludes_line_code_claimed_spend(
        self, mock_alerts, mock_tracking, mock_bq, mock_date
    ):
        """Finding 5: line A claims $600 by line_code; line B (no line_code) must
        get only the RESIDUAL ($1000 group total - $600 = $400), not a budget
        share of the full $1000 (which would double-count A's spend)."""
        mock_date.today.return_value = date(2026, 4, 15)
        mock_date.fromisoformat.side_effect = lambda s: date.fromisoformat(s)
        mock_bq.table.side_effect = lambda name: f"`proj.ds.{name}`"
        mock_bq.string_param.side_effect = lambda n, v: (n, v)
        mock_bq.date_param.side_effect = lambda n, v: (n, v)

        lines = [
            _make_line("A", "meta", 5000, date(2026, 4, 1), date(2026, 4, 30),
                       line_code="MA"),
            _make_line("B", "meta", 5000, date(2026, 4, 1), date(2026, 4, 30)),
        ]

        def query_router(sql, params=None):
            if "media_plan_lines" in sql:
                return lines
            if "blocking_chart_weeks" in sql:
                return []
            if "SUM(spend)" in sql and "line_code" in sql:
                return [{"total_spend": 600.0, "first_spend_date": date(2026, 4, 10)}]
            if "SUM(spend)" in sql:
                return [{"total_spend": 1000.0}]
            return []

        mock_bq.run_query.side_effect = query_router
        run_pacing_for_project("TEST", date(2026, 4, 15))

        by_id = {r["line_id"]: r for r in mock_tracking.call_args[0][2]}
        assert by_id["A"]["actual_spend_to_date"] == 600.0
        assert by_id["B"]["actual_spend_to_date"] == pytest.approx(400.0)
        # No double-count: attributed total equals the real platform spend.
        assert (by_id["A"]["actual_spend_to_date"]
                + by_id["B"]["actual_spend_to_date"]) == pytest.approx(1000.0)

    @patch("backend.services.pacing.date")
    @patch("backend.services.pacing.bq")
    @patch("backend.services.pacing._write_budget_tracking")
    @patch("backend.services.pacing._write_alerts")
    def test_blocking_weeks_query_dedups_by_sync_version(
        self, mock_alerts, mock_tracking, mock_bq, mock_date
    ):
        """Finding 6: the blocking_chart_weeks read dedups by latest sync_version
        (mirroring the media_plan_lines read), so a failed old-version cleanup
        can't double the active-day count."""
        mock_date.today.return_value = date(2026, 6, 14)
        mock_date.fromisoformat.side_effect = lambda s: date.fromisoformat(s)
        mock_bq.table.side_effect = lambda name: f"`proj.ds.{name}`"
        mock_bq.string_param.side_effect = lambda n, v: (n, v)
        mock_bq.date_param.side_effect = lambda n, v: (n, v)

        captured = {}

        def query_router(sql, params=None):
            if "blocking_chart_weeks" in sql:
                captured["sql"] = sql
                return []
            if "media_plan_lines" in sql:
                return [_make_line("L1", "meta", 1000,
                                   date(2026, 6, 1), date(2026, 6, 30))]
            if "SUM(spend)" in sql:
                return [{"total_spend": 0.0}]
            return []

        mock_bq.run_query.side_effect = query_router
        run_pacing_for_project("TEST", date(2026, 6, 14))

        sql = captured.get("sql", "")
        assert "ROW_NUMBER" in sql
        assert "sync_version" in sql
        assert "_rn = 1" in sql

    @patch("backend.services.pacing.date")
    @patch("backend.services.pacing.bq")
    @patch("backend.services.pacing._write_budget_tracking")
    @patch("backend.services.pacing._write_alerts")
    def test_bundle_window_spans_all_members(
        self, mock_alerts, mock_tracking, mock_bq, mock_date
    ):
        """Finding 7: the bundle spend window spans min(member start) to
        max(member end), not just the parent's flight, so a child running past
        the parent's end still has its delivery counted."""
        mock_date.today.return_value = date(2026, 6, 20)
        mock_date.fromisoformat.side_effect = lambda s: date.fromisoformat(s)
        mock_bq.table.side_effect = lambda name: f"`proj.ds.{name}`"
        mock_bq.string_param.side_effect = lambda n, v: (n, v)
        mock_bq.date_param.side_effect = lambda n, v: (n, v)
        mock_bq.array_param.side_effect = lambda n, t, v: (n, v)

        parent = _make_line("P", "meta", 5000, date(2026, 6, 1),
                            date(2026, 6, 30), line_code="#01")
        parent["bundle_id"] = "B1"
        parent["bundle_role"] = "suggested_parent"
        child = _make_line("C", "meta", None, date(2026, 6, 1),
                           date(2026, 7, 7), line_code="#02")
        child["bundle_id"] = "B1"
        child["bundle_role"] = "suggested_child"
        lines = [parent, child]

        captured = {}

        def query_router(sql, params=None):
            if "media_plan_lines" in sql:
                return lines
            if "blocking_chart_weeks" in sql:
                return []
            if "member_codes" in sql:  # the bundle aggregation query
                pd = {p[0]: p[1] for p in (params or [])}
                captured["fs"] = pd.get("flight_start")
                captured["fe"] = pd.get("flight_end")
                return [{"total_spend": 900.0}]
            if "SUM(spend)" in sql:
                return [{"total_spend": 0.0}]
            return []

        mock_bq.run_query.side_effect = query_router
        run_pacing_for_project("TEST", date(2026, 6, 20))

        # Window spans the child's later end (Jul 7), not the parent's Jun 30.
        assert captured.get("fs") == date(2026, 6, 1)
        assert captured.get("fe") == date(2026, 7, 7)


# ── Test: race fixes (sync-lock skip + spend-without-baseline) ─────


class TestRaceFixes:
    @patch("backend.services.pacing.date")
    @patch("backend.services.pacing.bq")
    @patch("backend.services.pacing._write_budget_tracking")
    @patch("backend.services.pacing._write_alerts")
    def test_pacing_skips_when_sync_lock_held(
        self, mock_alerts, mock_tracking, mock_bq, mock_date
    ):
        """(a) A pace skips a project whose media-plan sync lock is held, leaving
        the prior budget_tracking snapshot untouched (no write, no alerts)."""
        mock_date.today.return_value = date(2026, 6, 14)
        mock_date.fromisoformat.side_effect = lambda s: date.fromisoformat(s)
        mock_bq.table.side_effect = lambda name: f"`proj.ds.{name}`"
        mock_bq.string_param.side_effect = lambda n, v: (n, v)
        mock_bq.date_param.side_effect = lambda n, v: (n, v)

        def qr(sql, params=None):
            if "pacing_sync_locks" in sql:
                return [{"lock": 1}]  # a live lock → sync in progress
            return []

        mock_bq.run_query.side_effect = qr
        result = run_pacing_for_project("26023", date(2026, 6, 14))

        assert result.get("skipped") == "sync_in_progress"
        assert result["lines_processed"] == 0
        assert not mock_tracking.called
        assert not mock_alerts.called

    @patch("backend.services.pacing.date")
    @patch("backend.services.pacing.bq")
    @patch("backend.services.pacing._write_budget_tracking")
    @patch("backend.services.pacing._write_alerts")
    def test_no_lock_proceeds(self, mock_alerts, mock_tracking, mock_bq, mock_date):
        """No lock → pacing proceeds (here to the clean no-lines early return)."""
        mock_date.today.return_value = date(2026, 6, 14)
        mock_date.fromisoformat.side_effect = lambda s: date.fromisoformat(s)
        mock_bq.table.side_effect = lambda name: f"`proj.ds.{name}`"
        mock_bq.string_param.side_effect = lambda n, v: (n, v)
        mock_bq.date_param.side_effect = lambda n, v: (n, v)
        mock_bq.run_query.side_effect = lambda sql, params=None: []
        result = run_pacing_for_project("26023", date(2026, 6, 14))
        assert result.get("skipped") != "sync_in_progress"

    @patch("backend.services.pacing.date")
    @patch("backend.services.pacing.bq")
    @patch("backend.services.pacing._write_budget_tracking")
    @patch("backend.services.pacing._write_alerts")
    def test_replay_ignores_lock(self, mock_alerts, mock_tracking, mock_bq, mock_date):
        """Retrospective replay (skip_writes=True) is read-only and must not be
        blocked by a sync lock — the lock is never even checked."""
        mock_date.today.return_value = date(2026, 6, 14)
        mock_date.fromisoformat.side_effect = lambda s: date.fromisoformat(s)
        mock_bq.table.side_effect = lambda name: f"`proj.ds.{name}`"
        mock_bq.string_param.side_effect = lambda n, v: (n, v)
        mock_bq.date_param.side_effect = lambda n, v: (n, v)
        seen = {"lock_query": False}

        def qr(sql, params=None):
            if "pacing_sync_locks" in sql:
                seen["lock_query"] = True
                return [{"lock": 1}]
            return []

        mock_bq.run_query.side_effect = qr
        result = run_pacing_for_project("26023", date(2026, 6, 14), skip_writes=True)
        assert result.get("skipped") != "sync_in_progress"
        assert seen["lock_query"] is False

    # ── (c) spend-without-baseline surfaced in get_pacing ───────────
    def _brow(self, **over):
        base = {
            "date": date(2026, 6, 14),
            "line_id": "L", "line_code": "#01", "platform_id": "meta",
            "channel_category": "Digital", "line_status": "active",
            "planned_budget": 1000.0, "planned_spend_to_date": 1000.0,
            "actual_spend_to_date": 1000.0, "remaining_budget": 0.0,
            "remaining_days": 10, "pacing_percentage": 100.0,
            "daily_budget_required": 0.0, "is_over_pacing": False,
            "is_under_pacing": False, "bundle_id": None, "bundle_role": None,
            "audience_name": "A", "flight_start": date(2026, 6, 1),
            "flight_end": date(2026, 6, 30), "sheet_id": None,
            "phase_label": None, "phase_display_order": None,
        }
        base.update(over)
        return base

    def _bq(self, rows):
        class _BQ:
            @staticmethod
            def table(name):
                return f"`proj.ds.{name}`"

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
                    return [{"project_code": "TEST", "net_budget": 100000.0}]
                if "budget_tracking" in sql:
                    return rows
                return []
        return _BQ

    def test_zero_baseline_spend_is_surfaced(self):
        rows = [
            self._brow(line_id="healthy",
                       planned_spend_to_date=1000.0, actual_spend_to_date=1000.0),
            self._brow(line_id="zerobase",
                       planned_spend_to_date=0.0, actual_spend_to_date=500.0),
        ]
        with patch.object(pacing_router, "bq", self._bq(rows)), \
                patch.object(pacing_router, "_query_untracked_platform_spend",
                             return_value=[]):
            resp = asyncio.run(pacing_router.get_pacing("TEST"))
        # surfaced, not hidden
        assert resp.lines_without_baseline == 1
        assert resp.spend_without_baseline == 500.0
        # still excluded from the % (1000/1000 = 100, not 1500/1000 = 150)
        assert resp.overall_pacing_percentage == 100.0


# ── Ad-set-name → line audience attribution ─────────────────────────


class TestAudienceTokens:
    """The token normaliser underpinning ad-set → line matching."""

    def test_singularises_and_drops_numbers(self):
        assert _audience_tokens("List Lookalikes") == {"list", "lookalike"}

    def test_qualifier_tokens_kept_and_singularised(self):
        assert _audience_tokens("PNE Fan Zone Attendees") == {
            "pne", "fan", "zone", "attendee",
        }

    def test_none_and_empty(self):
        assert _audience_tokens(None) == set()
        assert _audience_tokens("   -  ") == set()

    def test_adset_name_strips_structural_tokens(self):
        toks = _audience_tokens("01 - 26023 Sierra Club - Conversion - Lookalike List")
        assert {"lookalike", "list"} <= toks
        assert "01" not in toks and "26023" not in toks


class TestMatchAdsetToLine:
    """Confident, unambiguous ad-set → line audience matching."""

    CANDS = [
        {"line_id": "L0", "audience_name": "List Lookalikes"},
        {"line_id": "L1", "audience_name": "Member List Match"},
        {"line_id": "L2", "audience_name": "PNE Fan Zone Attendees"},
    ]

    def test_lookalike_adset_matches_lookalike_line(self):
        assert _match_adset_to_line_id(
            "01 - 26023 Sierra Club - Conversion - Lookalike List", self.CANDS
        ) == "L0"

    def test_fan_zone_adset_matches_fan_zone_line(self):
        assert _match_adset_to_line_id(
            "02 - 26023 Sierra Club - Conversion - Fan Zone", self.CANDS
        ) == "L2"

    def test_reach_adset_matches_nothing(self):
        # No plan line is about "reach"/"awareness"; must not false-match.
        assert _match_adset_to_line_id(
            "01 - 26023 Sierra Club BC FIFA Reach", self.CANDS
        ) is None

    def test_full_match_beats_single_generic_token(self):
        # "Member List Match …" shares only 'list' with List Lookalikes (0.5,
        # 1 token) but fully matches Member List Match — the true line wins.
        assert _match_adset_to_line_id(
            "03 - 26023 Sierra Club - Member List Match Audience", self.CANDS
        ) == "L1"

    def test_single_generic_token_alone_does_not_match(self):
        # An ad set sharing only the generic 'list' token with one line, with no
        # stronger competitor, stays unmatched (needs ≥2 tokens or a full match).
        cands = [{"line_id": "X", "audience_name": "List Lookalikes"}]
        assert _match_adset_to_line_id("Some Random List Of Things", cands) is None

    def test_ambiguous_tie_returns_none(self):
        cands = [
            {"line_id": "A", "audience_name": "Fan Zone"},
            {"line_id": "B", "audience_name": "Fan Zone"},
        ]
        assert _match_adset_to_line_id("Fan Zone", cands) is None

    def test_empty_inputs(self):
        assert _match_adset_to_line_id("", self.CANDS) is None
        assert _match_adset_to_line_id(None, self.CANDS) is None


class TestAudienceAttributionInPacing:
    """End-to-end: audience-matched ad sets are measured directly; unmatched
    ad-set spend still flows through the budget-weight residual (no spend lost).
    Mirrors the real 26023 Meta case."""

    @patch("backend.services.pacing.date")
    @patch("backend.services.pacing.bq")
    @patch("backend.services.pacing._write_budget_tracking")
    @patch("backend.services.pacing._write_alerts")
    def test_meta_split_by_audience_with_residual_fallback(
        self, mock_alerts, mock_tracking, mock_bq, mock_date
    ):
        mock_date.today.return_value = date(2026, 6, 14)
        mock_date.fromisoformat.side_effect = lambda s: date.fromisoformat(s)
        mock_bq.table.side_effect = lambda name: f"`proj.ds.{name}`"
        mock_bq.string_param.side_effect = lambda n, v: (n, v)
        mock_bq.date_param.side_effect = lambda n, v: (n, v)
        mock_bq.array_param.side_effect = lambda n, t, v: (n, v)

        fs, fe = date(2026, 6, 4), date(2026, 7, 15)
        lines = [
            _make_line("L0", "meta", 19500, fs, fe, line_code="#01"),
            _make_line("L1", "meta", 4090, fs, fe, line_code="#02"),
        ]
        lines[0]["audience_name"] = "List Lookalikes"
        lines[1]["audience_name"] = "Member List Match"

        def query_router(sql, params=None):
            if "media_plan_lines" in sql:
                # Regression guard: the matcher can only run if the lines query
                # actually selects audience_name (it silently no-ops without it).
                assert "audience_name" in sql
                return lines
            if "blocking_chart_weeks" in sql:
                return []
            # My audience query — check ad_set_name BEFORE the SUM(spend) catch.
            if "ad_set_name" in sql:
                return [
                    {"ad_set_name": "01 - 26023 Sierra Club - Conversion - Lookalike List",
                     "spend": 1391.47},
                    {"ad_set_name": "01 - 26023 Sierra Club BC FIFA Reach",
                     "spend": 227.41},
                ]
            # line_code lookups hit the view; return no line_code-attributed spend
            # so attribution must fall to the audience tier.
            if "vw_fact_digital_daily" in sql or "line_codes" in sql:
                return [{"total_spend": 0.0, "first_spend_date": None}]
            if "SUM(spend)" in sql:  # platform group total
                return [{"total_spend": 1618.88}]
            return []

        mock_bq.run_query.side_effect = query_router

        run_pacing_for_project("26023", date(2026, 6, 14))

        rows = mock_tracking.call_args[0][2]
        by_id = {r["line_id"]: r for r in rows}

        # L0 measured directly from its matching ad set.
        assert by_id["L0"]["actual_spend_to_date"] == 1391.47
        # L1 has no matching ad set; the unmatched "FIFA Reach" spend ($227.41)
        # flows through the residual budget-weight split to the only unattributed
        # line — nothing is dropped.
        assert by_id["L1"]["actual_spend_to_date"] == pytest.approx(227.41, abs=0.01)
        # Total spend is conserved (matches the platform group total).
        total = by_id["L0"]["actual_spend_to_date"] + by_id["L1"]["actual_spend_to_date"]
        assert total == pytest.approx(1618.88, abs=0.01)
