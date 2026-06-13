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

from backend.services.pacing import run_pacing_for_project, _float


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
