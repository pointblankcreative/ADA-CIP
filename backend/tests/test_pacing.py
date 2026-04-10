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


def _mock_run_query(calls_map):
    """Return a side_effect function that matches SQL fragments to results."""
    def side_effect(sql, params=None):
        for fragment, result in calls_map:
            if fragment in sql:
                # If result is callable, call it with params for dynamic matching
                if callable(result):
                    return result(sql, params)
                return result
        return []
    return side_effect


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
        mock_date.today.return_value = date(2026, 4, 9)
        mock_date.fromisoformat = date.fromisoformat
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

        result = run_pacing_for_project("25042")

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
        mock_date.fromisoformat = date.fromisoformat
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

        result = run_pacing_for_project("25042")

        tracking_rows = mock_tracking.call_args[0][2]
        by_id = {r["line_id"]: r for r in tracking_rows}

        total_budget = 12742 + 5500
        expected_l1 = round(3000.0 * (12742 / total_budget), 2)
        expected_l2 = round(3000.0 * (5500 / total_budget), 2)

        assert by_id["L1"]["actual_spend_to_date"] == expected_l1
        assert by_id["L2"]["actual_spend_to_date"] == expected_l2

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
        mock_date.fromisoformat = date.fromisoformat
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

        result = run_pacing_for_project("TEST")

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
        mock_date.fromisoformat = date.fromisoformat
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

        run_pacing_for_project("TEST")

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
        mock_date.fromisoformat = date.fromisoformat
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

        result = run_pacing_for_project("TEST")

        tracking_rows = mock_tracking.call_args[0][2]
        # Should use the line_code spend ($4500), not platform group ($8000)
        assert tracking_rows[0]["actual_spend_to_date"] == 4500.0


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
