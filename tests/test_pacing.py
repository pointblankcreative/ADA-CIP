"""Tests for pacing engine — line status and grace period logic."""

from datetime import date, timedelta
from unittest.mock import patch

import pytest

from backend.services.pacing import _count_active_days


class TestCountActiveDays:
    """Verify _count_active_days handles edge cases correctly."""

    def test_flight_not_started(self):
        """If today is before flight_start, elapsed_active_days should be 0."""
        tomorrow = date.today() + timedelta(days=1)
        flight_end = tomorrow + timedelta(days=21)
        weeks = [{"week_start": tomorrow.isoformat(), "is_active": True}]

        total, elapsed = _count_active_days(weeks, tomorrow, flight_end)
        # elapsed should be 0 since the flight hasn't started
        assert elapsed == 0

    def test_flight_started_today(self):
        """If today == flight_start, elapsed_active_days should be 1."""
        today = date.today()
        flight_end = today + timedelta(days=21)
        weeks = [{"week_start": today.isoformat(), "is_active": True}]

        total, elapsed = _count_active_days(weeks, today, flight_end)
        assert elapsed == 1

    def test_flight_completed(self):
        """Elapsed should equal total when flight is fully in the past."""
        start = date.today() - timedelta(days=30)
        end = date.today() - timedelta(days=1)
        weeks = [{"week_start": start.isoformat(), "is_active": True}]

        total, elapsed = _count_active_days(weeks, start, end)
        assert elapsed == total


class TestLineStatus:
    """Verify line_status is correctly determined in run_pacing_for_project."""

    def _mock_pacing_run(self, flight_start: date, flight_end: date):
        """Run pacing with a single mocked line and return the tracking row."""
        from unittest.mock import MagicMock

        line_id = "test-line-001"
        line = {
            "line_id": line_id,
            "line_code": "TEST",
            "platform_id": "meta",
            "channel_category": "Digital",
            "budget": 10000.0,
            "flight_start": flight_start.isoformat(),
            "flight_end": flight_end.isoformat(),
        }
        blocking_weeks = [{
            "line_id": line_id,
            "week_start": flight_start.isoformat(),
            "is_active": True,
        }]

        # Capture what gets written to budget_tracking
        captured_rows = []

        def fake_write(project_code, as_of, rows):
            captured_rows.extend(rows)

        with patch("backend.services.pacing.bq") as mock_bq, \
             patch("backend.services.pacing._write_budget_tracking", side_effect=fake_write), \
             patch("backend.services.pacing._write_alerts"):

            mock_bq.table.return_value = "dummy_table"
            mock_bq.string_param.return_value = MagicMock()
            mock_bq.scalar_param.return_value = MagicMock()

            # First query: media plan lines
            # Second query: blocking chart weeks
            # Third query: spend by platform
            # Fourth query: spend by line_code
            call_count = [0]

            def mock_run_query(sql, params=None):
                call_count[0] += 1
                if call_count[0] == 1:
                    return [line]
                elif call_count[0] == 2:
                    return blocking_weeks
                else:
                    return []

            mock_bq.run_query.side_effect = mock_run_query

            from backend.services.pacing import run_pacing_for_project
            run_pacing_for_project("TEST01")

        return captured_rows[0] if captured_rows else None

    def test_not_started_flight(self):
        """Flight starting tomorrow should have status 'not_started'."""
        tomorrow = date.today() + timedelta(days=1)
        flight_end = tomorrow + timedelta(days=21)
        row = self._mock_pacing_run(tomorrow, flight_end)
        assert row is not None
        assert row["line_status"] == "not_started"
        assert row["planned_spend_to_date"] == 0.0

    def test_pending_flight_started_today(self):
        """Flight starting today should have status 'pending'."""
        today = date.today()
        flight_end = today + timedelta(days=21)
        row = self._mock_pacing_run(today, flight_end)
        assert row is not None
        assert row["line_status"] == "pending"
        assert row["planned_spend_to_date"] == 0.0

    def test_active_flight(self):
        """Flight that started 5 days ago should have status 'active'."""
        start = date.today() - timedelta(days=5)
        end = date.today() + timedelta(days=16)
        row = self._mock_pacing_run(start, end)
        assert row is not None
        assert row["line_status"] == "active"
        assert row["planned_spend_to_date"] > 0

    def test_completed_flight(self):
        """Flight that ended yesterday should have status 'completed'."""
        start = date.today() - timedelta(days=22)
        end = date.today() - timedelta(days=1)
        row = self._mock_pacing_run(start, end)
        assert row is not None
        assert row["line_status"] == "completed"

    def test_pause_restart_grace_period_survives_reactivation(self):
        """J3 fix: Grace period based on first_spend_date should NOT re-trigger
        when a flight pauses and resumes. This tests blocking_chart_weeks pattern
        [active, inactive, active] to verify grace period doesn't reset."""
        from unittest.mock import MagicMock

        line_id = "test-pause-resume"
        today = date.today()
        flight_start = today - timedelta(days=10)
        flight_end = today + timedelta(days=10)

        line = {
            "line_id": line_id,
            "line_code": "TEST_PAUSE",
            "platform_id": "meta",
            "channel_category": "Digital",
            "budget": 10000.0,
            "flight_start": flight_start.isoformat(),
            "flight_end": flight_end.isoformat(),
        }

        # Pattern: active (days 0-6), inactive (days 7-13), active (days 14-20)
        blocking_weeks = [
            {
                "line_id": line_id,
                "week_start": flight_start.isoformat(),
                "is_active": True,  # Week 1: active
            },
            {
                "line_id": line_id,
                "week_start": (flight_start + timedelta(days=7)).isoformat(),
                "is_active": False,  # Week 2: inactive (pause)
            },
            {
                "line_id": line_id,
                "week_start": (flight_start + timedelta(days=14)).isoformat(),
                "is_active": True,  # Week 3: active (resume)
            },
        ]

        captured_rows = []

        def fake_write(project_code, as_of, rows):
            captured_rows.extend(rows)

        with patch("backend.services.pacing.bq") as mock_bq, \
             patch("backend.services.pacing._write_budget_tracking", side_effect=fake_write), \
             patch("backend.services.pacing._write_alerts"):

            mock_bq.table.return_value = "dummy_table"
            mock_bq.string_param.return_value = MagicMock()
            mock_bq.scalar_param.return_value = MagicMock()
            mock_bq.date_param.return_value = MagicMock()

            call_count = [0]

            def mock_run_query(sql, params=None):
                call_count[0] += 1
                if call_count[0] == 1:
                    return [line]
                elif call_count[0] == 2:
                    return blocking_weeks
                else:
                    return []

            mock_bq.run_query.side_effect = mock_run_query

            from backend.services.pacing import run_pacing_for_project
            run_pacing_for_project("TEST_PAUSE")

        assert len(captured_rows) > 0, "No tracking rows written"
        row = captured_rows[0]

        # Even with pause/resume pattern, grace period calculation uses
        # first_spend_date from fact_digital_daily, which is spend-aware and
        # survives pause/restart. Line should be 'active' because enough time
        # has passed since flight_start.
        assert row["line_status"] == "active", \
            "Grace period should not re-trigger on resume; line should be active"
