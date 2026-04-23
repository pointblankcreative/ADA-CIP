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


# ── PR 4: bundled-optimization pacing awareness ─────────────────────


class TestBundleAwarePacing:
    """Pacing must:
      - skip bundle children (budget=NULL; their spend is attributed to parent)
      - aggregate spend across all member line_codes for bundle parents
      - not double-count when a single ad set has multiple line codes

    Accuracy matters: the whole reason for this feature is to stop pacing
    under/overspend from going undetected when CBO shares a budget pool.
    """

    def _run_pacing_with_mocked_data(
        self,
        lines: list[dict],
        blocking_weeks: list[dict],
        spend_responses: list[dict],
    ) -> list[dict]:
        """Drive run_pacing_for_project with synthetic mp_lines + weeks + spend rows.

        `spend_responses` is a FIFO queue consumed by each spend-query call
        (every one of the group-spend, per-code, and per-bundle queries).
        Each entry should be a list of row dicts (e.g. ``[{"total_spend": 500.0}]``).
        """
        from unittest.mock import MagicMock

        captured_rows: list[dict] = []

        def fake_write(project_code, as_of, rows):
            captured_rows.extend(rows)

        spend_queue = list(spend_responses)

        with patch("backend.services.pacing.bq") as mock_bq, \
             patch("backend.services.pacing._write_budget_tracking", side_effect=fake_write), \
             patch("backend.services.pacing._write_alerts"):

            mock_bq.table.side_effect = lambda n: f"`dummy.{n}`"
            mock_bq.string_param.return_value = MagicMock()
            mock_bq.scalar_param.return_value = MagicMock()
            mock_bq.date_param.return_value = MagicMock()
            mock_bq.array_param.return_value = MagicMock()

            call_count = [0]

            def mock_run_query(sql, params=None):
                call_count[0] += 1
                if call_count[0] == 1:
                    return lines
                elif call_count[0] == 2:
                    return blocking_weeks
                # All subsequent calls are spend queries
                return spend_queue.pop(0) if spend_queue else []

            mock_bq.run_query.side_effect = mock_run_query

            from backend.services.pacing import run_pacing_for_project
            run_pacing_for_project("BND_TEST")

        return captured_rows

    def test_bundle_child_emits_no_tracking_row(self):
        """A line with bundle_role='suggested_child' must be excluded from pacing."""
        today = date.today()
        flight_start = today - timedelta(days=5)
        flight_end = today + timedelta(days=15)

        lines = [
            {
                "line_id": "parent-01",
                "line_code": "#09",
                "platform_id": "meta",
                "channel_category": "Digital",
                "budget": 2238.19,
                "flight_start": flight_start.isoformat(),
                "flight_end": flight_end.isoformat(),
                "bundle_id": "25034-meta-09",
                "bundle_role": "suggested_parent",
            },
            {
                "line_id": "parent-01-bundled-01",
                "line_code": "#10",
                "platform_id": "meta",
                "channel_category": "Digital",
                "budget": None,  # child has NULL budget
                "flight_start": flight_start.isoformat(),
                "flight_end": flight_end.isoformat(),
                "bundle_id": "25034-meta-09",
                "bundle_role": "suggested_child",
            },
        ]
        blocking_weeks = [
            {"line_id": "parent-01", "week_start": flight_start.isoformat(), "is_active": True},
        ]
        # Enough spend_responses to cover any query pattern pacing makes
        spend_responses = [[{"total_spend": 500.0}]] * 20

        rows = self._run_pacing_with_mocked_data(lines, blocking_weeks, spend_responses)
        line_ids = [r["line_id"] for r in rows]
        assert "parent-01" in line_ids, "Bundle parent must still pace"
        assert "parent-01-bundled-01" not in line_ids, (
            "Bundle child must NOT produce a tracking row — child spend is "
            "attributed to the parent"
        )

    def test_bundle_parent_aggregates_spend_across_members(self):
        """Parent's actual_spend must equal the total bundle spend, regardless of
        whether the ad set names contain only #09, only #10, or both.

        This is the core accuracy guarantee: a bundle paces as ONE pool.
        """
        today = date.today()
        flight_start = today - timedelta(days=5)
        flight_end = today + timedelta(days=15)

        lines = [
            {
                "line_id": "parent-01",
                "line_code": "#09",
                "platform_id": "meta",
                "channel_category": "Digital",
                "budget": 2238.19,
                "flight_start": flight_start.isoformat(),
                "flight_end": flight_end.isoformat(),
                "bundle_id": "25034-meta-09",
                "bundle_role": "suggested_parent",
            },
            {
                "line_id": "parent-01-bundled-01",
                "line_code": "#10",
                "platform_id": "meta",
                "channel_category": "Digital",
                "budget": None,
                "flight_start": flight_start.isoformat(),
                "flight_end": flight_end.isoformat(),
                "bundle_id": "25034-meta-09",
                "bundle_role": "suggested_child",
            },
        ]
        blocking_weeks = [
            {"line_id": "parent-01", "week_start": flight_start.isoformat(), "is_active": True},
        ]
        # Spend query order pacing is expected to run:
        #   1. Per-flight-group total-spend query → $1200 (whole platform for flight)
        #   2. Per-line-code query for "#09"       → $400
        #   3. Per-line-code query for "#10"       → $800
        #   4. Per-bundle set-containment query    → $1200 (full bundle, no double-count)
        # Parent should take the bundle query value ($1200), NOT sum of per-code ($1200).
        spend_responses = [
            [{"total_spend": 1200.0}],                                    # group total
            [{"total_spend": 400.0, "first_spend_date": flight_start}],   # #09
            [{"total_spend": 800.0, "first_spend_date": flight_start}],   # #10
            [{"total_spend": 1200.0}],                                    # bundle
        ]
        rows = self._run_pacing_with_mocked_data(lines, blocking_weeks, spend_responses)
        parent_row = next((r for r in rows if r["line_id"] == "parent-01"), None)
        assert parent_row is not None
        # The actual_spend_to_date should equal the bundle query (1200), not
        # the sum of per-code queries (which would double-count a multi-code
        # ad set if one existed).
        assert parent_row["actual_spend_to_date"] == pytest.approx(1200.0), (
            f"Bundle parent must aggregate via the bundle query "
            f"(got {parent_row['actual_spend_to_date']}, expected 1200.0)"
        )

    def test_bundle_parent_does_not_double_count_multi_code_adsets(self):
        """Regression guard: in real CBO data, one ad set often carries multiple
        codes (e.g. "#11 viewers BC, #12 list"). Summing per-code spend would
        count that ad set's budget twice. The bundle-level query must win.

        In this scenario the per-code queries each return $500 (because both
        match the same multi-code ad set), but the bundle aggregate is $500
        (the ad set's true spend). The parent's actual_spend must reflect $500.
        """
        today = date.today()
        flight_start = today - timedelta(days=5)
        flight_end = today + timedelta(days=15)

        lines = [
            {
                "line_id": "p-11",
                "line_code": "#11",
                "platform_id": "meta",
                "channel_category": "Digital",
                "budget": 3104.00,
                "flight_start": flight_start.isoformat(),
                "flight_end": flight_end.isoformat(),
                "bundle_id": "25034-meta-11",
                "bundle_role": "suggested_parent",
            },
            {
                "line_id": "p-11-bundled-01",
                "line_code": "#12",
                "platform_id": "meta",
                "channel_category": "Digital",
                "budget": None,
                "flight_start": flight_start.isoformat(),
                "flight_end": flight_end.isoformat(),
                "bundle_id": "25034-meta-11",
                "bundle_role": "suggested_child",
            },
        ]
        blocking_weeks = [
            {"line_id": "p-11", "week_start": flight_start.isoformat(), "is_active": True},
        ]
        # Both per-code queries return $500 because the ad set's line_codes
        # array contains both "#11" and "#12". Naive summation would report
        # $1000 spent; the bundle-aggregate query returns the real $500.
        spend_responses = [
            [{"total_spend": 700.0}],                                      # group spend (irrelevant for parent)
            [{"total_spend": 500.0, "first_spend_date": flight_start}],    # #11
            [{"total_spend": 500.0, "first_spend_date": flight_start}],    # #12
            [{"total_spend": 500.0}],                                      # bundle aggregate — THE correct answer
        ]
        rows = self._run_pacing_with_mocked_data(lines, blocking_weeks, spend_responses)
        parent = next((r for r in rows if r["line_id"] == "p-11"), None)
        assert parent is not None
        assert parent["actual_spend_to_date"] == pytest.approx(500.0), (
            f"Double-count regression — parent shows "
            f"{parent['actual_spend_to_date']} but true spend is 500.0. "
            f"Bundle aggregate query must beat summing per-code results."
        )

    def test_standalone_line_uses_view_unnest_for_line_code_match(self):
        """Non-bundled standalone line with line_code must get correct attribution.

        Prior to PR 4, pacing queried `fact_digital_daily.line_code` (a column
        that's never populated), so line_code-matched spend was always $0 and
        lines fell through to proportional flight-group splits. With the view
        + IN UNNEST, line_code matches now work.
        """
        today = date.today()
        flight_start = today - timedelta(days=5)
        flight_end = today + timedelta(days=15)

        lines = [{
            "line_id": "solo-01",
            "line_code": "#05",
            "platform_id": "meta",
            "channel_category": "Digital",
            "budget": 5000.0,
            "flight_start": flight_start.isoformat(),
            "flight_end": flight_end.isoformat(),
            "bundle_id": None,
            "bundle_role": None,
        }]
        blocking_weeks = [
            {"line_id": "solo-01", "week_start": flight_start.isoformat(), "is_active": True},
        ]
        # Spend responses: group total, then line_code "#05"
        spend_responses = [
            [{"total_spend": 9000.0}],                                    # group (noise)
            [{"total_spend": 3000.0, "first_spend_date": flight_start}],  # #05
        ]
        rows = self._run_pacing_with_mocked_data(lines, blocking_weeks, spend_responses)
        assert len(rows) == 1
        assert rows[0]["line_code"] == "#05"
        # Actual spend comes from the per-code query ($3000), NOT from the
        # group-split fallback.
        assert rows[0]["actual_spend_to_date"] == pytest.approx(3000.0)
