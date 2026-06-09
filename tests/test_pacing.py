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

        # ADAC-51 commit 3: _count_active_days now takes as_of_date explicitly.
        total, elapsed = _count_active_days(weeks, tomorrow, flight_end, date.today())
        # elapsed should be 0 since the flight hasn't started
        assert elapsed == 0

    def test_flight_started_today(self):
        """If today == flight_start, elapsed_active_days should be 1."""
        today = date.today()
        flight_end = today + timedelta(days=21)
        weeks = [{"week_start": today.isoformat(), "is_active": True}]

        total, elapsed = _count_active_days(weeks, today, flight_end, today)
        assert elapsed == 1

    def test_flight_completed(self):
        """Elapsed should equal total when flight is fully in the past."""
        start = date.today() - timedelta(days=30)
        end = date.today() - timedelta(days=1)
        weeks = [{"week_start": start.isoformat(), "is_active": True}]

        total, elapsed = _count_active_days(weeks, start, end, date.today())
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
            # ADAC-51 commit 3: as_of_date now required.
            run_pacing_for_project("TEST01", date.today())

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
            run_pacing_for_project("TEST_PAUSE", date.today())

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
            run_pacing_for_project("BND_TEST", date.today())

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


class TestRunAllActivePostFlightReconciliation:
    """run_all_active must keep re-pacing recently-completed projects.

    Regression for the 26018 CAPE spend mismatch: _auto_complete_projects flips a
    project to 'completed' the morning after end_date, dropping it from the sweep
    before the final 1-3 days of Funnel spend land. budget_tracking then freezes at
    a partial-spend snapshot while Meta shows the full amount. The selection query
    must include projects whose end_date is within POST_FLIGHT_RECONCILE_DAYS days.
    """

    def _run(self, project_rows):
        from unittest.mock import patch
        from backend.services.pacing import run_all_active

        captured = {}

        def mock_run_query(sql, params=None):
            captured["sql"] = sql
            captured["params"] = params
            return project_rows

        with patch("backend.services.pacing.bq") as mock_bq, \
             patch("backend.services.pacing.run_pacing_for_project") as mock_run_proj:
            mock_bq.table.side_effect = lambda name: name
            mock_bq.date_param.side_effect = lambda name, value: ("date_param", name, value)
            mock_bq.run_query.side_effect = mock_run_query
            mock_run_proj.return_value = {"lines_processed": 1, "alerts": 0}
            result = run_all_active(date(2026, 6, 8))
        return captured, mock_run_proj, result

    def test_selection_includes_recently_completed_projects(self):
        captured, _, _ = self._run([{"project_code": "26018"}])
        sql = captured["sql"]
        # Still picks up live projects …
        assert "p.status IN ('active', 'in_flight')" in sql
        # … and now recently-completed ones within the reconciliation window.
        assert "p.status = 'completed'" in sql
        assert "DATE_SUB(@as_of_date, INTERVAL 7 DAY)" in sql
        # as_of_date is parameterised, not interpolated.
        assert captured["params"] == [("date_param", "as_of_date", date(2026, 6, 8))]

    def test_completed_project_in_window_gets_repaced(self):
        _, mock_run_proj, result = self._run([{"project_code": "26018"}])
        mock_run_proj.assert_called_once()
        assert mock_run_proj.call_args.args[0] == "26018"
        # as_of_date forwarded so spend is read through "today".
        assert mock_run_proj.call_args.args[1] == date(2026, 6, 8)
        assert result["projects_processed"] == 1


class TestBudgetExceededAlert:
    """budget_exceeded must not fire when a line merely lands at its budget.

    Regression: lines #7/#8 on 26018 finished at exactly 100% of budget (actual
    == budget), but the budget-proportional split left a sub-cent float over and
    the message rounded both numbers to whole dollars — "actual $239 exceeds
    budget $239" — firing a spurious critical. A $BUDGET_EXCEEDED_TOLERANCE buffer
    is now required, and the message reports the real overage.
    """

    def _alerts(self, actual, budget, pacing_pct=100.0):
        from backend.services.pacing import _generate_alerts
        return _generate_alerts(
            "26018", "L", "#7",
            pacing_pct, actual, budget,
            remaining_days=0, remaining_budget=budget - actual,
        )

    @staticmethod
    def _types(alerts):
        return {a["alert_type"] for a in alerts}

    def test_exactly_at_budget_is_silent(self):
        """A clean 100% completion fires nothing — the #7/#8 case."""
        alerts = self._alerts(238.87, 238.87)
        assert alerts == []

    def test_subcent_float_over_does_not_fire(self):
        """The proportional-split artifact: actual a floating-point hair over."""
        alerts = self._alerts(238.87 + 1e-9, 238.87)
        assert "budget_exceeded" not in self._types(alerts)

    def test_within_tolerance_does_not_fire(self):
        alerts = self._alerts(239.50, 238.87)  # $0.63 over, under the $1 buffer
        assert "budget_exceeded" not in self._types(alerts)

    def test_real_overage_fires_with_overage_in_message(self):
        import json
        alerts = self._alerts(250.00, 238.87)  # $11.13 over
        be = [a for a in alerts if a["alert_type"] == "budget_exceeded"]
        assert len(be) == 1
        assert be[0]["severity"] == "critical"
        # Message reports the real overage, not "$239 exceeds $239".
        assert "$11.13" in be[0]["message"]
        assert "$250.00" in be[0]["message"]
        assert "exceeds planned budget" not in be[0]["message"]
        assert json.loads(be[0]["metadata"])["overage"] == pytest.approx(11.13)
