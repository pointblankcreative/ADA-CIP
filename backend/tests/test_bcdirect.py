"""bcdirect: surfaced drops + is_direct model + reconciliation.

Covers the three builds added to the media-plan detail→line path:

  1. SURFACED DROPS — dropped/redirected rows are collected as structured
     records ({label, platform_id, budget, reason}) threaded out of
     _synthesise_lines_from_mp, and exposed in the sync response (not just logs).
  2. is_direct MODEL — a budgeted row on an UNRECOGNISED platform (no
     PLATFORM_MAP self-serve feed) is CAPTURED (not dropped) and tagged
     is_direct=TRUE; the line-record builder writes is_direct; pacing excludes
     is_direct lines exactly like is_traditional. is_direct is orthogonal to
     is_traditional (a line can be both).
  3. RECONCILIATION — captured-self-serve + captured-direct + dropped + delta,
     a SOFT signal surfaced in logs + response, never raising.

Numbers used in the reconciliation test are the real 26023 (Sierra Club BC /
FIFA Old Growth) figures: $29,090 self-serve, ~$70,272 direct, ~$99,362 total.
"""

from datetime import date

import pytest
from unittest.mock import patch

from backend.services.media_plan_sync import (
    PLATFORM_MAP,
    _build_line_records_for_bc_line,
    _build_reconciliation,
    _synthesise_lines_from_mp,
)
from backend.services import pacing as pacing_mod
from backend.services.pacing import run_pacing_for_project


_META = {
    "start_date": date(2026, 6, 11),
    "end_date": date(2026, 7, 19),
    "net_budget": 99362.06,
    "client_name": "Sierra Club of BC",
}


def _mp(platform, platform_id, budget, **over):
    """Minimal media-plan tab line dict as _parse_media_plan_tab emits."""
    base = {
        "platform": platform,
        "platform_id": platform_id,
        "goal": "Awareness",
        "budget": budget,
        "flight_start": date(2026, 6, 11),
        "flight_end": date(2026, 7, 19),
        "audience_name": f"{platform} aud",
        "line_code": "",
        "bundle_group": None,
    }
    base.update(over)
    return base


# ── Item 2: budgeted unrecognised platform captured as is_direct ──────


class TestIsDirectCapture:
    def test_budgeted_unrecognised_line_is_captured_not_dropped(self):
        """A real-budget direct buy (CTV) survives synthesis tagged is_direct."""
        mp_lines = [_mp("Connected TV", "connected_tv", 6000.0)]
        dropped: list[dict] = []
        out = _synthesise_lines_from_mp(mp_lines, _META, dropped=dropped)
        assert len(out) == 1, "budgeted unrecognised line must be RETAINED"
        assert out[0]["platform_id"] == "connected_tv"
        assert out[0]["budget"] == pytest.approx(6000.0)
        assert out[0]["is_direct"] is True
        assert dropped == [], "a budgeted direct buy is captured, not dropped"

    def test_recognised_platform_line_stays_is_direct_false(self):
        """Recognised platforms (Meta) keep is_direct=FALSE — existing projects
        whose lines are all recognised are unaffected."""
        mp_lines = [_mp("Meta", "meta", 19500.0)]
        out = _synthesise_lines_from_mp(mp_lines, _META, dropped=[])
        assert len(out) == 1
        assert out[0]["platform_id"] == "meta"
        assert out[0]["is_direct"] is False

    def test_is_direct_derives_from_platform_map_membership(self):
        """is_direct is exactly NOT-in-PLATFORM_MAP.values() — the trackability
        axis. Sanity check against the live map."""
        # stackadapt / meta / perion are feeds; led_truck is not.
        assert "stackadapt" in PLATFORM_MAP.values()
        assert "led_truck" not in PLATFORM_MAP.values()
        recognised = _synthesise_lines_from_mp(
            [_mp("Open Web Video", "stackadapt", 2000.0)], _META, dropped=[]
        )
        direct = _synthesise_lines_from_mp(
            [_mp("LED Truck", "led_truck", 17000.0)], _META, dropped=[]
        )
        assert recognised[0]["is_direct"] is False
        assert direct[0]["is_direct"] is True

    def test_build_line_records_writes_is_direct_true_for_direct_line(self):
        """The line-record builder must emit is_direct on the BQ row dict so it
        is written to media_plan_lines."""
        bc_line = {
            "platform": "Connected TV",
            "platform_id": "connected_tv",
            "budget": 6000.0,
            "objective_format": "Awareness",
            "flight_start": date(2026, 6, 11),
            "flight_end": date(2026, 7, 19),
            "audience_name": "TSN & Crave",
            "is_direct": True,  # tag carried from _synthesise_lines_from_mp
        }
        recs = _build_line_records_for_bc_line(
            bc_line=bc_line,
            mp_detail=None,
            all_mp_lines=[],
            plan_id="plan-x",
            line_id="plan-x-line-000",
            project_code="26023",
            meta=_META,
        )
        assert len(recs) == 1
        assert "is_direct" in recs[0], "is_direct must be on the written row dict"
        assert recs[0]["is_direct"] is True
        # Orthogonality: a direct CTV line is NOT traditional (no traditional
        # keyword in 'Connected TV').
        assert recs[0]["is_traditional"] is False

    def test_build_line_records_is_direct_false_for_recognised(self):
        """Recognised-platform bc_line (no is_direct tag) → is_direct=FALSE."""
        bc_line = {
            "platform": "Meta",
            "platform_id": "meta",
            "budget": 19500.0,
            "objective_format": "Conversion",
            "flight_start": date(2026, 6, 11),
            "flight_end": date(2026, 7, 19),
            "audience_name": "List Lookalikes",
            # no is_direct key at all — must default False
        }
        recs = _build_line_records_for_bc_line(
            bc_line=bc_line,
            mp_detail=None,
            all_mp_lines=[],
            plan_id="plan-x",
            line_id="plan-x-line-000",
            project_code="26023",
            meta=_META,
        )
        assert recs[0]["is_direct"] is False

    def test_is_direct_orthogonal_to_is_traditional_can_be_both(self):
        """A traditional-keyword direct buy (LED Truck contains no keyword, but
        'Out of Home' does) can be is_direct AND is_traditional simultaneously —
        the two axes are independent."""
        # 'Out Of Home' hits the traditional keyword AND is unrecognised at the
        # platform_id level (we feed a non-PLATFORM_MAP pid to model a direct OOH
        # line rather than the perion-mapped DOOH alias).
        bc_line = {
            "platform": "Out Of Home Transit Wrap",
            "platform_id": "out_of_home_transit_wrap",
            "budget": 17647.06,
            "objective_format": "Awareness",
            "flight_start": date(2026, 6, 11),
            "flight_end": date(2026, 7, 19),
            "audience_name": "Transit riders",
            "is_direct": True,
        }
        recs = _build_line_records_for_bc_line(
            bc_line=bc_line,
            mp_detail=None,
            all_mp_lines=[],
            plan_id="plan-x",
            line_id="plan-x-line-000",
            project_code="26023",
            meta=_META,
        )
        assert recs[0]["is_direct"] is True
        assert recs[0]["is_traditional"] is True


# ── Item 1: a no-budget row is dropped AND surfaced with a reason ─────


class TestSurfacedDrops:
    def test_no_budget_row_dropped_and_surfaced_with_reason(self):
        """A no-budget row is dropped, but appears in the surfaced dropped list
        with reason='no_budget' (not vanished into a log)."""
        mp_lines = [
            _mp("Meta", "meta", None, audience_name="No budget line"),
            _mp("Meta", "meta", 19500.0, audience_name="Real line"),
        ]
        dropped: list[dict] = []
        out = _synthesise_lines_from_mp(mp_lines, _META, dropped=dropped)
        # Only the budgeted line survives.
        assert len(out) == 1
        assert out[0]["audience_name"] == "Real line"
        # The no-budget line is surfaced.
        assert len(dropped) == 1
        rec = dropped[0]
        assert rec["reason"] == "no_budget"
        assert rec["platform_id"] == "meta"
        assert rec["budget"] is None
        # Structured record carries a human label.
        assert "label" in rec

    def test_zero_budget_row_dropped_with_no_budget_reason(self):
        mp_lines = [_mp("Meta", "meta", 0, audience_name="Zero")]
        dropped: list[dict] = []
        out = _synthesise_lines_from_mp(mp_lines, _META, dropped=dropped)
        assert out == []
        assert len(dropped) == 1
        assert dropped[0]["reason"] == "no_budget"

    def test_drop_reason_is_in_allowed_enum(self):
        """Every surfaced reason must be one of the two documented values."""
        mp_lines = [
            _mp("Meta", "meta", None),
            _mp("Connected TV", "connected_tv", 0),
        ]
        dropped: list[dict] = []
        _synthesise_lines_from_mp(mp_lines, _META, dropped=dropped)
        for rec in dropped:
            assert rec["reason"] in {"no_budget", "unrecognised_platform"}

    def test_dropped_param_optional_backward_compatible(self):
        """Existing 2-arg callers (no dropped list) still work and drop silently
        as before — additive change."""
        out = _synthesise_lines_from_mp([_mp("Meta", "meta", None)], _META)
        assert out == []  # no crash without the dropped kwarg

    def test_budgeted_direct_buy_not_in_dropped(self):
        """Regression guard: budgeted unrecognised lines must NOT appear in the
        dropped list (they're captured now)."""
        dropped: list[dict] = []
        _synthesise_lines_from_mp(
            [_mp("LED Truck", "led_truck", 17000.0)], _META, dropped=dropped
        )
        assert dropped == []


# ── Item 3: reconciliation reports self-serve + direct + delta ────────


class TestReconciliation:
    def _line(self, budget, is_direct=False, is_traditional=False):
        return {
            "budget": budget,
            "is_direct": is_direct,
            "is_traditional": is_traditional,
        }

    def test_26023_numbers(self):
        """The real 26023 split: $29,090 self-serve, ~$70,272 direct, net
        ~$99,362 → delta ~0. (Self-serve = Meta $19,500 + $4,090 + DOOH $3,500 +
        Open Web $2,000; direct = CTV $6,000+$6,000 + Building Projection
        $17,647.06 + LED Truck $17,000 + Skytrain $23,625.)"""
        line_records = [
            # self-serve (recognised, will pace)
            self._line(19500.0),
            self._line(4090.0),
            self._line(3500.0),   # DOOH → perion (recognised = self-serve)
            self._line(2000.0),   # Open Web → stackadapt
            # direct buys (is_direct, excluded from pacing)
            self._line(6000.0, is_direct=True),
            self._line(6000.0, is_direct=True),
            self._line(17647.06, is_direct=True),
            self._line(17000.0, is_direct=True),
            self._line(23625.0, is_direct=True),
        ]
        recon = _build_reconciliation(line_records, [], net_budget=99362.06)

        assert recon["captured_self_serve"] == pytest.approx(29090.0)
        assert recon["captured_direct"] == pytest.approx(70272.06)
        assert recon["captured_total"] == pytest.approx(99362.06)
        assert recon["net_budget"] == pytest.approx(99362.06)
        # Delta ~ 0 (the plan reconciles once direct buys are captured).
        assert recon["delta"] == pytest.approx(0.0, abs=0.01)
        assert recon["dropped_budget"] == pytest.approx(0.0)

    def test_dropped_budget_should_be_zero_when_direct_captured(self):
        """Because budgeted unrecognised lines are captured (not dropped),
        dropped_budget is ~0 — only no_budget rows drop, and those carry no
        budget to sum."""
        line_records = [self._line(2000.0, is_direct=True)]
        dropped = [
            {"label": "x", "platform_id": "meta", "budget": None,
             "reason": "no_budget"},
        ]
        recon = _build_reconciliation(line_records, dropped, net_budget=2000.0)
        assert recon["dropped_budget"] == pytest.approx(0.0)
        assert recon["dropped_count"] == 1
        assert recon["delta"] == pytest.approx(0.0)

    def test_non_zero_delta_is_reported_not_raised(self):
        """Delta is a SOFT signal: a legitimate net>captured gap is reported,
        never raised."""
        line_records = [self._line(50000.0)]
        recon = _build_reconciliation(line_records, [], net_budget=99365.0)
        assert recon["delta"] == pytest.approx(49365.0)

    def test_bundle_children_null_budget_not_double_counted(self):
        """Bundle children carry budget=None; reconciliation sums only the
        parent's pool."""
        line_records = [
            self._line(3000.0),   # parent
            self._line(None),     # child
            self._line(None),     # child
        ]
        recon = _build_reconciliation(line_records, [], net_budget=3000.0)
        assert recon["captured_self_serve"] == pytest.approx(3000.0)

    def test_traditional_lines_excluded_from_self_serve_bucket(self):
        """is_traditional lines don't count as paced self-serve; they're broken
        out separately so the buckets still sum to the written total."""
        line_records = [
            self._line(10000.0),                       # self-serve
            self._line(5000.0, is_traditional=True),   # traditional (not paced)
            self._line(7000.0, is_direct=True),        # direct (not paced)
        ]
        recon = _build_reconciliation(line_records, [], net_budget=22000.0)
        assert recon["captured_self_serve"] == pytest.approx(10000.0)
        assert recon["captured_traditional"] == pytest.approx(5000.0)
        assert recon["captured_direct"] == pytest.approx(7000.0)
        # captured_total intentionally = self_serve + direct (the "live spend"
        # surfaces); delta uses that.
        assert recon["captured_total"] == pytest.approx(17000.0)
        assert recon["delta"] == pytest.approx(5000.0)

    def test_none_net_budget_yields_none_delta(self):
        recon = _build_reconciliation([self._line(1000.0)], [], net_budget=None)
        assert recon["net_budget"] is None
        assert recon["delta"] is None
        assert recon["captured_self_serve"] == pytest.approx(1000.0)


# ── Item 2: pacing excludes is_direct lines ──────────────────────────


class TestPacingExcludesIsDirect:
    def test_lines_query_filters_is_direct(self):
        """The pacing line query must carry an is_direct exclusion alongside the
        is_traditional one, so direct buys never produce budget_tracking rows or
        alarms. We assert on the SQL the engine sends to BigQuery."""
        captured_sql = {}

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
            def run_query(sql, params=None):
                if "media_plan_lines" in sql and "ROW_NUMBER" in sql:
                    captured_sql["lines"] = sql
                    # Return no lines so the run short-circuits cleanly.
                    return []
                return []

        with patch.object(pacing_mod, "bq", _BQ):
            run_pacing_for_project("26023", date(2026, 6, 13))

        sql = captured_sql.get("lines", "")
        assert "is_traditional = FALSE" in sql, "baseline filter must remain"
        # The new exclusion (COALESCE guards the NULL window pre-resync).
        assert "is_direct" in sql
        assert "COALESCE(l.is_direct, FALSE) = FALSE" in sql

    @patch("backend.services.pacing.date")
    @patch("backend.services.pacing.bq")
    @patch("backend.services.pacing._write_budget_tracking")
    @patch("backend.services.pacing._write_alerts")
    def test_is_direct_line_produces_no_tracking_row(
        self, mock_alerts, mock_tracking, mock_bq, mock_date
    ):
        """Behavioural proof: a direct line that BQ filtered out (because of the
        is_direct = FALSE clause) yields zero tracking rows / alerts. We model
        BQ's filtering by returning the direct line ONLY when the query lacks
        the is_direct guard — with the guard present, it returns nothing, so the
        engine processes no lines."""
        mock_date.today.return_value = date(2026, 6, 13)
        mock_date.fromisoformat.side_effect = lambda s: date.fromisoformat(s)
        mock_bq.table.side_effect = lambda name: f"`proj.ds.{name}`"
        mock_bq.string_param.side_effect = lambda n, v: (n, v)
        mock_bq.date_param.side_effect = lambda n, v: (n, v)

        direct_line = {
            "line_id": "L-direct",
            "line_code": None,
            "platform_id": "led_truck",
            "channel_category": "Digital",
            "site_network": "LED Truck",
            "budget": 17000.0,
            "flight_start": date(2026, 6, 11),
            "flight_end": date(2026, 7, 19),
            "bundle_id": None,
            "bundle_role": None,
        }

        def query_router(sql, params=None):
            if "media_plan_lines" in sql and "ROW_NUMBER" in sql:
                # Real BQ would apply the is_direct filter and exclude the
                # direct line. Mirror that: guard present → no rows.
                if "COALESCE(l.is_direct, FALSE) = FALSE" in sql:
                    return []
                return [direct_line]
            if "blocking_chart_weeks" in sql:
                return []
            if "SUM(spend)" in sql:
                return [{"total_spend": 0.0}]
            return []

        mock_bq.run_query.side_effect = query_router

        result = run_pacing_for_project("26023", date(2026, 6, 13))

        # No lines processed → no tracking write, no alerts.
        assert result["lines_processed"] == 0
        assert result["alerts"] == 0
        assert not mock_tracking.called
        assert not mock_alerts.called
