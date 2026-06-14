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
from backend.routers import pacing as pacing_router
from backend.routers import admin as admin_mod
import asyncio


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

    def test_no_budget_bundle_child_not_surfaced_as_dropped(self):
        """Cry-wolf fix: a NULL-budget BUNDLE CHILD is written under its parent
        (budget=NULL by design, pool on the parent), so it is NOT a lost line
        and must NOT surface as 'dropped'. This is the 24058 case: 23 pooled
        bundle children were wrongly reported as dropped=23. A child is flagged
        by `merged_with_previous=True` (merge continuation) and/or a non-None
        `bundle_group`."""
        mp_lines = [
            # Bundle PARENT — carries the $9,000 pool (survives the budget gate).
            _mp("Meta", "meta", 9000.0, audience_name="Pool parent",
                bundle_group=0),
            # Two pooled children — NULL budget, merge continuation rows.
            _mp("Meta", "meta", None, audience_name="Child A",
                bundle_group=0, merged_with_previous=True),
            _mp("Meta", "meta", None, audience_name="Child B",
                bundle_group=0, merged_with_previous=True),
        ]
        dropped: list[dict] = []
        out = _synthesise_lines_from_mp(mp_lines, _META, dropped=dropped)
        # Only the budgeted parent synthesises a line here; children are written
        # downstream by the bundle-aware line-record builder, not via synthesis.
        assert len(out) == 1
        assert out[0]["audience_name"] == "Pool parent"
        # The crux: no pooled child surfaces as a dropped/lost line.
        assert dropped == [], "bundle children must NOT be reported as dropped"

    def test_genuine_no_budget_non_bundle_row_still_surfaces(self):
        """Counter-case: a NULL-budget row that is NOT a bundle child (no
        merge flag, no bundle_group) is a genuinely-lost line and MUST still
        surface as dropped — the cry-wolf fix only spares pooled children."""
        mp_lines = [
            _mp("Meta", "meta", None, audience_name="Truly lost"),  # standalone
        ]
        dropped: list[dict] = []
        out = _synthesise_lines_from_mp(mp_lines, _META, dropped=dropped)
        assert out == []
        assert len(dropped) == 1
        assert dropped[0]["reason"] == "no_budget"
        assert dropped[0]["label"]  # carries a human label


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
            self._line(3500.0),   # DOOH → stackadapt (recognised = self-serve)
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
    def test_lines_query_filters_is_direct_not_is_traditional(self):
        """Pacing inclusion is governed by TRACKABILITY (is_direct), NOT media
        type (is_traditional). The line query must carry the is_direct exclusion
        so direct buys never produce budget_tracking rows or alarms — AND must
        NOT carry an is_traditional filter (removed: a recognised-platform line
        whose label reads 'traditional', e.g. 26023's StackAdapt-backed DOOH,
        must still pace). We assert on the SQL the engine sends to BigQuery."""
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
        # The is_traditional media-type FILTER CLAUSE has been REMOVED —
        # trackability, not media type, governs pacing inclusion now. (We assert
        # on the clause, not the bare word, since the explanatory SQL comment
        # legitimately still names is_traditional.)
        assert "l.is_traditional = FALSE" not in sql, (
            "the is_traditional = FALSE pacing filter must be gone"
        )
        assert "AND l.is_traditional" not in sql, (
            "no is_traditional filter clause may gate pacing inclusion"
        )
        # The trackability exclusion remains, and NULL is now excluded too (race
        # fix b): only an explicit is_direct = FALSE paces, so a transiently-NULL
        # (mid-sync) line is never paced.
        assert "is_direct" in sql
        # Effective is_direct = COALESCE(is_direct_override, is_direct): the
        # manual override wins over auto, and NULL/NULL stays excluded.
        assert "COALESCE(l.is_direct_override, l.is_direct) = FALSE" in sql
        assert "COALESCE(l.is_direct, FALSE) = FALSE" not in sql

    @patch("backend.services.pacing.date")
    @patch("backend.services.pacing.bq")
    @patch("backend.services.pacing._write_budget_tracking")
    @patch("backend.services.pacing._write_alerts")
    def test_recognised_traditional_line_paces_direct_line_does_not(
        self, mock_alerts, mock_tracking, mock_bq, mock_date
    ):
        """Behavioural proof of the trackability swap: a recognised-platform
        line whose label is keyword-'traditional' (26023 DOOH on StackAdapt,
        is_traditional=TRUE / is_direct=FALSE) IS paced, while a direct buy
        (is_direct=TRUE) is NOT. We model BQ's filtering: the engine's query
        keeps is_direct=FALSE rows regardless of is_traditional, and drops
        is_direct=TRUE rows."""
        mock_date.today.return_value = date(2026, 6, 13)
        mock_date.fromisoformat.side_effect = lambda s: date.fromisoformat(s)
        mock_bq.table.side_effect = lambda name: f"`proj.ds.{name}`"
        mock_bq.string_param.side_effect = lambda n, v: (n, v)
        mock_bq.date_param.side_effect = lambda n, v: (n, v)

        # A recognised DOOH line (StackAdapt-backed): traditional label but a
        # real self-serve feed, so it must pace.
        dooh_line = {
            "line_id": "L-dooh",
            "line_code": None,
            "platform_id": "stackadapt",
            "channel_category": "Digital",
            "site_network": "Digital Out Of Home",
            "budget": 3500.0,
            "flight_start": date(2026, 6, 11),
            "flight_end": date(2026, 7, 19),
            "bundle_id": None,
            "bundle_role": None,
        }
        # A direct buy (LED truck) — no self-serve feed, must be excluded.
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
                # Model BQ filtering: is_direct=FALSE rows survive (DOOH paces
                # even though it's traditional); is_direct=TRUE rows are dropped.
                assert "AND l.is_traditional" not in sql, (
                    "the is_traditional pacing filter must be gone"
                )
                if "COALESCE(l.is_direct_override, l.is_direct) = FALSE" in sql:
                    return [dooh_line]
                return [dooh_line, direct_line]
            if "blocking_chart_weeks" in sql:
                return []
            if "SUM(spend)" in sql:
                return [{"total_spend": 500.0}]
            return []

        mock_bq.run_query.side_effect = query_router

        result = run_pacing_for_project("26023", date(2026, 6, 13))

        # Exactly one line paced — the recognised traditional DOOH line. The
        # direct LED-truck buy was filtered out by the is_direct guard.
        assert result["lines_processed"] == 1
        tracking_rows = mock_tracking.call_args[0][2]
        paced_ids = {r["line_id"] for r in tracking_rows}
        assert paced_ids == {"L-dooh"}, "recognised traditional line must pace"
        assert "L-direct" not in paced_ids, "direct buy must NOT pace"

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
                if "COALESCE(l.is_direct_override, l.is_direct) = FALSE" in sql:
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


# ── Item D: direct-buys budget-context surfacing in the pacing router ─


class TestDirectLinesRouter:
    """`_query_direct_lines` reads is_direct lines off media_plan_lines and maps
    them to DirectLine context records (budget context only — these are
    excluded from pacing). We assert the dedup-guard SQL shape + the mapping."""

    def _patched_bq(self, captured: dict, rows: list[dict]):
        class _BQ:
            @staticmethod
            def table(name):
                return f"`proj.ds.{name}`"

            @staticmethod
            def string_param(n, v):
                return (n, v)

            @staticmethod
            def run_query(sql, params=None):
                captured["sql"] = sql
                captured["params"] = params
                return rows

        return _BQ

    def test_query_direct_lines_sql_filters_is_direct_with_dedup_guard(self):
        captured: dict = {}
        with patch.object(
            pacing_router, "bq", self._patched_bq(captured, [])
        ):
            out = pacing_router._query_direct_lines("26023")
        assert out == []
        sql = captured["sql"]
        # Selects only is_direct lines (COALESCE guards the NULL migration window).
        assert "COALESCE(l.is_direct_override, l.is_direct, FALSE) = TRUE" in sql
        # Standard ROW_NUMBER dedup + plan_id-in-current-plans guard (mpl_dedup).
        assert "ROW_NUMBER" in sql
        assert "_rn = 1" in sql
        assert "is_current   = TRUE" in sql
        assert "pmp.is_active   = TRUE" in sql

    def test_query_direct_lines_maps_rows_to_models(self):
        rows = [
            {
                "site_network": "Connected TV",
                "platform_id": "connected_tv",
                "budget": 6000.0,
                "audience_name": "TSN & Crave",
            },
            {
                "site_network": "LED Truck",
                "platform_id": "led_truck",
                "budget": 17000.0,
                "audience_name": None,
            },
        ]
        captured: dict = {}
        with patch.object(
            pacing_router, "bq", self._patched_bq(captured, rows)
        ):
            out = pacing_router._query_direct_lines("26023")
        assert len(out) == 2
        # audience_name preferred as the label; falls back to site_network.
        assert out[0].label == "TSN & Crave"
        assert out[0].budget == pytest.approx(6000.0)
        assert out[0].platform == "Connected TV"
        assert out[0].audience == "TSN & Crave"
        # Row with no audience → label falls back to the platform/site label.
        assert out[1].label == "LED Truck"
        assert out[1].budget == pytest.approx(17000.0)
        assert out[1].audience is None


# ── is_direct manual override endpoint ────────────────────────────


class TestIsDirectOverrideEndpoint:
    def _patched_bq(self, captured):
        class _BQ:
            @staticmethod
            def table(name):
                return f"`proj.ds.{name}`"

            @staticmethod
            def string_param(n, v):
                return (n, v)

            @staticmethod
            def scalar_param(n, t, v):
                return (n, v)

            @staticmethod
            def run_query(sql, params=None):
                captured.append(sql)
                if "SELECT project_code" in sql:
                    return [{"project_code": "26023",
                             "platform_id": "building_projection",
                             "budget": 17647.06}]
                return []
        return _BQ

    def test_override_persists_to_row_and_table_and_repaces(self):
        captured: list = []
        with patch.object(admin_mod, "bq", self._patched_bq(captured)), \
                patch("backend.services.pacing.run_pacing_for_project") as mock_pace:
            result = asyncio.run(admin_mod.update_line_is_direct(
                "plan-26023-line-006",
                admin_mod.IsDirectOverrideUpdate(is_direct_override=False),
            ))
        assert result["status"] == "updated"
        assert result["is_direct_override"] is False
        assert result["repaced"] is True
        # live row update + durable override upsert
        assert any("UPDATE" in s and "is_direct_override" in s for s in captured)
        assert any("MERGE" in s and "media_plan_line_overrides" in s for s in captured)
        # re-paced the owning project so the line moves immediately
        mock_pace.assert_called_once()
        assert mock_pace.call_args[0][0] == "26023"

    def test_override_404_when_line_missing(self):
        class _BQ:
            @staticmethod
            def table(name):
                return f"`proj.ds.{name}`"

            @staticmethod
            def string_param(n, v):
                return (n, v)

            @staticmethod
            def scalar_param(n, t, v):
                return (n, v)

            @staticmethod
            def run_query(sql, params=None):
                return []  # line not found

        from fastapi import HTTPException
        with patch.object(admin_mod, "bq", _BQ):
            with pytest.raises(HTTPException):
                asyncio.run(admin_mod.update_line_is_direct(
                    "missing",
                    admin_mod.IsDirectOverrideUpdate(is_direct_override=True),
                ))
