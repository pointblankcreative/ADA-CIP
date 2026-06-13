"""Regression tests for the media-plan flight-date YEAR-inference bug.

Root cause (A3 hardening swarm, 2026-06-13):
    ``_parse_date`` stamps month-day-only cells ("June 1", "Oct 24") with a
    reference year. When the only year signal available was
    ``date.today().year``, a campaign that had already LANDED got its flights
    shifted a year forward — e.g. 24058 / OPSEU "Worth Fighting For", a real
    2025-06 → 2025-12 campaign, was stored as 2026-06 → 2026-12. The pacing
    engine then read every line as "not yet started" (AWAITING DATA / 0.0% /
    NOT STARTED), because today (2026-06-13) is before the (wrong) 2026 flight
    starts and the spend window clamps never overlapped the real 2025 spend.

The fix anchors year-less flight cells to the project's authoritative year
(``dim_projects.start_date``) via ``_resolve_ref_year`` and a
``_parse_blocking_chart(anchor_year=...)`` parameter, WITHOUT overriding sheets
that carry an explicit year (no regression for correctly-dated plans).

These tests are intentionally dependency-light: they import only the pure
parsing/inference helpers, so they run in the sandbox without FastAPI / the
admin router import chain.
"""

from datetime import date

import pytest

from backend.services.media_plan_sync import (
    _cell_has_explicit_year,
    _parse_date,
    _resolve_ref_year,
)


# ── _cell_has_explicit_year ──────────────────────────────────────────


class TestCellHasExplicitYear:
    """Distinguish year-less cells (need anchor) from explicit-year cells."""

    @pytest.mark.parametrize("cell", [
        "June 1",
        "Jun 1",
        "1 Jun",
        "Oct 24",
        "October 24",
        "  Nov 28  ",
    ])
    def test_year_less_cells_return_false(self, cell):
        assert _cell_has_explicit_year(cell) is False

    @pytest.mark.parametrize("cell", [
        "June 1, 2025",
        "Jun 1 2025",
        "2025-06-01",
        "6/1/2025",
        "01-Jun-2025",
        "December 15, 2025",
    ])
    def test_explicit_year_cells_return_true(self, cell):
        assert _cell_has_explicit_year(cell) is True

    def test_empty_and_none(self):
        assert _cell_has_explicit_year("") is False
        assert _cell_has_explicit_year(None) is False
        assert _cell_has_explicit_year("   ") is False

    def test_two_digit_year_not_treated_as_explicit(self):
        """'1-Jun-25' style two-digit years aren't matched by the 4-digit
        guard; they're already handled deterministically by the '%d-%b-%y'
        format in _parse_date, so anchoring is moot."""
        assert _cell_has_explicit_year("1-Jun-25") is False


# ── _resolve_ref_year ────────────────────────────────────────────────


class TestResolveRefYear:
    """The decision table that picks the year for month-day-only flights."""

    def test_explicit_year_in_sheet_wins_over_anchor(self):
        """If the blocking chart carried a real year, never override it —
        even if the project anchor disagrees. Guards against shifting a
        correctly-dated plan."""
        result = _resolve_ref_year(
            bc_start=date(2025, 6, 1),
            bc_dates_are_year_less=False,   # sheet had an explicit year
            project_anchor_year=2099,       # deliberately absurd anchor
        )
        assert result == 2025

    def test_year_less_sheet_uses_project_anchor(self):
        """THE FIX: year-less 'June 1' resolves to the campaign's real year
        (2025) instead of whatever today-defaulted value bc_start carries."""
        result = _resolve_ref_year(
            bc_start=date(2026, 6, 1),      # forward-defaulted to today (2026)
            bc_dates_are_year_less=True,
            project_anchor_year=2025,       # dim_projects says 2025
        )
        assert result == 2025

    def test_year_less_no_anchor_falls_back_to_bc_start(self):
        """No project window yet (brand-new plan): keep legacy behaviour —
        use whatever year bc_start resolved to (today's year)."""
        result = _resolve_ref_year(
            bc_start=date(2026, 6, 1),
            bc_dates_are_year_less=True,
            project_anchor_year=None,
        )
        assert result == 2026

    def test_no_bc_start_no_anchor_returns_none(self):
        """Nothing to go on → None, so _parse_date applies date.today().year
        (unchanged last-resort fallback)."""
        result = _resolve_ref_year(
            bc_start=None,
            bc_dates_are_year_less=True,
            project_anchor_year=None,
        )
        assert result is None

    def test_no_bc_start_uses_anchor(self):
        result = _resolve_ref_year(
            bc_start=None,
            bc_dates_are_year_less=True,
            project_anchor_year=2025,
        )
        assert result == 2025


# ── _parse_date with a campaign anchor (the end-to-end unit) ──────────


class TestParseDateYearInference:
    """Drive _parse_date the way the fixed sync now does: month-day cells
    parsed against the campaign's real year, not today's."""

    def test_24058_june_line_resolves_to_2025_not_2026(self):
        """The live failing line: 'Jun 16' for a 2025 campaign must land in
        2025. Before the fix this came out 2026 (today's year) and read as
        '30d remaining' / NOT STARTED."""
        campaign_year = 2025  # dim_projects.start_date.year for 24058
        assert _parse_date("Jun 16", campaign_year) == date(2025, 6, 16)
        assert _parse_date("Jul 5", campaign_year) == date(2025, 7, 5)

    def test_24058_october_line_resolves_to_2025_not_2026(self):
        """The other live failing line: 'Oct 24 — Nov 28' (the big Q4 2025
        burst, $116K in Nov 2025) must land in 2025."""
        campaign_year = 2025
        assert _parse_date("Oct 24", campaign_year) == date(2025, 10, 24)
        assert _parse_date("Nov 28", campaign_year) == date(2025, 11, 28)

    def test_explicit_year_cell_ignores_ref_year(self):
        """A cell that already has a year is parsed deterministically — the
        ref_year argument is not consulted. Proves the anchor can't corrupt a
        correctly-dated plan."""
        assert _parse_date("June 16, 2024", 2025) == date(2024, 6, 16)
        assert _parse_date("2024-10-24", 2025) == date(2024, 10, 24)

    def test_current_year_plan_still_parses_correctly(self):
        """Regression guard for in-flight 2026 plans (26018, 26023): a 2026
        anchor keeps 2026 flights in 2026."""
        assert _parse_date("May 7", 2026) == date(2026, 5, 7)
        assert _parse_date("Jun 5", 2026) == date(2026, 6, 5)


# ── End-to-end style: simulate the propagation the sync performs ─────


class TestBlockingChartYearPropagationLogic:
    """Mirror the sync's flow at the function-composition level: anchor →
    resolve ref_year → parse per-line flights. This is the exact sequence
    _parse_blocking_chart + sync_media_plan execute, without needing gspread."""

    def _line_flights(self, start_cell, end_cell, ref_year):
        return (
            _parse_date(start_cell, ref_year),
            _parse_date(end_cell, ref_year),
        )

    def test_landed_2025_campaign_does_not_shift_forward(self):
        """Full reproduction of the 24058 bug + its fix.

        BEFORE fix: bc Start/End cells are year-less 'June 1'/'December 15';
        ref_year defaulted to today (2026) → flights stored 2026 → broken.

        AFTER fix: anchor_year=2025 (dim_projects) feeds _resolve_ref_year,
        which — because the cells are year-less — returns 2025. Per-line
        flights then resolve to 2025, matching the true campaign + delivered
        spend window.
        """
        anchor_year = 2025
        bc_start = _parse_date("June 1", anchor_year)        # seeded w/ anchor
        bc_start_has_year = _cell_has_explicit_year("June 1")  # False
        ref_year = _resolve_ref_year(
            bc_start=bc_start,
            bc_dates_are_year_less=not bc_start_has_year,
            project_anchor_year=anchor_year,
        )
        assert ref_year == 2025

        fs, fe = self._line_flights("Jun 16", "Jul 5", ref_year)
        assert fs == date(2025, 6, 16)
        assert fe == date(2025, 7, 5)
        # The specific guarantee: NOT a year ahead.
        assert fs.year == 2025 and fe.year == 2025

        fs2, fe2 = self._line_flights("Oct 24", "Nov 28", ref_year)
        assert fs2 == date(2025, 10, 24)
        assert fe2 == date(2025, 11, 28)

    def test_explicit_year_sheet_unaffected_by_anchor(self):
        """A sheet that spells out the year is immune to a (possibly stale)
        anchor — proves no cross-project regression."""
        # Sheet says 2026 explicitly; anchor (somehow) says 2025.
        bc_start = _parse_date("June 1, 2026", 2025)
        bc_start_has_year = _cell_has_explicit_year("June 1, 2026")  # True
        ref_year = _resolve_ref_year(
            bc_start=bc_start,
            bc_dates_are_year_less=not bc_start_has_year,
            project_anchor_year=2025,
        )
        assert ref_year == 2026  # explicit sheet year wins
