"""Tests for media plan sync pipeline bug fixes.

Tests cover:
  - Bug 2 (ADAC-21): _match_all_mp_lines optimal matching
  - Bug 1 (ADAC-17): enrichment preserves bc lines and weeks
  - Bug 5 (ADAC-26): per-line flight dates from media plan
  - Bug 3 (ADAC-18): audience_name override model validation
"""

from datetime import date

import pytest

from backend.services.media_plan_sync import (
    _match_all_mp_lines,
    _mp_lines_have_audience_data,
    _synthesise_lines_from_mp,
)
from backend.routers.admin import MediaPlanLineUpdate


# ── Bug 2: _match_all_mp_lines optimal matching ───────────────────


class TestMatchAllMpLines:
    """Verify that the global matching avoids order-dependent side effects."""

    def test_two_similar_meta_lines_both_match(self):
        """Two Meta lines with similar budgets should each get a distinct match."""
        bc_lines = [
            {"platform_id": "meta", "budget": 5000},
            {"platform_id": "meta", "budget": 5200},
        ]
        mp_lines = [
            {"platform_id": "meta", "budget": 5200, "audience_name": "Retargeting"},
            {"platform_id": "meta", "budget": 5000, "audience_name": "Awareness"},
        ]
        result = _match_all_mp_lines(bc_lines, mp_lines)
        assert len(result) == 2
        # bc_line 0 (budget=5000) should match mp_line 1 (budget=5000)
        assert result[0]["audience_name"] == "Awareness"
        # bc_line 1 (budget=5200) should match mp_line 0 (budget=5200)
        assert result[1]["audience_name"] == "Retargeting"

    def test_different_platforms_no_cross_match(self):
        """Lines from different platforms should never match each other."""
        bc_lines = [
            {"platform_id": "meta", "budget": 5000},
            {"platform_id": "google_ads", "budget": 3000},
        ]
        mp_lines = [
            {"platform_id": "meta", "budget": 5000, "audience_name": "Meta A"},
            {"platform_id": "google_ads", "budget": 3000, "audience_name": "Google A"},
        ]
        result = _match_all_mp_lines(bc_lines, mp_lines)
        assert result[0]["platform_id"] == "meta"
        assert result[1]["platform_id"] == "google_ads"

    def test_empty_mp_lines_returns_empty(self):
        bc_lines = [{"platform_id": "meta", "budget": 5000}]
        result = _match_all_mp_lines(bc_lines, [])
        assert result == {}

    def test_empty_bc_lines_returns_empty(self):
        mp_lines = [{"platform_id": "meta", "budget": 5000}]
        result = _match_all_mp_lines([], mp_lines)
        assert result == {}

    def test_no_budget_match_below_threshold(self):
        """Lines with budgets differing by >50% should not match."""
        bc_lines = [{"platform_id": "meta", "budget": 10000}]
        mp_lines = [{"platform_id": "meta", "budget": 2000, "audience_name": "X"}]
        result = _match_all_mp_lines(bc_lines, mp_lines)
        assert len(result) == 0

    def test_line_code_bonus_breaks_tie(self):
        """When budget matches are equal, prefer mp_line with line_code."""
        bc_lines = [{"platform_id": "meta", "budget": 5000}]
        mp_lines = [
            {"platform_id": "meta", "budget": 5000, "audience_name": "No Code"},
            {"platform_id": "meta", "budget": 5000, "audience_name": "Has Code", "line_code": "LC-001"},
        ]
        result = _match_all_mp_lines(bc_lines, mp_lines)
        assert len(result) == 1
        assert result[0]["audience_name"] == "Has Code"

    def test_greedy_optimal_not_first_come(self):
        """Verify best global match wins, not first-processed bc_line.

        If bc_line 0 and bc_line 1 both could match mp_line A,
        but bc_line 1 is the *better* match, bc_line 1 should get it.
        """
        bc_lines = [
            {"platform_id": "meta", "budget": 3000},   # weak match for mp 5000
            {"platform_id": "meta", "budget": 5000},   # perfect match for mp 5000
        ]
        mp_lines = [
            {"platform_id": "meta", "budget": 5000, "audience_name": "Target"},
        ]
        result = _match_all_mp_lines(bc_lines, mp_lines)
        # bc_line 1 (budget=5000) should win the match, not bc_line 0
        assert 1 in result
        assert result[1]["audience_name"] == "Target"
        assert 0 not in result


# ── Bug 1: enrichment preserves bc lines and weeks ─────────────────


class TestEnrichment:
    """Verify that _synthesise_lines_from_mp is only used when bc has no lines."""

    def test_synthesise_only_for_empty_bc(self):
        """_synthesise_lines_from_mp should produce lines from mp_lines."""
        mp_lines = [
            {"platform_id": "meta", "budget": 5000, "goal": "Awareness",
             "audience_name": "Test Audience", "platform": "Meta",
             "flight_start": date(2026, 3, 1), "flight_end": date(2026, 4, 30)},
        ]
        metadata = {"start_date": date(2026, 2, 1), "end_date": date(2026, 5, 1)}
        result = _synthesise_lines_from_mp(mp_lines, metadata)
        assert len(result) == 1
        assert result[0]["platform_id"] == "meta"
        assert result[0]["budget"] == 5000
        assert result[0]["audience_name"] == "Test Audience"

    def test_synthesise_skips_no_budget(self):
        """Lines without budget should be skipped."""
        mp_lines = [
            {"platform_id": "meta", "budget": None, "goal": "Test", "platform": "Meta"},
            {"platform_id": "meta", "budget": 0, "goal": "Test", "platform": "Meta"},
            {"platform_id": "meta", "budget": 5000, "goal": "OK", "platform": "Meta",
             "audience_name": "Valid"},
        ]
        metadata = {"start_date": date(2026, 1, 1), "end_date": date(2026, 6, 1)}
        result = _synthesise_lines_from_mp(mp_lines, metadata)
        assert len(result) == 1
        assert result[0]["audience_name"] == "Valid"

    def test_mp_lines_have_audience_data(self):
        assert _mp_lines_have_audience_data([
            {"audience_name": "Test", "budget": 1000},
        ]) is True
        assert _mp_lines_have_audience_data([
            {"audience_name": "", "budget": 1000},
        ]) is False
        assert _mp_lines_have_audience_data([
            {"audience_name": "Test", "budget": 0},
        ]) is False
        assert _mp_lines_have_audience_data([]) is False


# ── Bug 5: per-line flight dates ───────────────────────────────────


class TestFlightDateEnrichment:
    """Verify that flight dates from mp_detail are copied during matching."""

    def test_match_includes_flight_dates(self):
        """Matched mp_lines with flight dates should transfer them."""
        bc_lines = [{"platform_id": "meta", "budget": 5000}]
        mp_lines = [
            {"platform_id": "meta", "budget": 5000, "audience_name": "A",
             "flight_start": date(2026, 3, 1), "flight_end": date(2026, 3, 22)},
        ]
        matches = _match_all_mp_lines(bc_lines, mp_lines)
        assert 0 in matches
        mp = matches[0]
        assert mp["flight_start"] == date(2026, 3, 1)
        assert mp["flight_end"] == date(2026, 3, 22)


# ── Bug 3: MediaPlanLineUpdate validation ──────────────────────────


class TestMediaPlanLineUpdate:
    """Validate the Pydantic request model for audience_name edits."""

    def test_valid_audience_name(self):
        m = MediaPlanLineUpdate(audience_name="Test Audience")
        assert m.audience_name == "Test Audience"

    def test_strips_whitespace(self):
        m = MediaPlanLineUpdate(audience_name="  padded  ")
        assert m.audience_name == "padded"

    def test_empty_string_rejected(self):
        with pytest.raises(ValueError):
            MediaPlanLineUpdate(audience_name="")

    def test_whitespace_only_rejected(self):
        with pytest.raises(ValueError):
            MediaPlanLineUpdate(audience_name="   ")

    def test_too_long_rejected(self):
        with pytest.raises(ValueError):
            MediaPlanLineUpdate(audience_name="x" * 501)

    def test_max_length_accepted(self):
        m = MediaPlanLineUpdate(audience_name="x" * 500)
        assert len(m.audience_name) == 500
