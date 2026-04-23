"""Tests for media plan sync pipeline bug fixes.

Tests cover:
  - Bug 2 (ADAC-21): _match_all_mp_lines optimal matching
  - Bug 1 (ADAC-17): enrichment preserves bc lines and weeks
  - Bug 5 (ADAC-26): per-line flight dates from media plan
  - Bug 3 (ADAC-18): audience_name override model validation
  - Bundled-optimization PR 1a: header widening + line_code extraction
"""

from datetime import date

import pytest

from backend.services.media_plan_sync import (
    _extract_line_code,
    _filter_canonical_tabs,
    _match_all_mp_lines,
    _mp_lines_have_audience_data,
    _parse_media_plan_tab,
    _synthesise_lines_from_mp,
)
from backend.routers.admin import MediaPlanLineUpdate


# ── ADAC-50: Tab disambiguation ──────────────────────────────────


class TestFilterCanonicalTabs:
    """Verify that non-canonical media plan tabs are filtered out."""

    def test_filters_client_copy(self):
        tabs = ["Media Plan V2", "[CLIENT] Media Plan V2"]
        assert _filter_canonical_tabs(tabs) == ["Media Plan V2"]

    def test_filters_only_subset(self):
        tabs = ["Media Plan V2", "Media Plan V2 F1 Only"]
        assert _filter_canonical_tabs(tabs) == ["Media Plan V2"]

    def test_filters_multiple_non_canonical(self):
        """The exact OSSTF scenario: three tabs, only one canonical."""
        tabs = ["Media Plan V2", "[CLIENT] Media Plan V2", "Media Plan V2 F1 Only"]
        assert _filter_canonical_tabs(tabs) == ["Media Plan V2"]

    def test_keeps_all_when_all_non_canonical(self):
        """If every tab matches a non-canonical pattern, keep them all."""
        tabs = ["[CLIENT] Media Plan V2 Only", "[CLIENT] Old Plan"]
        result = _filter_canonical_tabs(tabs)
        assert set(result) == set(tabs)

    def test_single_tab_unchanged(self):
        assert _filter_canonical_tabs(["Media Plan"]) == ["Media Plan"]

    def test_empty_list(self):
        assert _filter_canonical_tabs([]) == []

    def test_filters_draft_and_backup(self):
        tabs = ["Media Plan V2", "Media Plan Draft", "Media Plan Backup"]
        assert _filter_canonical_tabs(tabs) == ["Media Plan V2"]

    def test_case_insensitive(self):
        tabs = ["Media Plan V2", "Media Plan V2 ONLY"]
        assert _filter_canonical_tabs(tabs) == ["Media Plan V2"]

    def test_preserves_multiple_canonical(self):
        """Two legitimate tabs (e.g. different flights) should both be kept."""
        tabs = ["Media Plan V2 F1", "Media Plan V2 F2"]
        assert _filter_canonical_tabs(tabs) == ["Media Plan V2 F1", "Media Plan V2 F2"]


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


# ── Bundled-optimization PR 1a: _extract_line_code ────────────────


class TestExtractLineCode:
    """Extract line codes from Squamish-style (#XX) and OSSTF-style (1A) cells.

    Source sheets in the wild:
      - Squamish (25034) Col G "Group Name": "#09 North Van Engagers",
        "#01 Engagers BC", "#91 Organic Boost"
      - OSSTF (25042) Col G "ID": "1", "1A", "1B", "2A", "2B" (code only)
    """

    def test_squamish_hash_prefix_with_remainder(self):
        assert _extract_line_code("#09 North Van Engagers") == (
            "#09",
            "North Van Engagers",
        )

    def test_squamish_hash_prefix_alone(self):
        assert _extract_line_code("#01") == ("#01", "")

    def test_squamish_non_sequential_code(self):
        """#91 (TikTok Organic Boost) must not be special-cased."""
        assert _extract_line_code("#91 Organic Boost") == (
            "#91",
            "Organic Boost",
        )

    def test_osstf_bare_alphanumeric_alone(self):
        assert _extract_line_code("1A") == ("1A", "")
        assert _extract_line_code("2B") == ("2B", "")

    def test_osstf_bare_number_alone(self):
        """OSSTF sometimes uses plain '1' or '2' with no letter suffix."""
        assert _extract_line_code("1") == ("1", "")
        assert _extract_line_code("2") == ("2", "")

    def test_osstf_code_with_trailing_description(self):
        """Permissive fallback: '1A Teachers' → ('1A', 'Teachers')."""
        assert _extract_line_code("1A Teachers") == ("1A", "Teachers")

    def test_strips_surrounding_whitespace(self):
        assert _extract_line_code("  #05 Awareness  ") == ("#05", "Awareness")
        assert _extract_line_code("  1A  ") == ("1A", "")

    def test_no_code_preserves_remainder(self):
        """Plain descriptive text with no code should return ('', original)."""
        assert _extract_line_code("Retargeting Ontario") == (
            "",
            "Retargeting Ontario",
        )

    def test_empty_and_none(self):
        assert _extract_line_code("") == ("", "")
        assert _extract_line_code(None) == ("", "")
        assert _extract_line_code("   ") == ("", "")

    def test_two_digit_code(self):
        """Codes >=10 should work: #09, #10, #14."""
        assert _extract_line_code("#10") == ("#10", "")
        assert _extract_line_code("#14 Squamish Engagers") == (
            "#14",
            "Squamish Engagers",
        )


# ── Bundled-optimization PR 1a: _parse_media_plan_tab header widening ──


# Minimum rows required by parser (len(all_data) >= 14 and header at idx 4..14)
def _pad_to_min(rows: list, target: int = 16) -> list:
    while len(rows) < target:
        rows.append([""] * (len(rows[0]) if rows else 1))
    return rows


# Squamish (25034) column order as confirmed via Drive MCP read
_SQUAMISH_HEADER = [
    "Site/Network",
    "Campaign Type/Objective",
    "Start Date",
    "End Date",
    "# Days",
    "Audience Group",
    "Group Name",
    "Notes/Targeting",
    "Geo Target",
    "Creative",
    "Pricing",
    "Est'd Rate",
    "Est'd Impressions",
    "Budget $",
]


def _squamish_data(*data_rows: list[str]) -> list[list[str]]:
    """Build a Squamish-layout sheet. Header lands at row index 5."""
    rows: list[list[str]] = [[""] * len(_SQUAMISH_HEADER) for _ in range(5)]
    rows.append(_SQUAMISH_HEADER)
    rows.extend(list(data_rows))
    return _pad_to_min(rows)


# OSSTF (25042) column order as confirmed via Drive MCP read
_OSSTF_HEADER = [
    "Site/Network",
    "Flight",
    "Goal",
    "Start",
    "End",
    "Days",
    "ID",
    "Audience Name",
    "Geo",
    "Audience Targeting",
    "Technical Targeting",
    "Creative",
    "Pricing",
    "Est'd Impressions",
    "Budget",
]


def _osstf_data(*data_rows: list[str]) -> list[list[str]]:
    """Build an OSSTF-layout sheet. Header lands at row index 5."""
    rows: list[list[str]] = [[""] * len(_OSSTF_HEADER) for _ in range(5)]
    rows.append(_OSSTF_HEADER)
    rows.extend(list(data_rows))
    return _pad_to_min(rows)


class TestParseMediaPlanTabSquamish:
    """Squamish-style plans with decorated headers (Start Date, # Days, Budget $, Group Name)."""

    def test_widened_date_and_budget_headers(self):
        """'Start Date'/'End Date'/'# Days'/'Budget $' must be recognized."""
        row = [
            "Meta (Facebook, Instagram)",
            "Conversions",
            "March 17",
            "April 12",
            "27",
            "Conversions CA",
            "#09 North Van Engagers",
            "",
            "North Van",
            "Creative Bundle A",
            "CPC",
            "$1.50",
            "50,000",
            "$2,238.19",
        ]
        data = _squamish_data(row)
        lines = _parse_media_plan_tab(None, prefetched_data=data, ref_year=2026)

        assert len(lines) == 1
        ln = lines[0]
        assert ln["flight_start"] == date(2026, 3, 17)
        assert ln["flight_end"] == date(2026, 4, 12)
        assert ln["days"] == "27"
        assert ln["budget"] == pytest.approx(2238.19)

    def test_group_name_populates_line_code_and_audience(self):
        """Squamish Col G 'Group Name' → line_code + audience_name (remainder)."""
        row = [
            "Meta (Facebook, Instagram)",
            "Conversions",
            "March 26",
            "April 22",
            "27",
            "Conversions CA",
            "#11 Viewers BC",
            "",
            "BC Excl.",
            "Creative Bundle A",
            "CPC",
            "$1.50",
            "50,000",
            "$1,552.00",
        ]
        data = _squamish_data(row)
        lines = _parse_media_plan_tab(None, prefetched_data=data, ref_year=2026)

        assert len(lines) == 1
        ln = lines[0]
        assert ln["line_code"] == "#11"
        assert ln["audience_name"] == "Viewers BC"

    def test_platform_normalisation(self):
        """'Meta (Facebook, Instagram)' → platform_id 'meta'."""
        row = [
            "Meta (Facebook, Instagram)",
            "Conversions",
            "March 17",
            "April 12",
            "27",
            "Conversions CA",
            "#09 North Van Engagers",
            "",
            "North Van",
            "",
            "CPC",
            "",
            "",
            "$2,238.19",
        ]
        data = _squamish_data(row)
        lines = _parse_media_plan_tab(None, prefetched_data=data, ref_year=2026)
        assert lines[0]["platform_id"] == "meta"

    def test_no_goal_but_line_code_is_kept(self):
        """Squamish Col B is 'Campaign Type/Objective' — goal may not match.

        Row must still survive because line_code is present (from Group Name).
        """
        row = [
            "Meta (Facebook, Instagram)",
            "",  # goal blank
            "March 17",
            "April 12",
            "27",
            "Conversions CA",
            "#09 North Van Engagers",
            "",
            "",
            "",
            "",
            "",
            "",
            "$2,238.19",
        ]
        data = _squamish_data(row)
        lines = _parse_media_plan_tab(None, prefetched_data=data, ref_year=2026)
        assert len(lines) == 1
        assert lines[0]["line_code"] == "#09"

    def test_non_sequential_line_code_preserved(self):
        """#91 organic boost — non-monotonic codes must not be special-cased."""
        row = [
            "TikTok",
            "",
            "March 17",
            "April 12",
            "27",
            "Organic Boost",
            "#91 Organic Boost",
            "",
            "BC",
            "",
            "",
            "",
            "",
            "$500.00",
        ]
        data = _squamish_data(row)
        lines = _parse_media_plan_tab(None, prefetched_data=data, ref_year=2026)
        assert len(lines) == 1
        assert lines[0]["line_code"] == "#91"
        assert lines[0]["platform_id"] == "tiktok"

    def test_non_numeric_impressions_tolerated(self):
        """'TBD' in Est'd Impressions must not crash the parser."""
        row = [
            "Meta (Facebook, Instagram)",
            "Conversions",
            "March 17",
            "April 12",
            "27",
            "Conversions CA",
            "#09 North Van Engagers",
            "",
            "North Van",
            "",
            "CPC",
            "",
            "TBD",
            "$2,238.19",
        ]
        data = _squamish_data(row)
        lines = _parse_media_plan_tab(None, prefetched_data=data, ref_year=2026)
        assert len(lines) == 1
        # _parse_money returns None for non-numeric
        assert lines[0]["estimated_impressions"] is None

    def test_multiple_lines_parse_independently(self):
        """All three of Flight 2's Meta sub-bundles parse as three rows."""
        rows = [
            ["Meta (Facebook, Instagram)", "Conversions", "March 17", "April 12",
             "27", "Conversions CA", "#09 North Van Engagers", "",
             "North Van", "", "CPC", "", "", "$1,119.10"],
            ["", "", "", "", "", "", "#10 North Van List", "",
             "North Van", "", "CPC", "", "", "$1,119.09"],
            ["", "", "March 26", "April 22", "27", "Conversions CA",
             "#11 Viewers BC", "", "BC Excl.", "", "CPC", "", "", "$1,552.00"],
        ]
        data = _squamish_data(*rows)
        lines = _parse_media_plan_tab(None, prefetched_data=data, ref_year=2026)
        assert len(lines) == 3
        codes = [ln["line_code"] for ln in lines]
        assert codes == ["#09", "#10", "#11"]


class TestParseMediaPlanTabOsstf:
    """OSSTF-style plans with bare headers. Must remain fully parseable."""

    def test_bare_headers_still_work(self):
        """Regression guard: 'Start'/'End'/'Days'/'Budget' must continue to match."""
        row = [
            "Meta",
            "1",
            "Awareness",
            "Apr 1",
            "Apr 30",
            "30",
            "1A",
            "Provincial Teachers",
            "Ontario",
            "",
            "",
            "",
            "CPC",
            "100,000",
            "$10,000.00",
        ]
        data = _osstf_data(row)
        lines = _parse_media_plan_tab(None, prefetched_data=data, ref_year=2026)
        assert len(lines) == 1
        ln = lines[0]
        assert ln["flight_start"] == date(2026, 4, 1)
        assert ln["flight_end"] == date(2026, 4, 30)
        assert ln["days"] == "30"
        assert ln["budget"] == pytest.approx(10000.00)

    def test_osstf_line_code_and_audience_separate(self):
        """ID col holds code-only ('1A'); Audience Name is separate."""
        row = [
            "Meta",
            "1",
            "Awareness",
            "Apr 1",
            "Apr 30",
            "30",
            "1A",
            "Provincial Teachers",
            "Ontario",
            "",
            "",
            "",
            "CPC",
            "",
            "$10,000.00",
        ]
        data = _osstf_data(row)
        lines = _parse_media_plan_tab(None, prefetched_data=data, ref_year=2026)
        assert len(lines) == 1
        ln = lines[0]
        assert ln["line_code"] == "1A"
        assert ln["audience_name"] == "Provincial Teachers"

    def test_osstf_multiple_flights(self):
        """Two lines across two flights; each gets distinct code + audience."""
        rows = [
            ["Meta", "1", "Awareness", "Apr 1", "Apr 30", "30",
             "1A", "Provincial Teachers", "Ontario", "", "", "", "CPC", "", "$5,000.00"],
            ["Google", "2", "Conversions", "May 1", "May 30", "30",
             "2B", "Union Members", "Ontario", "", "", "", "CPC", "", "$3,000.00"],
        ]
        data = _osstf_data(*rows)
        lines = _parse_media_plan_tab(None, prefetched_data=data, ref_year=2026)
        assert len(lines) == 2
        assert lines[0]["line_code"] == "1A"
        assert lines[0]["audience_name"] == "Provincial Teachers"
        assert lines[0]["platform_id"] == "meta"
        assert lines[1]["line_code"] == "2B"
        assert lines[1]["audience_name"] == "Union Members"
        assert lines[1]["platform_id"] == "google_ads"


class TestParseMediaPlanTabMissingColumnsWarning:
    """Silent-fail warnings — parser must log when expected columns are missing."""

    def test_warns_when_budget_missing(self, caplog):
        """No budget column at all → WARN log but parsing continues."""
        # Deliberately drop budget column from Squamish header
        header = _SQUAMISH_HEADER[:-1]  # remove 'Budget $'
        row = [
            "Meta (Facebook, Instagram)",
            "Conversions",
            "March 17",
            "April 12",
            "27",
            "Conversions CA",
            "#09 North Van Engagers",
            "",
            "North Van",
            "",
            "CPC",
            "",
            "",
        ]
        rows: list[list[str]] = [[""] * len(header) for _ in range(5)]
        rows.append(header)
        rows.append(row)
        while len(rows) < 16:
            rows.append([""] * len(header))

        import logging
        with caplog.at_level(logging.WARNING):
            lines = _parse_media_plan_tab(None, prefetched_data=rows, ref_year=2026)

        # Parsing still succeeds (with budget=None on the line)
        assert len(lines) == 1
        assert lines[0]["budget"] is None
        # A warning mentions the missing column
        assert any("budget" in rec.message.lower() for rec in caplog.records)
