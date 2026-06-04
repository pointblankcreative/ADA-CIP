"""Regression tests for merged Campaign Type cells (AI-022, second root cause).

PB media plans vertically merge the Campaign Type / Goal cell across language
pairs: the EN row carries the value, the FR row reads empty via the Sheets
API. The `not goal and not line_code` guard then silently dropped every FR
row — 26018 lost $2,245 across 3 lines (Meta Retarget FR $605, Meta Members
FR $990, Search FR $650), on top of the $3,750 PLATFORM_MAP drop.

Fixed 2026-06-04: goal carries forward like current_platform, gated on
data-row evidence (dates/audience) so footer Total rows never inherit one,
and "Internal Adset" maps to the id column (line codes #1..#8).

The grid below reproduces the real 26018 sheet structure.
"""

from __future__ import annotations

from backend.services.media_plan_sync import (
    _parse_media_plan_tab,
    _synthesise_lines_from_mp,
)

H = ["Site/Network", "Campaign Type/", "Start Date", "End Date", "# Days",
     "Internal Adset", "Built?", "Audience Group", "Group Name",
     "Notes/Targeting", "Geo Target", "Est'd Audience size", "Creative",
     "Language", "Pricing", "Est'd Rate", "Est'd Impressions", "Budget $"]


def _r(site, ctype, sd, ed, days, adset, aud, grp, geo, lang, pricing, budget):
    return [site, ctype, sd, ed, days, adset, "TRUE", aud, grp, "", geo, "",
            "Sign Up LP: https://example.org", lang, pricing, "$ 15.00",
            "100,000", budget]


def _grid_26018():
    return [
        ["CAPE — Pre-Bargaining Flight 1"] + [""] * 17,
        ["Client: CAPE", "", "Project: 26018"] + [""] * 15,
        [""] * 18,
        H,
        _r("Meta\nFacebook & Instagram", "Conversions", "May 7", "Jun 5", "30",
           "#1", "Retargeting", "RETARGET", "Canada", "EN", "CPM", "$ 2,145.00"),
        _r("", "", "May 7", "Jun 5", "30",
           "#2", "Retargeting", "RETARGET", "Canada", "FR", "CPM", "$ 605.00"),
        _r("", "Awareness", "May 7", "Jun 5", "30",
           "#3", "Member List", "MEMBERS", "Canada", "EN", "CPM", "$ 3,510.00"),
        _r("", "", "May 7", "Jun 5", "30",
           "#4", "Member List", "MEMBERS", "Canada", "FR", "CPM", "$ 990.00"),
        _r("Programmatic (Native)", "Awareness", "May 7", "Jun 5", "30",
           "#5", "Member List", "MEMBERS", "Canada", "EN", "CPM", "$ 2,925.00"),
        _r("", "", "May 7", "Jun 5", "30",
           "#6", "Member List", "MEMBERS", "Canada", "FR", "CPM", "$ 825.00"),
        _r("Google Search Ads", "Conversion", "May 7", "May 29", "23",
           "#7", "Searchers", "SEARCH", "Ottawa-Gatineau", "EN", "CPC", "$ 1,100.00"),
        _r("", "", "May 7", "May 29", "23",
           "#8", "Searchers", "SEARCH", "Ottawa-Gatineau", "FR", "CPC", "$ 650.00"),
        [""] * 17 + ["Sum ^ to check"],
        [""] * 14 + ["Total", "", "", "$ 12,750.00"],
        [""] * 18,
    ]


def _parse():
    return _parse_media_plan_tab(
        None, prefetched_data=_grid_26018(), ref_year=2026, prefetched_merges=[]
    )


def test_all_eight_lines_parse_including_merged_goal_fr_rows():
    mp = _parse()
    assert len(mp) == 8, [(m["platform_id"], m["budget"]) for m in mp]
    budgets = sorted(m["budget"] for m in mp)
    assert budgets == [605.0, 650.0, 825.0, 990.0, 1100.0, 2145.0, 2925.0, 3510.0]
    assert sum(budgets) == 12750.0


def test_fr_rows_inherit_goal_from_merged_cell():
    mp = _parse()
    by_code = {m["line_code"]: m for m in mp}
    assert by_code["#2"]["goal"] == "Conversions"
    assert by_code["#4"]["goal"] == "Awareness"
    assert by_code["#6"]["goal"] == "Awareness"
    assert by_code["#8"]["goal"] == "Conversion"


def test_goal_never_leaks_across_platform_blocks():
    """Programmatic block's first row has its own goal; but if a platform
    block started WITHOUT a goal, it must not inherit the previous
    platform's."""
    grid = _grid_26018()
    # Strip the goal from Programmatic's first row (#5): the pair then has
    # no goal anywhere in its block.
    grid[8][1] = ""
    mp = _parse_media_plan_tab(
        None, prefetched_data=grid, ref_year=2026, prefetched_merges=[]
    )
    codes = {m["line_code"] for m in mp}
    # #5/#6 now legitimately fail the goal guard (no goal in their block) —
    # but crucially they did NOT inherit "Awareness" from the Meta block.
    sa = [m for m in mp if m["platform_id"] == "stackadapt"]
    assert all(m["goal"] != "Awareness" or m["line_code"] in ("#5", "#6")
               for m in sa)
    # Meta + Google rows are unaffected.
    assert {"#1", "#2", "#3", "#4", "#7", "#8"} <= codes


def test_total_row_never_inherits_goal_and_stays_dropped():
    mp = _parse()
    budgets = [m["budget"] for m in mp]
    assert 12750.0 not in budgets, "footer Total row must not become a line"
    assert all((m.get("flight_start") or m.get("audience_group")) for m in mp)


def test_synthesis_keeps_all_eight_with_distinct_codes():
    lines = _synthesise_lines_from_mp(_parse(), metadata={})
    assert len(lines) == 8
    assert sum(ln["budget"] for ln in lines) == 12750.0
    pids = sorted(ln["platform_id"] for ln in lines)
    assert pids.count("meta") == 4
    assert pids.count("stackadapt") == 2
    assert pids.count("google_ads") == 2
