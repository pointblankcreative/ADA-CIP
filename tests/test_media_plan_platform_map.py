"""Regression tests for PLATFORM_MAP aliases (AI-002 / AI-022 root cause).

26018's media plan labels its StackAdapt buys "Programmatic (Native)". That
alias was missing from PLATFORM_MAP, so _normalise_platform returned an
unrecognised id and _synthesise_lines_from_mp silently dropped both rows
($3,750 of planned spend) at DEBUG level. Fixed 2026-06-04: "programmatic"
maps to stackadapt (post-Hivestack, programmatic == StackAdapt at PB) and the
skip log is WARNING level.

Note: the legacy "perion"/"hivestack" PLATFORM_MAP aliases were removed. DOOH is
now bought through StackAdapt, and those aliases used to force a legacy label to
platform_id "perion" — a feed pacing never reads. "Hivestack" now slugifies to
"hivestack" via the fallback (out of PLATFORM_MAP.values(), so is_direct=True).
"""

from __future__ import annotations

import logging

from backend.services.media_plan_sync import (
    PLATFORM_MAP,
    _normalise_platform,
    _synthesise_lines_from_mp,
)


def test_programmatic_variants_map_to_stackadapt():
    for raw in (
        "Programmatic (Native)",
        "Programmatic (Display)",
        "Programmatic (OLV)",
        "programmatic",
        "Programmatic  (CTV)",
    ):
        assert _normalise_platform(raw) == "stackadapt", raw


def test_existing_aliases_unchanged():
    assert _normalise_platform("Open Internet") == "stackadapt"
    assert _normalise_platform("StackAdapt") == "stackadapt"
    assert _normalise_platform("Meta (Facebook, Instagram)") == "meta"
    assert _normalise_platform("Google Ads") == "google_ads"
    assert _normalise_platform("Hivestack") == "hivestack"
    assert _normalise_platform(None) is None


def test_unknown_platform_still_falls_through_to_slug():
    assert _normalise_platform("Blimp Ads") == "blimp_ads"
    assert "blimp_ads" not in PLATFORM_MAP.values()


def _mp_row(platform, budget=1000.0, **overrides):
    base = {
        "platform": platform,
        "platform_id": _normalise_platform(platform),
        "budget": budget,
        "goal": "Awareness",
        "audience_name": "MEMBERS",
        "flight_start": None,
        "flight_end": None,
    }
    base.update(overrides)
    return base


def test_synthesise_keeps_programmatic_rows_as_stackadapt():
    """The 26018 case: two Programmatic (Native) rows must survive synthesis."""
    mp_lines = [
        _mp_row("Programmatic (Native)", 2925.0, audience_name="MEMBERS EN"),
        _mp_row("Programmatic (Native)", 825.0, audience_name="MEMBERS FR"),
        _mp_row("Meta (Facebook, Instagram)", 3510.0),
    ]
    lines = _synthesise_lines_from_mp(mp_lines, metadata={})
    pids = [ln["platform_id"] for ln in lines]
    assert pids.count("stackadapt") == 2, lines
    assert "meta" in pids
    budgets = sorted(ln["budget"] for ln in lines if ln["platform_id"] == "stackadapt")
    assert budgets == [825.0, 2925.0]


def test_synthesise_skips_unrecognised_at_warning_level(caplog):
    mp_lines = [_mp_row("Blimp Ads", 500.0)]
    with caplog.at_level(logging.WARNING):
        lines = _synthesise_lines_from_mp(mp_lines, metadata={})
    assert lines == []
    assert any(
        "unrecognised platform" in rec.message and rec.levelno == logging.WARNING
        for rec in caplog.records
    ), caplog.records
