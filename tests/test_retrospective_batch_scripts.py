"""Tests for the retrospective batch scripts (commits 9 + 10).

The scripts call into snapshots.find_or_compute, which talks to BigQuery.
We don't exercise BQ here — we test the pure helpers (date math, project
filtering, CSV row shaping, cost arithmetic) and stub out BQ for the
end-to-end run loop.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path
from unittest.mock import patch

import pytest

from scripts import estimate_backfill_cost as cost
from scripts import historical_backfill as backfill
from scripts import phase0_validation_batch as phase0


# ── estimate_backfill_cost ──────────────────────────────────────────


def test_queries_per_run_with_and_without_ga4():
    """8 queries when project has GA4 URLs (extra ga4_main fires),
    7 otherwise. Locks the count so adding signals doesn't silently
    inflate the cost estimate."""
    assert cost.queries_per_run(has_ga4=False) == 7
    assert cost.queries_per_run(has_ga4=True) == 8


def test_billed_bytes_uses_10mib_minimum_per_query():
    """The full estimate floor is 10 MiB × queries_per_run."""
    assert cost.billed_bytes_for_run(has_ga4=False) == 7 * 10 * 1024 * 1024
    assert cost.billed_bytes_for_run(has_ga4=True) == 8 * 10 * 1024 * 1024


def test_usd_for_bytes_uses_5_per_tib():
    """Sanity check the pricing arithmetic — 1 TiB → $5."""
    assert cost.usd_for_bytes(1024 ** 4) == pytest.approx(5.00)
    assert cost.usd_for_bytes(0) == 0
    # 100 GiB ≈ $0.488
    assert cost.usd_for_bytes(100 * 1024 ** 3) == pytest.approx(100 / 1024 * 5.00)


def test_estimate_backfill_excludes_active_by_default():
    flights = [
        cost.ProjectFlight("p1", "completed", date(2026, 1, 1), date(2026, 1, 10), 10),
        cost.ProjectFlight("p2", "active",    date(2026, 1, 1), date(2026, 1, 10), 10),
        cost.ProjectFlight("p3", "active",    None,             None,             0),
    ]
    out = cost.estimate_backfill(flights, ga4_codes=set(), include_active=False)
    assert out["project_count"] == 1
    assert out["total_runs"] == 10  # only p1


def test_estimate_backfill_include_active_picks_up_active_with_data():
    flights = [
        cost.ProjectFlight("p1", "completed", date(2026, 1, 1), date(2026, 1, 5),  5),
        cost.ProjectFlight("p2", "active",    date(2026, 1, 1), date(2026, 1, 10), 10),
        cost.ProjectFlight("p3", "active",    None,             None,              0),  # no spend, skipped
    ]
    out = cost.estimate_backfill(flights, ga4_codes=set(), include_active=True)
    assert out["project_count"] == 2  # p3 excluded due to no spend
    assert out["total_runs"] == 15


def test_estimate_phase0_clamps_to_flight_window():
    """Phase 0 is trailing 14 days. A short-flight project (5 days) only
    contributes 5 runs, not 14."""
    flights = [
        cost.ProjectFlight("p1", "active", date(2026, 4, 1), date(2026, 4, 5), 5),
    ]
    out = cost.estimate_phase0(flights, ga4_codes=set(), window_days=14)
    assert out["per_project"][0]["runs"] == 5


# ── phase0_validation_batch ─────────────────────────────────────────


def test_date_range_is_inclusive():
    out = phase0.date_range(date(2026, 4, 1), date(2026, 4, 3))
    assert out == [date(2026, 4, 1), date(2026, 4, 2), date(2026, 4, 3)]


def test_pillar_score_handles_dict_bare_and_missing():
    """Pillars JSON varies in shape — the helper must tolerate all of:
    nested {score: x}, bare numeric, missing, and non-dict input."""
    assert phase0._pillar_score({"distribution": {"score": 0.85}}, "distribution") == "0.85"
    assert phase0._pillar_score({"distribution": 0.85}, "distribution") == "0.85"
    assert phase0._pillar_score({}, "distribution") == ""
    assert phase0._pillar_score(None, "distribution") == ""
    assert phase0._pillar_score({"distribution": {}}, "distribution") == ""


def test_write_csv_emits_one_row_per_diagnostic_output(tmp_path: Path):
    """Mixed projects produce 2 rows per (project, eval_date) — one per
    campaign_type. Pure projects produce 1. Errors and no-data cases get
    a placeholder row each."""
    results = [
        # Pure project, 1 output
        phase0.RunResult(
            project_code="25034",
            evaluation_date=date(2026, 4, 1),
            rows=[{
                "campaign_type": "persuasion",
                "health_score": 87,
                "health_status": "ON_TRACK",
                "pillars": {"distribution": {"score": 0.9}, "attention": {"score": 0.8}},
            }],
            cached=True,
        ),
        # Mixed project, 2 outputs
        phase0.RunResult(
            project_code="25042",
            evaluation_date=date(2026, 4, 1),
            rows=[
                {"campaign_type": "persuasion", "health_score": 75, "health_status": "WATCH",
                 "pillars": {"distribution": {"score": 0.7}}},
                {"campaign_type": "conversion", "health_score": 60, "health_status": "ACTION",
                 "pillars": {"acquisition": {"score": 0.5}, "funnel": {"score": 0.7}}},
            ],
            cached=False,
        ),
        # Error case
        phase0.RunResult("99999", date(2026, 4, 1), [], False, error="kaboom"),
        # No media plan case
        phase0.RunResult("99998", date(2026, 4, 1), [], False),
    ]
    out_path = tmp_path / "out.csv"
    phase0.write_csv(results, out_path)
    lines = out_path.read_text().splitlines()

    # Header + 5 data rows (1 + 2 + 1 + 1)
    assert len(lines) == 6
    # Mixed-project rows are present and labelled by campaign_type
    assert any("25042,2026-04-01,persuasion" in line for line in lines)
    assert any("25042,2026-04-01,conversion" in line for line in lines)
    # Error row
    assert any("99999,2026-04-01,," in line and "kaboom" in line for line in lines)
    # No-data row
    assert any("99998,2026-04-01," in line and "no_media_plan_or_flight" in line for line in lines)


# ── historical_backfill ─────────────────────────────────────────────


def test_project_flight_date_range_is_inclusive():
    f = backfill.ProjectFlight(
        project_code="p1", status="completed",
        first_spend_date=date(2026, 4, 1),
        last_spend_date=date(2026, 4, 3),
    )
    assert f.flight_days == 3
    assert f.date_range() == [date(2026, 4, 1), date(2026, 4, 2), date(2026, 4, 3)]


def test_filter_targets_default_completed_only():
    flights = [
        backfill.ProjectFlight("p1", "completed", date(2026, 4, 1), date(2026, 4, 5)),
        backfill.ProjectFlight("p2", "active",    date(2026, 4, 1), date(2026, 4, 5)),
    ]
    targets = backfill.filter_targets(flights, include_active=False)
    assert [t.project_code for t in targets] == ["p1"]


def test_filter_targets_include_active_picks_up_both():
    flights = [
        backfill.ProjectFlight("p1", "completed", date(2026, 4, 1), date(2026, 4, 5)),
        backfill.ProjectFlight("p2", "active",    date(2026, 4, 1), date(2026, 4, 5)),
    ]
    targets = backfill.filter_targets(flights, include_active=True)
    assert sorted(t.project_code for t in targets) == ["p1", "p2"]


def test_run_single_swallows_exceptions_and_returns_error():
    """One bad day must not kill the whole batch — the engine raising
    surfaces as an error tuple, not a re-raise."""
    f = backfill.ProjectFlight("p1", "completed", date(2026, 4, 1), date(2026, 4, 1))

    with patch.object(backfill.snapshots, "find_or_compute", side_effect=RuntimeError("nope")):
        cached, err = backfill.run_single(f, date(2026, 4, 1), bypass_cache=False)

    assert cached is False
    assert err == "nope"


def test_run_single_returns_cached_flag_from_snapshots():
    f = backfill.ProjectFlight("p1", "completed", date(2026, 4, 1), date(2026, 4, 1))

    with patch.object(backfill.snapshots, "find_or_compute",
                      return_value=([{"campaign_type": "persuasion"}], True)):
        cached, err = backfill.run_single(f, date(2026, 4, 1), bypass_cache=False)

    assert cached is True
    assert err is None
