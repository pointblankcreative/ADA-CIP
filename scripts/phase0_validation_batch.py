#!/usr/bin/env python3
"""Phase 0 validation batch (commit 9, absorbs ADAC-31).

Sweeps every active project for the trailing 14 days, runs the diagnostic
engine through the snapshots cache, and writes a flat CSV that the analyst
team can read alongside their manual reads to validate the engine's outputs
before team rollout.

Why this isn't a daily job: it's a one-shot validation pass. Once the analyst
sign-off is recorded on the ticket, this script can be retired or repurposed
for ad-hoc spot-checks.

Cost: estimated ~$0.01 per full sweep across the current 2-active-project
corpus (3 active projects, but 25049 has no spend data and is skipped). See
``scripts/estimate_backfill_cost.py`` for the full cost model.

Usage:
    python -m scripts.phase0_validation_batch                                    # default 14-day window, write CSV next to the script
    python -m scripts.phase0_validation_batch --window 7                         # last 7 days only
    python -m scripts.phase0_validation_batch --output /tmp/phase0.csv           # custom output path
    python -m scripts.phase0_validation_batch --projects 25034,25042             # restrict to specific projects
    python -m scripts.phase0_validation_batch --dry-run                          # print (project, eval_date) pairs without calling engine

Environment:
    Same as the backend service — needs GOOGLE_APPLICATION_CREDENTIALS set
    so the BQ client + Funnel reads work. Easiest from a workstation that
    already has the cip-sheets-reader SA configured.

Output CSV columns:
    project_code, evaluation_date, campaign_type, health_score, health_status,
    distribution_score, attention_score, resonance_score,        # persuasion pillars
    acquisition_score, funnel_score,                             # conversion pillars
    cached, error
"""

from __future__ import annotations

import argparse
import csv
import logging
import sys
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

# Pydantic-settings reads environment vars on import. Configure logging
# before importing backend so we get the engine's INFO messages.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("phase0_batch")

from backend.services import bigquery_client as bq  # noqa: E402
from backend.services import snapshots               # noqa: E402


@dataclass
class RunResult:
    project_code: str
    evaluation_date: date
    rows: list[dict]
    cached: bool
    error: str | None = None


def fetch_active_projects() -> list[str]:
    """All project_codes in dim_projects with status='active'."""
    sql = f"""
        SELECT project_code
        FROM {bq.table('dim_projects')}
        WHERE status = 'active'
        ORDER BY project_code
    """
    return [r["project_code"] for r in bq.run_query(sql)]


def project_has_spend(project_code: str) -> bool:
    """True iff fact_digital_daily has at least one row for this project."""
    sql = f"""
        SELECT COUNT(*) AS n
        FROM {bq.table('fact_digital_daily')}
        WHERE project_code = @project_code
        LIMIT 1
    """
    rows = bq.run_query(sql, [bq.string_param("project_code", project_code)])
    return bool(rows and rows[0]["n"] > 0)


def date_range(start: date, end: date) -> list[date]:
    """Inclusive list of dates from start to end."""
    days = (end - start).days
    return [start + timedelta(days=i) for i in range(days + 1)]


def run_single(project_code: str, eval_date: date) -> RunResult:
    """Call snapshots.find_or_compute for one (project, date)."""
    try:
        rows, cached = snapshots.find_or_compute(
            project_code=project_code,
            as_of_date=eval_date,
            bypass_cache=False,
        )
        return RunResult(project_code, eval_date, rows, cached)
    except Exception as e:
        logger.exception("Failed: %s @ %s", project_code, eval_date)
        return RunResult(project_code, eval_date, [], False, error=str(e))


def write_csv(results: list[RunResult], output_path: Path) -> None:
    """Flatten DiagnosticOutput rows into one CSV row per (project, date, campaign_type)."""
    fieldnames = [
        "project_code", "evaluation_date", "campaign_type",
        "health_score", "health_status",
        "distribution_score", "attention_score", "resonance_score",
        "acquisition_score", "funnel_score",
        "cached", "error",
    ]
    with output_path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for result in results:
            if result.error:
                writer.writerow({
                    "project_code": result.project_code,
                    "evaluation_date": result.evaluation_date.isoformat(),
                    "campaign_type": "",
                    "error": result.error,
                    "cached": "False",
                })
                continue
            if not result.rows:
                # No media plan / no flight — record an empty row so the
                # analyst sees the project was checked.
                writer.writerow({
                    "project_code": result.project_code,
                    "evaluation_date": result.evaluation_date.isoformat(),
                    "campaign_type": "",
                    "cached": str(result.cached),
                    "error": "no_media_plan_or_flight",
                })
                continue
            for row in result.rows:
                pillars = row.get("pillars") or {}
                writer.writerow({
                    "project_code": result.project_code,
                    "evaluation_date": result.evaluation_date.isoformat(),
                    "campaign_type": row.get("campaign_type", ""),
                    "health_score": row.get("health_score"),
                    "health_status": row.get("health_status", ""),
                    "distribution_score": _pillar_score(pillars, "distribution"),
                    "attention_score":    _pillar_score(pillars, "attention"),
                    "resonance_score":    _pillar_score(pillars, "resonance"),
                    "acquisition_score":  _pillar_score(pillars, "acquisition"),
                    "funnel_score":       _pillar_score(pillars, "funnel"),
                    "cached": str(result.cached),
                    "error": "",
                })


def _pillar_score(pillars: object, name: str) -> str:
    """Pillars is JSON in BQ — comes back as a dict from the client.

    Returns empty string if the pillar isn't present (e.g. no acquisition
    pillar on a persuasion-only campaign). Defensive: handles both nested
    {score: x} and bare score values."""
    if not isinstance(pillars, dict):
        return ""
    p = pillars.get(name)
    if p is None:
        return ""
    if isinstance(p, dict):
        return str(p.get("score", ""))
    return str(p)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--window", type=int, default=14,
                        help="Number of trailing days to sweep (default 14)")
    parser.add_argument("--end-date", type=str, default=None,
                        help="Last day to sweep (default today). YYYY-MM-DD.")
    parser.add_argument("--projects", type=str, default=None,
                        help="Comma-separated list of project codes to restrict the sweep to")
    parser.add_argument("--output", type=Path,
                        default=Path("phase0_validation.csv"),
                        help="CSV output path (default ./phase0_validation.csv)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print the (project, eval_date) sweep without calling the engine")
    args = parser.parse_args()

    end_date = date.fromisoformat(args.end_date) if args.end_date else date.today()
    start_date = end_date - timedelta(days=args.window - 1)

    if args.projects:
        projects = [p.strip() for p in args.projects.split(",") if p.strip()]
    else:
        projects = fetch_active_projects()

    # Skip projects with no spend data — the engine returns no outputs for them
    # and we don't want to fill the CSV with noise.
    projects_with_data = [p for p in projects if project_has_spend(p)]
    skipped = sorted(set(projects) - set(projects_with_data))
    if skipped:
        logger.info("Skipping projects with no spend data: %s", ", ".join(skipped))

    eval_dates = date_range(start_date, end_date)
    logger.info(
        "Sweep: %d projects × %d days = %d (project, eval_date) pairs",
        len(projects_with_data), len(eval_dates),
        len(projects_with_data) * len(eval_dates),
    )

    if args.dry_run:
        for p in projects_with_data:
            for d in eval_dates:
                print(f"{p}  {d.isoformat()}")
        return 0

    results: list[RunResult] = []
    for p in projects_with_data:
        for d in eval_dates:
            results.append(run_single(p, d))

    write_csv(results, args.output)

    error_count = sum(1 for r in results if r.error)
    cached_count = sum(1 for r in results if r.cached)
    output_count = sum(len(r.rows) for r in results if not r.error)
    logger.info(
        "Done. %d runs, %d cached, %d errors, %d DiagnosticOutput rows. Wrote %s",
        len(results), cached_count, error_count, output_count, args.output,
    )
    return 0 if error_count == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
