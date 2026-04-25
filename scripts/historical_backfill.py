#!/usr/bin/env python3
"""ADAC-37 historical backfill (commit 10).

Replays the diagnostic engine for every (project, evaluation_date) across
each project's full flight, writing the results to fact_diagnostic_signals.

The corpus this populates is what gives the retrospective UI something to
render for any project / date combination without needing to recompute on
the fly (which would still work via cache-on-read, but at higher latency
and slightly higher BQ spend over time).

Cost: estimated ~$0.34 for the current 8-completed-project corpus, ~$0.40
if --include-active is passed. See ``scripts/estimate_backfill_cost.py``
for the cost model.

Usage:
    python -m scripts.historical_backfill --dry-run                              # print (project, date) pairs without firing engine
    python -m scripts.historical_backfill                                        # backfill completed projects only
    python -m scripts.historical_backfill --include-active                       # also backfill active projects
    python -m scripts.historical_backfill --projects 24058,25001                 # restrict to specific projects
    python -m scripts.historical_backfill --force                                # bypass cache (recompute even if rows exist for current engine_version)

Resume semantics:
    Default mode (no --force) skips (project, date) pairs that already have a
    row for the current settings.engine_version. So if the script gets
    interrupted, re-running picks up where it left off without duplicating
    work. Use --force to rewrite everything (e.g. after a calibration change
    where you want the new engine SHA's output to overwrite, though even
    that's safe because engine_version is part of the cache key — the old
    SHA's rows stay in place for forensic comparison).

Environment:
    Same as the backend service — needs GOOGLE_APPLICATION_CREDENTIALS set
    so the BQ client + Funnel reads work.
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from dataclasses import dataclass
from datetime import date, timedelta

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("historical_backfill")

from backend.services import bigquery_client as bq  # noqa: E402
from backend.services import snapshots               # noqa: E402


@dataclass
class ProjectFlight:
    project_code: str
    status: str
    first_spend_date: date
    last_spend_date: date

    @property
    def flight_days(self) -> int:
        return (self.last_spend_date - self.first_spend_date).days + 1

    def date_range(self) -> list[date]:
        return [
            self.first_spend_date + timedelta(days=i)
            for i in range(self.flight_days)
        ]


def fetch_project_flights(project_codes: list[str] | None = None) -> list[ProjectFlight]:
    """Pull (project, status, first_spend_date, last_spend_date) for every
    project that has at least one row in fact_digital_daily."""
    where = ""
    params = []
    if project_codes:
        where = "AND p.project_code IN UNNEST(@codes)"
        params.append(bq.array_param("codes", "STRING", project_codes))

    sql = f"""
        SELECT
          p.project_code,
          p.status,
          MIN(f.date) AS first_spend_date,
          MAX(f.date) AS last_spend_date
        FROM {bq.table('dim_projects')} p
        JOIN {bq.table('fact_digital_daily')} f USING (project_code)
        WHERE TRUE
          {where}
        GROUP BY p.project_code, p.status
        ORDER BY p.project_code
    """
    rows = bq.run_query(sql, params)
    return [
        ProjectFlight(
            project_code=r["project_code"],
            status=r["status"] or "unknown",
            first_spend_date=r["first_spend_date"],
            last_spend_date=r["last_spend_date"],
        )
        for r in rows
        if r["first_spend_date"] is not None
    ]


def filter_targets(flights: list[ProjectFlight], include_active: bool) -> list[ProjectFlight]:
    """Backfill targets per build plan §6 #10 = completed projects.
    --include-active also picks up live projects so retro mode has a
    populated history for them on day 1 of team rollout."""
    return [
        f for f in flights
        if f.status == "completed" or (include_active and f.status == "active")
    ]


def run_single(
    flight: ProjectFlight,
    eval_date: date,
    bypass_cache: bool,
) -> tuple[bool, str | None]:
    """Returns (was_cached, error_message). Errors are logged but not raised
    so a single bad day doesn't kill the whole batch."""
    try:
        rows, cached = snapshots.find_or_compute(
            project_code=flight.project_code,
            as_of_date=eval_date,
            bypass_cache=bypass_cache,
        )
        return cached, None
    except Exception as e:
        logger.exception("Failed: %s @ %s", flight.project_code, eval_date)
        return False, str(e)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--include-active", action="store_true",
                        help="Also backfill active projects (default: completed only)")
    parser.add_argument("--projects", type=str, default=None,
                        help="Comma-separated list of project codes to restrict the backfill to")
    parser.add_argument("--force", action="store_true",
                        help="Bypass cache (recompute even if rows exist for current engine_version)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print the (project, eval_date) plan without firing the engine")
    parser.add_argument("--limit", type=int, default=None,
                        help="Stop after this many runs (useful for smoke testing)")
    args = parser.parse_args()

    project_codes = (
        [p.strip() for p in args.projects.split(",") if p.strip()]
        if args.projects else None
    )
    flights = fetch_project_flights(project_codes)
    targets = filter_targets(flights, include_active=args.include_active)

    if not targets:
        logger.warning("No projects to backfill. Pass --include-active or use --projects.")
        return 0

    total_runs = sum(f.flight_days for f in targets)
    logger.info(
        "Backfill scope: %d projects, %d total (project, eval_date) pairs.",
        len(targets), total_runs,
    )
    for f in targets:
        logger.info(
            "  %s [%s]  %s → %s  (%d days)",
            f.project_code, f.status,
            f.first_spend_date, f.last_spend_date, f.flight_days,
        )

    if args.dry_run:
        for f in targets:
            for d in f.date_range():
                print(f"{f.project_code}  {d.isoformat()}")
        return 0

    started = time.monotonic()
    completed = 0
    cached_hits = 0
    errors: list[tuple[str, date, str]] = []

    for f in targets:
        for d in f.date_range():
            was_cached, err = run_single(f, d, bypass_cache=args.force)
            completed += 1
            if was_cached:
                cached_hits += 1
            if err:
                errors.append((f.project_code, d, err))

            # Progress heartbeat every 50 runs
            if completed % 50 == 0:
                elapsed = time.monotonic() - started
                rate = completed / elapsed if elapsed > 0 else 0
                eta_s = (total_runs - completed) / rate if rate > 0 else 0
                logger.info(
                    "Progress: %d/%d (%.1f%%)  %d cached  %d errors  ETA %.0fs",
                    completed, total_runs, 100 * completed / total_runs,
                    cached_hits, len(errors), eta_s,
                )

            if args.limit is not None and completed >= args.limit:
                logger.info("Hit --limit %d, stopping early.", args.limit)
                break
        if args.limit is not None and completed >= args.limit:
            break

    elapsed = time.monotonic() - started
    logger.info(
        "Done. %d runs in %.0fs (%.1f runs/sec). %d cached, %d errors.",
        completed, elapsed,
        completed / elapsed if elapsed > 0 else 0,
        cached_hits, len(errors),
    )
    if errors:
        logger.warning("Errors:")
        for code, d, msg in errors[:25]:
            logger.warning("  %s @ %s: %s", code, d, msg)
        if len(errors) > 25:
            logger.warning("  ...and %d more", len(errors) - 25)

    return 0 if not errors else 1


if __name__ == "__main__":
    sys.exit(main())
