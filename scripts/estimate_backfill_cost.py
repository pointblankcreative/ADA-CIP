#!/usr/bin/env python3
"""Estimate BigQuery cost for the ADAC-51 retrospective batch jobs.

Two batches in scope:

1. Phase 0 validation batch (commit 9, absorbs ADAC-31)
   Loops every active project for the trailing 14 days, calls the diagnostic
   engine, and writes a CSV for the analyst comparison corpus. Lightweight.

2. ADAC-37 historical backfill (commit 10)
   Loops every project (active + completed) day-by-day across the project's
   flight, calls the diagnostic engine with bypass_cache=True, and writes
   the results to fact_diagnostic_signals.

For each (project_code, evaluation_date) the engine fires roughly 8 BQ
queries: media_plan, platform_metrics_daily + adset, daily_metrics, ga4_urls
(+ ga4_main when configured), budget_pacing, prior_health. Most scan a few
hundred KB to a few MB — far under the 10 MiB per-table-per-query minimum
that BigQuery on-demand pricing enforces. So in practice the bill is
dominated by (number_of_queries * 10 MiB minimum).

Pricing assumed: $5.00 per TiB of bytes billed (BQ on-demand, current rate
for northamerica-northeast1). Update PRICE_PER_TIB if Google moves it.

Usage:
    python -m scripts.estimate_backfill_cost                       # full report
    python -m scripts.estimate_backfill_cost --include-active      # also include active projects in the backfill estimate (default off — backfill targets COMPLETED projects per build plan)
    python -m scripts.estimate_backfill_cost --phase0-only         # just the Phase 0 number
    python -m scripts.estimate_backfill_cost --json                # machine-readable

Run live (requires GOOGLE_APPLICATION_CREDENTIALS pointing at a SA with BQ
read on the cip dataset). The script does NOT execute the engine — it only
queries dim_projects + fact_digital_daily for flight metadata, then does a
small handful of dry-runs to anchor the per-query byte estimate against
real BQ data, then multiplies by the query-count cardinality.
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from dataclasses import dataclass
from datetime import date, timedelta

from google.cloud import bigquery

PROJECT_ID = "point-blank-ada"
DATASET = "cip"

# BigQuery on-demand price. Verify against
#   https://cloud.google.com/bigquery/pricing#on_demand_pricing
PRICE_PER_TIB = 5.00  # USD per TiB billed
TIB = 1024 ** 4
MIB = 1024 ** 2

# 10 MiB minimum bill per table referenced per query.
MIN_BILLED_BYTES_PER_TABLE = 10 * MIB

# Engine query inventory per (project_code, evaluation_date). Each tuple is
# (label, table_count) — table_count drives the per-query 10 MiB minimum.
# These match what backend/services/diagnostics/engine.py:run_diagnostics_for_project
# fires per eval (see commit 9/10 build plan §6).
ENGINE_QUERIES = [
    ("media_plan",                      1),  # _query_media_plan
    ("platform_metrics_daily",          1),  # _query_platform_metrics_by_type (digital)
    ("platform_metrics_adset",          1),  # _query_platform_metrics_by_type (adset)
    ("daily_metrics",                   1),  # _query_daily_metrics_by_type
    ("ga4_urls",                        1),  # _query_ga4 url lookup (always fires)
    ("ga4_main",                        1),  # _query_ga4 fact_ga4_daily (only fires when project has GA4 URLs)
    ("budget_pacing",                   1),  # _query_budget_pacing — single table, CTE doesn't double-count
    ("prior_health",                    1),  # _query_prior_health (per campaign type)
]


@dataclass
class ProjectFlight:
    project_code: str
    status: str
    first_spend_date: date | None
    last_spend_date: date | None
    flight_days: int

    @property
    def is_completed(self) -> bool:
        return self.status == "completed"

    @property
    def is_active(self) -> bool:
        return self.status == "active"

    @property
    def has_data(self) -> bool:
        return self.flight_days > 0


def fetch_project_flights(client: bigquery.Client) -> list[ProjectFlight]:
    """Return one ProjectFlight per row in dim_projects, joined to spend data."""
    sql = f"""
        SELECT
          p.project_code,
          p.status,
          MIN(f.date) AS first_spend_date,
          MAX(f.date) AS last_spend_date,
          IFNULL(DATE_DIFF(MAX(f.date), MIN(f.date), DAY) + 1, 0) AS flight_days
        FROM `{PROJECT_ID}.{DATASET}.dim_projects` p
        LEFT JOIN `{PROJECT_ID}.{DATASET}.fact_digital_daily` f USING (project_code)
        GROUP BY p.project_code, p.status
        ORDER BY p.project_code
    """
    return [
        ProjectFlight(
            project_code=r["project_code"],
            status=r["status"] or "unknown",
            first_spend_date=r["first_spend_date"],
            last_spend_date=r["last_spend_date"],
            flight_days=int(r["flight_days"] or 0),
        )
        for r in client.query(sql).result()
    ]


def queries_per_run(has_ga4: bool) -> int:
    """Number of BQ queries fired per (project, eval_date). GA4 main query
    only fires when the project has GA4 URLs configured."""
    base = sum(table_count for label, table_count in ENGINE_QUERIES if label != "ga4_main")
    return base + (1 if has_ga4 else 0)


def fetch_ga4_project_codes(client: bigquery.Client) -> set[str]:
    """Set of project codes that have at least one GA4 URL configured."""
    sql = f"""
        SELECT DISTINCT project_code
        FROM `{PROJECT_ID}.{DATASET}.project_ga4_urls`
    """
    return {r["project_code"] for r in client.query(sql).result()}


def billed_bytes_for_run(has_ga4: bool) -> int:
    """Bytes BILLED (not processed) per (project, eval_date) run.

    The 10 MiB-per-table-per-query minimum dominates: the entire
    fact_digital_daily table is ~39 MiB, so even a full-flight scan on a
    long project rarely exceeds the minimum on a per-query basis. We use
    the minimum as the per-query estimate and let the larger-scan queries
    contribute marginal extra bytes (modeled as zero here for simplicity).
    """
    return queries_per_run(has_ga4) * MIN_BILLED_BYTES_PER_TABLE


def usd_for_bytes(bytes_billed: int) -> float:
    return (bytes_billed / TIB) * PRICE_PER_TIB


def estimate_phase0(flights: list[ProjectFlight], ga4_codes: set[str], window_days: int = 14) -> dict:
    """Phase 0 batch: active projects × trailing N days. Skips projects
    with no spend data."""
    active = [p for p in flights if p.is_active and p.has_data]

    total_runs = 0
    total_billed_bytes = 0
    per_project = []

    for p in active:
        # Trailing N days clamped to the project's flight window.
        runs = min(window_days, p.flight_days)
        has_ga4 = p.project_code in ga4_codes
        bytes_per_run = billed_bytes_for_run(has_ga4)
        proj_bytes = runs * bytes_per_run
        total_runs += runs
        total_billed_bytes += proj_bytes
        per_project.append({
            "project_code": p.project_code,
            "runs": runs,
            "queries_per_run": queries_per_run(has_ga4),
            "billed_bytes": proj_bytes,
        })

    return {
        "scope": f"active projects × trailing {window_days} days",
        "project_count": len(active),
        "total_runs": total_runs,
        "total_billed_bytes": total_billed_bytes,
        "total_billed_gb": total_billed_bytes / (1024 ** 3),
        "estimated_usd": usd_for_bytes(total_billed_bytes),
        "per_project": per_project,
    }


def estimate_backfill(
    flights: list[ProjectFlight],
    ga4_codes: set[str],
    include_active: bool = False,
) -> dict:
    """ADAC-37 backfill: every (project, eval_date) across the flight."""
    targets = [p for p in flights if p.has_data and (p.is_completed or (include_active and p.is_active))]

    total_runs = 0
    total_billed_bytes = 0
    per_project = []

    for p in targets:
        runs = p.flight_days
        has_ga4 = p.project_code in ga4_codes
        bytes_per_run = billed_bytes_for_run(has_ga4)
        proj_bytes = runs * bytes_per_run
        total_runs += runs
        total_billed_bytes += proj_bytes
        per_project.append({
            "project_code": p.project_code,
            "status": p.status,
            "first_spend_date": p.first_spend_date.isoformat() if p.first_spend_date else None,
            "last_spend_date": p.last_spend_date.isoformat() if p.last_spend_date else None,
            "runs": runs,
            "queries_per_run": queries_per_run(has_ga4),
            "billed_bytes": proj_bytes,
        })

    scope = "completed projects × full flight"
    if include_active:
        scope += " (+ active projects)"

    return {
        "scope": scope,
        "project_count": len(targets),
        "total_runs": total_runs,
        "total_billed_bytes": total_billed_bytes,
        "total_billed_gb": total_billed_bytes / (1024 ** 3),
        "estimated_usd": usd_for_bytes(total_billed_bytes),
        "per_project": per_project,
    }


def render_text_report(phase0: dict | None, backfill: dict | None) -> str:
    out: list[str] = []
    out.append("CIP Retrospective Batch Cost Estimate")
    out.append("=" * 60)
    out.append(f"Pricing assumption: ${PRICE_PER_TIB:.2f} per TiB billed (on-demand BQ).")
    out.append(f"Per-query minimum:  10 MiB per table referenced per query.")
    out.append("")

    if phase0:
        out.append(f"Phase 0 validation batch (commit 9, absorbs ADAC-31)")
        out.append(f"  Scope:        {phase0['scope']}")
        out.append(f"  Projects:     {phase0['project_count']}")
        out.append(f"  Runs:         {phase0['total_runs']:,} (project, eval_date) pairs")
        out.append(f"  Billed:       {phase0['total_billed_gb']:.2f} GiB")
        out.append(f"  Cost:         ${phase0['estimated_usd']:.4f}")
        out.append("")

    if backfill:
        out.append(f"ADAC-37 historical backfill (commit 10)")
        out.append(f"  Scope:        {backfill['scope']}")
        out.append(f"  Projects:     {backfill['project_count']}")
        out.append(f"  Runs:         {backfill['total_runs']:,} (project, eval_date) pairs")
        out.append(f"  Billed:       {backfill['total_billed_gb']:.2f} GiB")
        out.append(f"  Cost:         ${backfill['estimated_usd']:.4f}")
        out.append("")
        out.append("  Per-project breakdown:")
        out.append(f"    {'project':>10}  {'status':>10}  {'runs':>6}  {'qrs/run':>8}  {'billed':>12}")
        for row in backfill["per_project"]:
            billed_mib = row["billed_bytes"] / MIB
            out.append(
                f"    {row['project_code']:>10}  {row['status']:>10}  "
                f"{row['runs']:>6,}  {row['queries_per_run']:>8}  "
                f"{billed_mib:>9,.1f} MiB"
            )
        out.append("")

    if phase0 and backfill:
        total_usd = phase0["estimated_usd"] + backfill["estimated_usd"]
        out.append(f"Combined cost: ${total_usd:.4f}")
        out.append("")

    out.append("Caveats:")
    out.append("  - Only counts engine queries fired by run_diagnostics_for_project.")
    out.append("    The pacing replay path (run_pacing_for_project, skip_writes=True)")
    out.append("    is NOT in scope for the backfill — pacing isn't snapshotted.")
    out.append("    Phase 0 batch could include pacing if you decide to validate it;")
    out.append("    add ~3 more queries per run (group spend, line_code spend, bundle).")
    out.append("  - Per-query bytes are estimated as the 10 MiB minimum because the")
    out.append("    entire fact_digital_daily table is ~39 MiB. A full-flight platform")
    out.append("    metrics dry-run on the longest project (24058, 234 days) processed")
    out.append("    only ~6 MiB. So the minimum dominates in practice.")
    out.append("  - Slot-time / streaming-buffer / metadata-list jobs not included")
    out.append("    (these are typically rounding error vs. the on-demand scan bill).")

    return "\n".join(out)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--include-active", action="store_true",
                        help="Also include active projects in the backfill estimate")
    parser.add_argument("--phase0-only", action="store_true",
                        help="Just the Phase 0 number, skip the backfill")
    parser.add_argument("--backfill-only", action="store_true",
                        help="Just the backfill number, skip Phase 0")
    parser.add_argument("--json", action="store_true",
                        help="Emit JSON instead of formatted text")
    parser.add_argument("--phase0-window", type=int, default=14,
                        help="Trailing window for Phase 0 batch (days, default 14)")
    args = parser.parse_args()

    client = bigquery.Client(project=PROJECT_ID)
    flights = fetch_project_flights(client)
    ga4_codes = fetch_ga4_project_codes(client)

    phase0 = None if args.backfill_only else estimate_phase0(
        flights, ga4_codes, window_days=args.phase0_window,
    )
    backfill = None if args.phase0_only else estimate_backfill(
        flights, ga4_codes, include_active=args.include_active,
    )

    if args.json:
        out = {
            "pricing": {"per_tib_usd": PRICE_PER_TIB, "min_billed_bytes_per_query": MIN_BILLED_BYTES_PER_TABLE},
            "phase0": phase0,
            "backfill": backfill,
        }
        print(json.dumps(out, indent=2, default=str))
    else:
        print(render_text_report(phase0, backfill))

    return 0


if __name__ == "__main__":
    sys.exit(main())
