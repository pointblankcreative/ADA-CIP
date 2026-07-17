"""ADA 26023 — add the missing Outdoor DOOH StackAdapt media-plan line.

Ships ATOMICALLY with the P-FRESH-PACE pacing-engine change. Campaign
`3285601` ("26023 - Outdoor - DOOH Awareness", StackAdapt, ~$5,991.52 spent
06-23 → 07-07) has NO row in `cip.media_plan_lines`, so its spend is
residual-split onto the other StackAdapt lines and distorts pacing. This
adds the missing line so that spend has a real home. The sibling
StackAdapt DOOH line already in the plan — line-004 "Bars - DOOH Awareness"
($3,500, campaign 3273636) — is copied field-for-field, overriding only the
line_id, audience, and budget.

WHY BOTH MUST SHIP TOGETHER (do not run this alone, and do not deploy the
engine change alone):
  - Engine-only (freshness "not reporting" for Meta, which stopped 06-24 but
    whose plan runs to 07-19) WITHOUT this line flips 26023 from a false
    "LAGGING" into a false "OVERSPENDING", because the orphaned $5,991.52 is
    still mis-attributed to the other StackAdapt lines.
  - This line WITHOUT the engine change gives the DOOH spend a home but does
    not fix the false Meta LAGGING read.

INVARIANT (verified before + must hold after): 26023 total FACT spend stays
$19,046.70. This migration only inserts a media-plan BUDGET line — it never
touches `fact_digital_daily` — so the spend total is unchanged by
construction; only the pacing % legitimately changes (to an honest read).

DECISION (2026-07-17, Frazer): the DURABLE path was chosen — add the Outdoor
DOOH line to the source media-plan SHEET (so a normal sync picks it up and it
survives future syncs). This script is therefore NOT to be executed; it is
retained as the exact reference for the row to add to the sheet (platform,
DOOH channel, flight window, budget, is_traditional/is_direct).

⚠️ DURABILITY — why the direct insert was NOT chosen. A direct
`media_plan_lines` insert is a STOPGAP: the next media-plan sync calls
`_clear_existing_plan()`, which DELETEs every line for the project before
re-inserting from the Google Sheet — so a directly-inserted row is wiped on
the next 26023 sync. Adding it to the sheet is durable.

⚠️ ATOMIC ROLLOUT / SEQUENCING. The freshness engine change in this branch and
this DOOH line must land together. Before the engine change is merged to
staging, the Outdoor DOOH line must already be in the 26023 media plan (add to
sheet + sync), otherwise 26023 shows a transient false OVERSPENDING (the
engine holds Meta out of the % while the orphaned DOOH spend still mis-splits
onto the other StackAdapt lines).

⚠️ JUDGMENT CALLS for Frazer to confirm before --execute:
  - `audience_name` below is a best-guess placeholder ("Outdoor DOOH …"). The
    real plan wording should match the media plan / client deck.
  - `budget` = 6000 per the ticket (actual spend to date 5991.52).
  - `flight_start`/`flight_end` = the PLAN window 06-11 → 07-19 (mirrors the
    other 26023 lines), NOT the campaign's actual 06-23 → 07-07 delivery.

Cross-region note: this uses the Python `load_table_from_json` client pattern
(WRITE_APPEND), the codebase-standard write into the Montreal `cip` dataset —
never a cross-region `INSERT … SELECT`.

Run:
    python 2026-07-17_26023_outdoor_dooh_line.py            # DRY RUN (default) — prints the row, writes nothing
    python 2026-07-17_26023_outdoor_dooh_line.py --execute  # appends the one row
"""

import sys

from google.cloud import bigquery

from backend.config import settings

# Field-for-field copy of line-004 (Bars DOOH, campaign 3273636) with the
# Outdoor overrides. Nullable columns omitted here load as NULL. line_id,
# plan_id, project_code are the table's only NOT NULL columns.
OUTDOOR_DOOH_LINE = {
    "line_id": "plan-26023-f0e55b94-line-009",
    "plan_id": "plan-26023-f0e55b94",
    "project_code": "26023",
    "line_code": "",
    "platform_id": "stackadapt",
    "channel_category": "Digital",
    "site_network": "Digital Out Of Home",
    "flight_start": "2026-06-11",
    "flight_end": "2026-07-19",
    "objective": "Awareness",
    # ⚠️ placeholder — confirm the real plan wording with Frazer.
    "audience_name": "Outdoor DOOH screens — Vancouver (FIFA)",
    "budget": 6000,
    "pricing_model": "CPM",
    "is_traditional": True,   # keyword media-type label (kept informational)
    "is_direct": False,       # has a self-serve StackAdapt feed → it PACES
    # Matches the current plan snapshot so it reads as part of plan-26023-f0e55b94.
    "sync_version": "2026-07-16T21:28:11.816758+00:00",
}

TARGET = "media_plan_lines"


def main(execute: bool) -> None:
    target = f"{settings.gcp_project_id}.{settings.bigquery_dataset}.{TARGET}"
    print(f"Target: {target}")
    print("Row to append:")
    for k, v in OUTDOOR_DOOH_LINE.items():
        print(f"  {k}: {v!r}")

    if not execute:
        print("\nDRY RUN — nothing written. Re-run with --execute to append.")
        return

    client = bigquery.Client(
        project=settings.gcp_project_id, location=settings.gcp_region
    )
    try:
        cfg = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )
        client.load_table_from_json([OUTDOOR_DOOH_LINE], target, job_config=cfg).result()
        print("\nAppended 1 row to media_plan_lines.")
    finally:
        client.close()


if __name__ == "__main__":
    main("--execute" in sys.argv[1:])
