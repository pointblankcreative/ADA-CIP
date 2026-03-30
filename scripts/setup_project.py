"""End-to-end project onboarding: create in dim_projects, optionally sync a
media plan, run the transformation, and calculate pacing.

Usage:
    python -m scripts.setup_project --code 25013 --name "BCGEU Bargaining" \
        --client-id client-bcgeu --start 2025-05-01 --end 2026-03-31 \
        --budget 375000

    python -m scripts.setup_project --code 26009 --name "CUPE OMERS" \
        --client-id client-cupe --start 2026-03-05 --end 2026-03-24 \
        --budget 85000 --sheet-id 1eAxbCs8GBYQXYCREq_YFIpm98cZpLmW-vsINlo9u_5M
"""

import argparse
import json
import logging
import time
from datetime import datetime, timezone

from google.cloud import bigquery

from backend.config import settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
logger = logging.getLogger(__name__)


def _table(name: str) -> str:
    return f"{settings.gcp_project_id}.{settings.bigquery_dataset}.{name}"


def setup_project(
    code: str,
    name: str,
    client_id: str = "",
    start_date: str = "",
    end_date: str = "",
    budget: float = 0,
    sheet_id: str | None = None,
    run_transform: str = "daily",
    run_pacing: bool = True,
    slack_channel_id: str = "",
) -> dict:
    results: dict = {"project_code": code, "steps": {}}
    start = time.time()

    # ── Step 1: Upsert dim_projects ─────────────────────────────────
    logger.info("=== Step 1: Upsert project %s ===", code)
    mtl = bigquery.Client(project=settings.gcp_project_id, location=settings.gcp_region)
    try:
        mtl.query(
            f"DELETE FROM `{_table('dim_projects')}` WHERE project_code = @pc",
            job_config=bigquery.QueryJobConfig(
                query_parameters=[bigquery.ScalarQueryParameter("pc", "STRING", code)]
            ),
        ).result()

        record = {
            "project_code": code,
            "project_name": name,
            "client_id": client_id or None,
            "status": "active",
            "start_date": start_date or None,
            "end_date": end_date or None,
            "net_budget": budget or None,
            "slack_channel_id": slack_channel_id or None,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        cfg = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )
        mtl.load_table_from_json([record], _table("dim_projects"), job_config=cfg).result()
        results["steps"]["dim_projects"] = "created"
        logger.info("  Project %s upserted into dim_projects", code)
    except Exception as e:
        results["steps"]["dim_projects"] = f"error: {e}"
        logger.error("  Failed: %s", e)
    finally:
        mtl.close()

    # ── Step 2: Media plan sync (optional) ──────────────────────────
    if sheet_id:
        logger.info("=== Step 2: Sync media plan from sheet %s ===", sheet_id)
        try:
            from backend.services.media_plan_sync import sync_media_plan
            mp_result = sync_media_plan(sheet_id, code)
            results["steps"]["media_plan_sync"] = {
                "status": "success",
                "lines": mp_result.get("lines_created", 0),
                "weeks": mp_result.get("weeks_created", 0),
            }
        except Exception as e:
            results["steps"]["media_plan_sync"] = f"error: {e}"
            logger.error("  Media plan sync failed: %s", e)
    else:
        results["steps"]["media_plan_sync"] = "skipped (no sheet_id)"

    # ── Step 3: Transformation ──────────────────────────────────────
    if run_transform:
        logger.info("=== Step 3: Run transformation (mode=%s) ===", run_transform)
        try:
            from backend.services.transformation import run_transformation
            t_result = run_transformation(run_transform)
            results["steps"]["transformation"] = {
                "status": t_result.get("status", "unknown"),
                "rows_loaded": t_result.get("rows_loaded", 0),
            }
        except Exception as e:
            results["steps"]["transformation"] = f"error: {e}"
            logger.error("  Transformation failed: %s", e)
    else:
        results["steps"]["transformation"] = "skipped"

    # ── Step 4: Pacing ──────────────────────────────────────────────
    if run_pacing:
        logger.info("=== Step 4: Run pacing for %s ===", code)
        try:
            from backend.services.pacing import run_pacing_for_project
            p_result = run_pacing_for_project(code)
            results["steps"]["pacing"] = {
                "status": "success",
                "lines_processed": p_result.get("lines_processed", 0),
                "alerts": p_result.get("alerts", 0),
            }
        except Exception as e:
            results["steps"]["pacing"] = f"error: {e}"
            logger.error("  Pacing failed: %s", e)
    else:
        results["steps"]["pacing"] = "skipped"

    results["total_seconds"] = round(time.time() - start, 1)
    logger.info("=== Project setup complete in %.1fs ===", results["total_seconds"])
    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Set up a new CIP project")
    parser.add_argument("--code", required=True, help="Project code (YYNNN)")
    parser.add_argument("--name", required=True, help="Project name")
    parser.add_argument("--client-id", default="", help="Client ID")
    parser.add_argument("--start", default="", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", default="", help="End date (YYYY-MM-DD)")
    parser.add_argument("--budget", type=float, default=0, help="Net budget (CAD)")
    parser.add_argument("--sheet-id", default=None, help="Google Sheets media plan ID")
    parser.add_argument("--transform", default="daily", choices=["daily", "full", "skip"],
                        help="Transformation mode (default: daily)")
    parser.add_argument("--no-pacing", action="store_true", help="Skip pacing calculation")
    args = parser.parse_args()

    result = setup_project(
        code=args.code,
        name=args.name,
        client_id=args.client_id,
        start_date=args.start,
        end_date=args.end,
        budget=args.budget,
        sheet_id=args.sheet_id,
        run_transform=args.transform if args.transform != "skip" else "",
        run_pacing=not args.no_pacing,
    )
    print(json.dumps(result, indent=2))
