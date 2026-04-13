"""Daily job orchestrator — runs the full pipeline in sequence:

    1. Transformation: Funnel.io → fact_digital_daily (daily mode)
    2. Pacing: Compute even-pacing for all active projects
    3. Staleness check: Flag platforms with no data in 36+ hours

Designed to be called from:
    - POST /api/admin/daily-run  (manual trigger)
    - Cloud Scheduler HTTP target (automated daily)
    - CLI: python -m backend.services.daily_job
"""

import logging
import time
from datetime import date, datetime, timezone

from backend.services import bigquery_client as bq
from backend.services.pacing import run_all_active as run_all_pacing
from backend.services.transformation import run_transformation

logger = logging.getLogger(__name__)

DATA_STALE_HOURS = 36


def _check_data_staleness() -> list[dict]:
    """Check each platform for stale data (>36h since last record)."""
    sql = f"""
        SELECT
            platform_id,
            MAX(date) AS latest_date,
            MAX(loaded_at) AS latest_loaded_at
        FROM {bq.table('fact_digital_daily')}
        GROUP BY platform_id
    """
    try:
        rows = bq.run_query(sql)
    except Exception as e:
        logger.warning("Staleness check failed: %s", e)
        return []

    now = datetime.now(timezone.utc)
    stale = []
    for r in rows:
        loaded = r.get("latest_loaded_at")
        if loaded and hasattr(loaded, "timestamp"):
            age_hours = (now.timestamp() - loaded.timestamp()) / 3600
        else:
            latest_date = r.get("latest_date")
            if latest_date:
                age_hours = (date.today() - latest_date).days * 24
            else:
                age_hours = 999

        if age_hours > DATA_STALE_HOURS:
            stale.append({
                "platform_id": r["platform_id"],
                "latest_date": str(r.get("latest_date")),
                "hours_since_load": round(age_hours, 1),
            })
    return stale


def _write_stale_alerts(stale_platforms: list[dict]) -> int:
    """Write data_stale alerts to the alerts table."""
    if not stale_platforms:
        return 0

    from google.cloud import bigquery as bqmod
    from backend.config import settings

    now = datetime.now(timezone.utc).isoformat()
    today = date.today().isoformat()
    records = []

    for sp in stale_platforms:
        records.append({
            "alert_id": f"stale-{sp['platform_id']}-{today}",
            "project_code": "__system__",
            "alert_type": "data_stale",
            "severity": "warning",
            "title": f"Stale data: {sp['platform_id']}",
            "message": f"No data loaded for {sp['platform_id']} in {sp['hours_since_load']}h (threshold: {DATA_STALE_HOURS}h). Latest date: {sp['latest_date']}.",
            "metric_value": sp["hours_since_load"],
            "threshold_value": DATA_STALE_HOURS,
            "is_resolved": False,
            "created_at": now,
        })

    mtl = bqmod.Client(project=settings.gcp_project_id, location=settings.gcp_region)
    try:
        target = f"{settings.gcp_project_id}.{settings.bigquery_dataset}.alerts"
        cfg = bqmod.LoadJobConfig(
            source_format=bqmod.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bqmod.WriteDisposition.WRITE_APPEND,
        )
        mtl.load_table_from_json(records, target, job_config=cfg).result()
    finally:
        mtl.close()

    return len(records)


def _auto_complete_projects() -> dict:
    """Mark projects as completed when their booked end_date has passed."""
    from backend.config import settings
    dataset = f"{settings.gcp_project_id}.{settings.bigquery_dataset}"
    client = bq.get_client()
    results = {"expired": 0}

    expired_sql = f"""
        UPDATE `{dataset}.dim_projects`
        SET status = 'completed'
        WHERE end_date < CURRENT_DATE() AND status = 'active'
    """
    try:
        job = client.query(expired_sql)
        job.result()
        results["expired"] = job.num_dml_affected_rows or 0
        logger.info("  Auto-complete (expired): %d projects", results["expired"])
    except Exception:
        logger.warning("Auto-complete (expired) query failed", exc_info=True)

    return results


def run_daily_pipeline() -> dict:
    """Execute the full daily pipeline.

    Returns a summary dict with results from each stage.
    """
    start = time.time()
    pipeline_date = date.today().isoformat()
    results = {
        "pipeline_date": pipeline_date,
        "stages": {},
        "status": "success",
    }

    # ── Stage 0: Auto-complete expired/stale projects ────────────────
    logger.info("=== Daily Pipeline: Stage 0 — Auto-Complete Projects ===")
    try:
        t0 = time.time()
        ac_result = _auto_complete_projects()
        results["stages"]["auto_complete"] = {
            "status": "success",
            "expired": ac_result.get("expired", 0),
            "elapsed_seconds": round(time.time() - t0, 1),
        }
    except Exception as e:
        logger.error("Auto-complete failed: %s", e, exc_info=True)
        results["stages"]["auto_complete"] = {"status": "error", "error": str(e)}

    # ── Stage 1: Transformation ─────────────────────────────────────
    logger.info("=== Daily Pipeline: Stage 1 — Transformation ===")
    try:
        t1 = time.time()
        transform_result = run_transformation("daily")
        results["stages"]["transformation"] = {
            "status": transform_result.get("status", "unknown"),
            "rows_loaded": transform_result.get("rows_loaded", 0),
            "elapsed_seconds": round(time.time() - t1, 1),
        }
    except Exception as e:
        logger.error("Transformation failed: %s", e, exc_info=True)
        results["stages"]["transformation"] = {
            "status": "error",
            "error": str(e),
        }
        results["status"] = "partial_failure"

    # ── Stage 1b: GA4 Transformation ──────────────────────────────
    logger.info("=== Daily Pipeline: Stage 1b — GA4 Transformation ===")
    try:
        from ingestion.transformation.ga4_transform import run_ga4_transformation

        t1b = time.time()
        ga4_result = run_ga4_transformation("daily")
        results["stages"]["ga4_transformation"] = {
            "status": ga4_result.get("status", "unknown"),
            "rows_loaded": ga4_result.get("rows_loaded", 0),
            "elapsed_seconds": round(time.time() - t1b, 1),
        }
    except Exception as e:
        logger.error("GA4 Transformation failed: %s", e, exc_info=True)
        results["stages"]["ga4_transformation"] = {
            "status": "error",
            "error": str(e),
        }
        # GA4 transform is non-critical — don't mark pipeline as partial_failure

    # ── Stage 1c: Ad-set reach / frequency ─────────────────────────
    # E4: Dependency Documentation
    #
    # The ad-set transformation (run_adset_transformation) loads reach/frequency metrics
    # from Funnel.io into fact_adset_daily. These metrics are used by:
    #   - Dashboard reach/frequency displays (fact-based UI metrics)
    #   - Diagnostic signals (audience reach attainment calculations)
    #   - Performance reporting (reach vs. impressions ratios)
    #
    # If this stage fails (error status):
    #   - fact_adset_daily remains stale (no new rows loaded)
    #   - Reach metrics in UI will be out of date
    #   - Diagnostic signals may report lower-confidence reaches
    #   - Pacing does NOT depend on adset data — stage 2 proceeds independently
    #
    # This is NON-CRITICAL for pacing calculations, but CRITICAL for reach-aware
    # diagnostics. Mark at WARNING level if it fails so stakeholders can investigate
    # but the pipeline continues.
    #
    logger.info("=== Daily Pipeline: Stage 1c — Ad Set Reach/Frequency ===")
    try:
        from ingestion.transformation.adset_transform import run_adset_transformation

        t1c = time.time()
        adset_result = run_adset_transformation("daily")
        results["stages"]["adset_transformation"] = {
            "status": adset_result.get("status", "unknown"),
            "rows_loaded": adset_result.get("rows_loaded", 0),
            "elapsed_seconds": round(time.time() - t1c, 1),
        }
    except Exception as e:
        logger.warning("Ad-set Transformation failed (non-critical): %s — pacing continues, reach metrics may be stale", e, exc_info=True)
        results["stages"]["adset_transformation"] = {
            "status": "error",
            "error": str(e),
        }

    # ── Stage 2: Pacing ─────────────────────────────────────────────
    logger.info("=== Daily Pipeline: Stage 2 — Pacing ===")
    try:
        t2 = time.time()
        pacing_result = run_all_pacing()
        results["stages"]["pacing"] = {
            "status": "success",
            "projects_processed": pacing_result.get("projects_processed", 0),
            "total_alerts": pacing_result.get("total_alerts", 0),
            "elapsed_seconds": round(time.time() - t2, 1),
        }
    except Exception as e:
        logger.error("Pacing failed: %s", e, exc_info=True)
        results["stages"]["pacing"] = {
            "status": "error",
            "error": str(e),
        }
        results["status"] = "partial_failure"

    # ── Stage 3: Staleness Check ────────────────────────────────────
    logger.info("=== Daily Pipeline: Stage 3 — Staleness Check ===")
    try:
        t3 = time.time()
        stale = _check_data_staleness()
        stale_count = _write_stale_alerts(stale) if stale else 0
        results["stages"]["staleness"] = {
            "status": "success",
            "stale_platforms": len(stale),
            "alerts_created": stale_count,
            "elapsed_seconds": round(time.time() - t3, 1),
        }
        if stale:
            results["stages"]["staleness"]["details"] = stale
    except Exception as e:
        logger.error("Staleness check failed: %s", e, exc_info=True)
        results["stages"]["staleness"] = {
            "status": "error",
            "error": str(e),
        }

    # ── Stage 4: Slack Dispatch ─────────────────────────────────────
    logger.info("=== Daily Pipeline: Stage 4 — Slack Dispatch ===")
    try:
        from backend.services.slack_alerts import dispatch_unsent_alerts, post_daily_digest

        t4 = time.time()
        dispatch_result = dispatch_unsent_alerts()
        digest_result = post_daily_digest()
        results["stages"]["slack"] = {
            "status": "success",
            "dispatched": dispatch_result.get("dispatched", 0),
            "failed": dispatch_result.get("failed", 0),
            "digest_posted": digest_result.get("posted", False),
            "elapsed_seconds": round(time.time() - t4, 1),
        }
        if dispatch_result.get("skipped_no_token"):
            results["stages"]["slack"]["status"] = "skipped"
            results["stages"]["slack"]["reason"] = "SLACK_BOT_TOKEN not set"
    except Exception as e:
        logger.error("Slack dispatch failed: %s", e, exc_info=True)
        results["stages"]["slack"] = {
            "status": "error",
            "error": str(e),
        }

    results["total_elapsed_seconds"] = round(time.time() - start, 1)
    logger.info("=== Daily Pipeline Complete: %s in %.1fs ===",
                results["status"], results["total_elapsed_seconds"])

    return results


# CLI entrypoint
if __name__ == "__main__":
    import json
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    result = run_daily_pipeline()
    print(json.dumps(result, indent=2))
