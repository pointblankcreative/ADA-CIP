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
import uuid
from datetime import date, datetime, timezone

from backend.services import bigquery_client as bq
from backend.services.pacing import run_all_active as run_all_pacing
from backend.services.transformation import run_transformation

logger = logging.getLogger(__name__)

DATA_STALE_HOURS = 36


def _check_data_staleness() -> list[dict]:
    """Stale platforms as of today, via the shared freshness primitive.

    Returns one dict per stale platform in the shape ``_write_stale_alerts``
    expects: ``platform_id``, ``latest_date`` (str), ``hours_since_load``.
    Detection now lives in ``data_freshness.compute_platform_freshness`` (global
    call — no flight-end guard) so this sweep, the admin panel, and pacing's
    not-reporting logic all agree. Fails open (empty) so a freshness read error
    never takes the pipeline down.
    """
    from backend.services.data_freshness import compute_platform_freshness

    try:
        platforms = compute_platform_freshness(date.today())
    except Exception as e:
        logger.warning("Staleness check failed: %s", e)
        return []

    stale = []
    for p in platforms:
        if not p.get("is_stale"):
            continue
        ldd = p.get("latest_data_date")
        stale.append({
            "platform_id": p["platform_id"],
            "latest_date": str(ldd) if ldd else None,
            "hours_since_load": p.get("age_hours"),
        })
    return stale


def _projects_on_platform(platform_id: str) -> list[str]:
    """Active projects that carry a non-direct (self-serve) media plan line on
    ``platform_id`` — the projects a platform outage actually degrades.

    Mirrors the pacing inclusion rule (``COALESCE(is_direct_override, is_direct)
    = FALSE``) and guards stale plan_ids / retired phases via the current-
    media_plans × active-project_media_plans JOIN. Aggregation is dedup-safe on
    its own, so this deliberately avoids the per-line ROW_NUMBER dedup CTE.
    """
    sql = f"""
        SELECT DISTINCT p.project_code
        FROM {bq.table('dim_projects')} p
        JOIN {bq.table('media_plans')} mp
          ON p.project_code = mp.project_code AND mp.is_current = TRUE
        JOIN {bq.table('project_media_plans')} pmp
          ON mp.project_code = pmp.project_code
         AND mp.sheet_id     = pmp.sheet_id
         AND pmp.is_active    = TRUE
        JOIN {bq.table('media_plan_lines')} l
          ON l.plan_id = mp.plan_id
        WHERE p.status IN ('active', 'in_flight')
          AND l.platform_id = @platform_id
          AND COALESCE(l.is_direct_override, l.is_direct) = FALSE
    """
    try:
        rows = bq.run_query(sql, [bq.string_param("platform_id", platform_id)])
    except Exception as e:
        logger.warning(
            "Project-scoping query for stale platform %s failed: %s",
            platform_id, e,
        )
        return []
    return [r["project_code"] for r in rows if r.get("project_code")]


def _write_stale_alerts(stale_platforms: list[dict]) -> int:
    """Write ``data_stale`` alerts in the metadata-JSON alert shape the rest of
    the system uses (``pacing._generate_alerts``), via ``pacing._write_alerts``.

    The prior version wrote ``metric_value`` / ``threshold_value`` /
    ``is_resolved`` columns that do NOT exist in ``cip.alerts`` — the load was
    silently rejected, which is why zero stale alerts ever landed. Here each
    stale platform emits:

      * a global ``__system__`` outage alert (the platform-down signal), and
      * one ``warning`` alert per active project carrying a non-direct line on
        that platform (so an outage surfaces on the affected campaign's feed).

    ``pacing._write_alerts`` dedups on (project_code, alert_type, severity) over
    24h, so repeat daily runs don't spam.
    """
    if not stale_platforms:
        return 0

    import json

    from backend.services.pacing import _write_alerts

    now = datetime.now(timezone.utc).isoformat()
    records = []

    def _alert(project_code: str, severity: str, title: str, message: str, meta: dict):
        records.append({
            "alert_id": str(uuid.uuid4()),
            "project_code": project_code,
            "alert_type": "data_stale",
            "severity": severity,
            "title": title,
            "message": message,
            "metadata": json.dumps(meta),
            "created_at": now,
            "slack_sent": False,
        })

    for sp in stale_platforms:
        pid = sp["platform_id"]
        hours = sp.get("hours_since_load")
        latest = sp.get("latest_date")
        hours_txt = f"{hours:.1f}h" if isinstance(hours, (int, float)) else "an unknown time"
        meta = {
            "platform_id": pid,
            "hours_since_load": hours,
            "threshold": DATA_STALE_HOURS,
            "latest_date": latest,
        }

        # Global platform-down signal, independent of any project.
        _alert(
            "__system__",
            "warning",
            f"Stale data: {pid}",
            f"No data loaded for {pid} in {hours_txt} "
            f"(threshold {DATA_STALE_HOURS}h). Latest date: {latest}.",
            meta,
        )

        # Per-project × per-platform: scope the outage to affected campaigns.
        for project_code in _projects_on_platform(pid):
            _alert(
                project_code,
                "warning",
                f"{pid} not reporting",
                f"{pid} has not reported new data in {hours_txt} "
                f"(threshold {DATA_STALE_HOURS}h). Latest date: {latest}. "
                f"This platform's lines are held out of the pacing % until it "
                f"resumes reporting.",
                meta,
            )

    _write_alerts(records)
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

    # ── Stage 1d: Creative assets + audience targeting (Phase 19) ──
    # Pulls ad stills from Meta/StackAdapt into GCS (creative-assets/) and
    # renders ad-set targeting specs into plain-English personas. Runs
    # after 1c so fresh ad-set names are matchable. Best-effort by design:
    # run_sync never raises, and empty tokens make it a no-op, so this
    # stage can't take the pipeline down.
    logger.info("=== Daily Pipeline: Stage 1d — Creative Assets Sync ===")
    try:
        from backend.services.creative_assets import run_sync as run_creative_assets_sync

        t1d = time.time()
        ca_result = run_creative_assets_sync()
        results["stages"]["creative_assets"] = {
            "status": "success",
            "images": ca_result.get("images", {}),
            "targeting": ca_result.get("targeting", {}),
            "elapsed_seconds": round(time.time() - t1d, 1),
        }
    except Exception as e:
        logger.warning("Creative assets sync failed (non-critical): %s", e, exc_info=True)
        results["stages"]["creative_assets"] = {
            "status": "error",
            "error": str(e),
        }

    # ── Stage 1e: StackAdapt Reach/Frequency (Asana 1215990005858637) ──
    # Pulls dedup reach/frequency from StackAdapt's own reachFrequency API into
    # cip_stackadapt.stackadapt_reach_frequency (Funnel's SA reach/freq is a
    # 1-day per-creative field that overcounts 7-10x). Best-effort by design:
    # run_sync never raises and no-ops when STACKADAPT_API_KEY is unset, so this
    # stage can't take the pipeline down.
    logger.info("=== Daily Pipeline: Stage 1e — StackAdapt Reach/Frequency ===")
    try:
        from backend.services.stackadapt_rf_sync import run_sync as run_stackadapt_rf_sync

        t1e = time.time()
        rf_result = run_stackadapt_rf_sync()
        results["stages"]["stackadapt_rf"] = {
            "status": rf_result.get("status", "unknown"),
            "campaigns": rf_result.get("campaigns", 0),
            "rows_upserted": rf_result.get("rows_upserted", 0),
            "grains": rf_result.get("grains", {}),
            "elapsed_seconds": round(time.time() - t1e, 1),
        }
    except Exception as e:
        logger.warning("StackAdapt R&F sync failed (non-critical): %s", e, exc_info=True)
        results["stages"]["stackadapt_rf"] = {
            "status": "error",
            "error": str(e),
        }

    # ── Stage 2: Pacing ─────────────────────────────────────────────
    logger.info("=== Daily Pipeline: Stage 2 — Pacing ===")
    try:
        t2 = time.time()
        # ``as_of_date`` became required in ADAC-51 commit 3. The daily
        # pipeline's semantic is 'pace today', so pass ``date.today()``
        # explicitly.
        pacing_result = run_all_pacing(date.today())
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

    # ── Stage 2b: Diagnostics ───────────────────────────────────────
    # Runs after pacing so budget_tracking is fresh (used by efficiency layer).
    #
    # Current scope:
    #   - Persuasion: Distribution (D1-D4), Attention (A1-A5), Resonance (R1-R3)
    #   - Conversion: Acquisition (C1-C3), Funnel (F1-F5)
    #     (Quality (Q1-Q3) deferred — see docs/diagnostics/quality-pillar-deferred.md)
    #   - Mixed-campaign aware: partitions media plan lines by type and runs
    #     persuasion + conversion independently on projects that carry both.
    #   - Alerts: signal-level ACTION + health-regression (ACTION transition),
    #     with 24h dedup — see docs/diagnostics/alert-rules.md.
    #
    # Failures are non-critical — the rest of the pipeline (staleness, Slack)
    # still runs.
    logger.info("=== Daily Pipeline: Stage 2b — Diagnostics ===")
    try:
        from backend.services.diagnostics.engine import run_all_diagnostics

        t2b = time.time()
        # ``evaluation_date`` became required in ADAC-51 commit 2. The daily
        # pipeline's semantic is "score today", so pass ``date.today()``
        # explicitly rather than relying on an implicit default.
        diag_result = run_all_diagnostics(date.today())
        results["stages"]["diagnostics"] = {
            "status": "success",
            "projects_processed": diag_result.get("projects_processed", 0),
            "projects_skipped": diag_result.get("projects_skipped", 0),
            "total_alerts": diag_result.get("total_alerts", 0),
            "errors": diag_result.get("errors", []),
            "elapsed_seconds": round(time.time() - t2b, 1),
        }
    except Exception as e:
        logger.error("Diagnostics failed (non-critical): %s", e, exc_info=True)
        results["stages"]["diagnostics"] = {
            "status": "error",
            "error": str(e),
        }

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
