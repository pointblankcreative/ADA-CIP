"""Diagnostic engine orchestrator.

Classify → Query → Compute → Store → Alert.

For each active project:
    1. Determine campaign type (persuasion / conversion) from media plan
    2. Query 5 BQ tables to assemble CampaignData
    3. Route to the right health computation
    4. Store results in fact_diagnostic_signals
    5. Fire critical alerts through existing Slack pipeline

Designed to be called from daily_job.py after pacing stage, or
manually via /api/diagnostics/{code}/run.
"""

from __future__ import annotations

import json
import logging
import uuid
from datetime import date, datetime, timezone
from typing import Any

from google.cloud import bigquery as bqmod

from backend.config import settings
from backend.services import bigquery_client as bq
from backend.services.diagnostics.models import (
    AudienceType,
    CampaignData,
    CampaignType,
    DailyMetrics,
    DiagnosticAlert,
    DiagnosticOutput,
    FlightContext,
    GA4Metrics,
    MediaPlanLine,
    PlatformMetrics,
)
from backend.services.diagnostics.persuasion.health import compute_persuasion_health

logger = logging.getLogger(__name__)


# ── Public API ──────────────────────────────────────────────────────


def run_diagnostics_for_project(
    project_code: str,
    evaluation_date: date | None = None,
) -> list[DiagnosticOutput]:
    """Run diagnostic evaluation for a single project.

    Returns one DiagnosticOutput per campaign type found.
    """
    eval_date = evaluation_date or date.today()
    results: list[DiagnosticOutput] = []

    # Step 1: Get project metadata + campaign type
    campaign_type = _classify_campaign(project_code)
    if campaign_type is None:
        logger.warning("Could not classify campaign type for %s", project_code)
        return results

    # Step 2: Get flight dates from media plan
    media_plan = _query_media_plan(project_code)
    if not media_plan:
        logger.warning("No media plan lines for %s", project_code)
        return results

    flight = _derive_flight(media_plan, eval_date)
    if flight is None:
        logger.warning("Could not derive flight dates for %s", project_code)
        return results

    # Step 3: Query all data sources
    platform_metrics = _query_platform_metrics(
        project_code, flight.flight_start, eval_date
    )
    daily_metrics = _query_daily_metrics(
        project_code, flight.flight_start, eval_date
    )
    ga4 = _query_ga4(project_code, flight.flight_start, eval_date)
    pacing_pct = _query_budget_pacing(project_code, eval_date)

    # Step 4: Assemble CampaignData
    data = CampaignData(
        project_code=project_code,
        campaign_type=campaign_type,
        flight=flight,
        platform_metrics=platform_metrics,
        daily_metrics=daily_metrics,
        media_plan=media_plan,
        ga4=ga4,
        budget_pacing_pct=pacing_pct,
    )

    # Step 5: Compute diagnostics based on campaign type
    if campaign_type == CampaignType.PERSUASION:
        output = compute_persuasion_health(data)
    else:
        # Conversion health — Phase 2
        logger.info("Conversion diagnostics not yet implemented for %s", project_code)
        return results

    results.append(output)

    # Step 6: Store results
    _store_results(results)

    # Step 7: Fire alerts
    _fire_alerts(output)

    logger.info(
        "Diagnostics complete for %s: health=%s status=%s",
        project_code,
        output.health_score,
        output.health_status,
    )

    return results


def run_all_diagnostics(evaluation_date: date | None = None) -> dict:
    """Run diagnostics for all active projects.

    Called from daily_job.py.
    Returns a summary dict.
    """
    eval_date = evaluation_date or date.today()
    projects = _get_active_projects()
    summary = {
        "projects_processed": 0,
        "projects_skipped": 0,
        "total_alerts": 0,
        "errors": [],
    }

    for project_code in projects:
        try:
            outputs = run_diagnostics_for_project(project_code, eval_date)
            if outputs:
                summary["projects_processed"] += 1
                summary["total_alerts"] += sum(
                    len(o.alerts) for o in outputs
                )
            else:
                summary["projects_skipped"] += 1
        except Exception as e:
            logger.error("Diagnostics failed for %s: %s", project_code, e, exc_info=True)
            summary["errors"].append({"project": project_code, "error": str(e)})

    return summary


# ── Data queries ────────────────────────────────────────────────────


def _get_active_projects() -> list[str]:
    """Get all active project codes."""
    sql = f"""
        SELECT project_code
        FROM {bq.table('dim_projects')}
        WHERE status = 'active'
        ORDER BY project_code
    """
    rows = bq.run_query(sql)
    return [r["project_code"] for r in rows]


def _classify_campaign(project_code: str) -> CampaignType | None:
    """Determine campaign type from the objective classifier or media plan.

    Uses the objective_classifier service logic: if the dominant
    campaign_objective is awareness/reach-oriented → persuasion,
    if conversion-oriented → conversion.
    """
    sql = f"""
        SELECT campaign_objective, SUM(impressions) as total_impressions
        FROM {bq.table('fact_digital_daily')}
        WHERE project_code = @project_code
          AND campaign_objective IS NOT NULL
        GROUP BY campaign_objective
        ORDER BY total_impressions DESC
        LIMIT 1
    """
    rows = bq.run_query(sql, [bq.string_param("project_code", project_code)])

    if not rows:
        # Fallback: check media plan for objective hints
        return _classify_from_media_plan(project_code)

    objective = (rows[0].get("campaign_objective") or "").upper()

    # Meta objectives that map to persuasion
    persuasion_objectives = {
        "OUTCOME_AWARENESS", "BRAND_AWARENESS", "REACH", "VIDEO_VIEWS",
        "POST_ENGAGEMENT", "OUTCOME_ENGAGEMENT", "OUTCOME_TRAFFIC",
    }
    # Conversion objectives
    conversion_objectives = {
        "CONVERSIONS", "OUTCOME_SALES", "OUTCOME_LEADS",
        "LEAD_GENERATION", "WEBSITE_CONVERSIONS", "APP_INSTALLS",
    }

    if objective in persuasion_objectives:
        return CampaignType.PERSUASION
    if objective in conversion_objectives:
        return CampaignType.CONVERSION

    # Default to persuasion for ambiguous objectives
    return CampaignType.PERSUASION


def _classify_from_media_plan(project_code: str) -> CampaignType | None:
    """Fallback classification from media plan line objectives."""
    sql = f"""
        SELECT objective
        FROM {bq.table('media_plan_lines')}
        WHERE project_code = @project_code
        LIMIT 5
    """
    rows = bq.run_query(sql, [bq.string_param("project_code", project_code)])
    if not rows:
        return None

    objectives = [r.get("objective", "").lower() for r in rows]
    if any("conversion" in o or "lead" in o for o in objectives):
        return CampaignType.CONVERSION
    return CampaignType.PERSUASION


def _query_media_plan(project_code: str) -> list[MediaPlanLine]:
    """Query media plan lines for a project."""
    sql = f"""
        SELECT
            line_id,
            platform_id,
            channel_category,
            audience_name,
            audience_type,
            planned_budget,
            COALESCE(planned_impressions, 0) as planned_impressions,
            COALESCE(planned_reach, 0) as planned_reach,
            COALESCE(frequency_cap, 0) as frequency_cap,
            flight_start,
            flight_end,
            ffs_score,
            objective
        FROM {bq.table('media_plan_lines')}
        WHERE project_code = @project_code
    """
    rows = bq.run_query(sql, [bq.string_param("project_code", project_code)])

    lines = []
    for r in rows:
        audience = r.get("audience_type")
        try:
            audience_type = AudienceType(audience) if audience else None
        except ValueError:
            audience_type = None

        lines.append(MediaPlanLine(
            line_id=str(r["line_id"]),
            platform_id=r.get("platform_id"),
            channel_category=r.get("channel_category"),
            audience_name=r.get("audience_name"),
            audience_type=audience_type,
            planned_budget=float(r.get("planned_budget") or 0),
            planned_impressions=int(r.get("planned_impressions") or 0),
            planned_reach=int(r.get("planned_reach") or 0),
            frequency_cap=float(r.get("frequency_cap") or 0),
            flight_start=r.get("flight_start"),
            flight_end=r.get("flight_end"),
            ffs_score=float(r["ffs_score"]) if r.get("ffs_score") else None,
            objective=r.get("objective"),
        ))

    return lines


def _derive_flight(
    media_plan: list[MediaPlanLine], eval_date: date
) -> FlightContext | None:
    """Derive flight start/end from media plan lines."""
    starts = [l.flight_start for l in media_plan if l.flight_start]
    ends = [l.flight_end for l in media_plan if l.flight_end]

    if not starts or not ends:
        return None

    flight_start = min(starts)
    flight_end = max(ends)

    # Convert strings to dates if needed
    if isinstance(flight_start, str):
        flight_start = date.fromisoformat(flight_start)
    if isinstance(flight_end, str):
        flight_end = date.fromisoformat(flight_end)

    return FlightContext(
        flight_start=flight_start,
        flight_end=flight_end,
        evaluation_date=eval_date,
    )


def _query_platform_metrics(
    project_code: str, flight_start: date, eval_date: date
) -> list[PlatformMetrics]:
    """Query aggregated metrics per platform from fact_digital_daily.

    Uses MAX for reach (Phase 0 finding) and SUM for everything else.
    Also JOINs fact_adset_daily for reach/frequency data.
    """
    sql = f"""
        WITH daily_agg AS (
            SELECT
                platform_id,
                SUM(spend) as spend,
                SUM(impressions) as impressions,
                SUM(clicks) as clicks,
                SUM(conversions) as conversions,
                SUM(video_views_3s) as video_views_3s,
                SUM(thruplay) as thruplay,
                SUM(video_q25) as video_q25,
                SUM(video_q50) as video_q50,
                SUM(video_q75) as video_q75,
                SUM(video_q100) as video_q100,
                SUM(post_engagement) as post_engagement,
                SUM(post_reactions) as post_reactions,
                SUM(post_comments) as post_comments,
                SUM(outbound_clicks) as outbound_clicks,
                SUM(landing_page_views) as landing_page_views,
                SUM(registrations) as registrations,
                SUM(leads) as leads,
                SUM(on_platform_leads) as on_platform_leads,
                SUM(contacts) as contacts,
                SUM(donations) as donations,
                MAX(campaign_objective) as campaign_objective,
                SUM(viewability_measured) as viewability_measured,
                SUM(viewability_viewed) as viewability_viewed
            FROM {bq.table('fact_digital_daily')}
            WHERE project_code = @project_code
              AND date BETWEEN @flight_start AND @eval_date
            GROUP BY platform_id
        ),
        adset_agg AS (
            SELECT
                platform_id,
                MAX(reach_7day) as reach,
                AVG(frequency_7day) as frequency
            FROM {bq.table('fact_adset_daily')}
            WHERE project_code = @project_code
              AND date BETWEEN @flight_start AND @eval_date
            GROUP BY platform_id
        )
        SELECT
            d.*,
            COALESCE(a.reach, 0) as reach,
            COALESCE(a.frequency, 0) as frequency
        FROM daily_agg d
        LEFT JOIN adset_agg a USING (platform_id)
    """
    params = [
        bq.string_param("project_code", project_code),
        bq.date_param("flight_start", flight_start),
        bq.date_param("eval_date", eval_date),
    ]
    rows = bq.run_query(sql, params)

    return [
        PlatformMetrics(
            platform_id=r["platform_id"],
            spend=float(r.get("spend") or 0),
            impressions=int(r.get("impressions") or 0),
            clicks=int(r.get("clicks") or 0),
            conversions=float(r.get("conversions") or 0),
            reach=int(r.get("reach") or 0),
            frequency=float(r.get("frequency") or 0),
            video_views_3s=int(r.get("video_views_3s") or 0),
            thruplay=int(r.get("thruplay") or 0),
            video_q25=int(r.get("video_q25") or 0),
            video_q50=int(r.get("video_q50") or 0),
            video_q75=int(r.get("video_q75") or 0),
            video_q100=int(r.get("video_q100") or 0),
            post_engagement=int(r.get("post_engagement") or 0),
            post_reactions=int(r.get("post_reactions") or 0),
            post_comments=int(r.get("post_comments") or 0),
            outbound_clicks=int(r.get("outbound_clicks") or 0),
            landing_page_views=int(r.get("landing_page_views") or 0),
            registrations=float(r.get("registrations") or 0),
            leads=float(r.get("leads") or 0),
            on_platform_leads=float(r.get("on_platform_leads") or 0),
            contacts=float(r.get("contacts") or 0),
            donations=float(r.get("donations") or 0),
            campaign_objective=r.get("campaign_objective"),
            viewability_measured=int(r.get("viewability_measured") or 0),
            viewability_viewed=int(r.get("viewability_viewed") or 0),
        )
        for r in rows
    ]


def _query_daily_metrics(
    project_code: str, flight_start: date, eval_date: date
) -> list[DailyMetrics]:
    """Query daily breakdown for trend signals."""
    sql = f"""
        SELECT
            date,
            platform_id,
            SUM(spend) as spend,
            SUM(impressions) as impressions,
            SUM(clicks) as clicks,
            SUM(conversions) as conversions,
            SUM(video_views_3s) as video_views_3s,
            SUM(thruplay) as thruplay,
            SUM(post_engagement) as post_engagement
        FROM {bq.table('fact_digital_daily')}
        WHERE project_code = @project_code
          AND date BETWEEN @flight_start AND @eval_date
        GROUP BY date, platform_id
        ORDER BY date
    """
    params = [
        bq.string_param("project_code", project_code),
        bq.date_param("flight_start", flight_start),
        bq.date_param("eval_date", eval_date),
    ]
    rows = bq.run_query(sql, params)

    return [
        DailyMetrics(
            date=r["date"],
            platform_id=r["platform_id"],
            spend=float(r.get("spend") or 0),
            impressions=int(r.get("impressions") or 0),
            clicks=int(r.get("clicks") or 0),
            conversions=float(r.get("conversions") or 0),
            video_views_3s=int(r.get("video_views_3s") or 0),
            thruplay=int(r.get("thruplay") or 0),
            post_engagement=int(r.get("post_engagement") or 0),
        )
        for r in rows
    ]


def _query_ga4(
    project_code: str, flight_start: date, eval_date: date
) -> GA4Metrics:
    """Query GA4 session data."""
    sql = f"""
        SELECT
            COALESCE(SUM(sessions), 0) as sessions,
            COALESCE(SUM(scroll_events), 0) as scrolls,
            COALESCE(SUM(engaged_sessions), 0) as engaged_sessions,
            COALESCE(SUM(form_starts), 0) as form_starts,
            COALESCE(SUM(form_submits), 0) as form_submits,
            COALESCE(SUM(key_events), 0) as key_events
        FROM {bq.table('fact_ga4_daily')}
        WHERE project_code = @project_code
          AND date BETWEEN @flight_start AND @eval_date
    """
    params = [
        bq.string_param("project_code", project_code),
        bq.date_param("flight_start", flight_start),
        bq.date_param("eval_date", eval_date),
    ]
    rows = bq.run_query(sql, params)

    if rows:
        r = rows[0]
        return GA4Metrics(
            sessions=int(r.get("sessions") or 0),
            scrolls=int(r.get("scrolls") or 0),
            engaged_sessions=int(r.get("engaged_sessions") or 0),
            form_starts=int(r.get("form_starts") or 0),
            form_submits=int(r.get("form_submits") or 0),
            key_events=int(r.get("key_events") or 0),
        )
    return GA4Metrics()


def _query_budget_pacing(project_code: str, eval_date: date) -> float | None:
    """Get latest budget pacing percentage from budget_tracking."""
    sql = f"""
        SELECT pacing_percentage
        FROM {bq.table('budget_tracking')}
        WHERE project_code = @project_code
          AND date = @eval_date
        LIMIT 1
    """
    params = [
        bq.string_param("project_code", project_code),
        bq.date_param("eval_date", eval_date),
    ]
    try:
        rows = bq.run_query(sql, params)
        if rows:
            return float(rows[0].get("pacing_percentage") or 0)
    except Exception:
        logger.debug("Budget pacing query failed for %s", project_code, exc_info=True)
    return None


# ── Storage ─────────────────────────────────────────────────────────


def _store_results(outputs: list[DiagnosticOutput]) -> None:
    """Write diagnostic results to fact_diagnostic_signals."""
    if not outputs:
        return

    records = [o.to_bq_row() for o in outputs]
    target = f"{settings.gcp_project_id}.{settings.bigquery_dataset}.fact_diagnostic_signals"

    client = bqmod.Client(
        project=settings.gcp_project_id,
        location=settings.gcp_region,
    )
    try:
        cfg = bqmod.LoadJobConfig(
            source_format=bqmod.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bqmod.WriteDisposition.WRITE_APPEND,
        )
        job = client.load_table_from_json(records, target, job_config=cfg)
        job.result()
        logger.info("Stored %d diagnostic records", len(records))
    except Exception:
        logger.error("Failed to store diagnostic results", exc_info=True)
        raise
    finally:
        client.close()


# ── Alert dispatch ──────────────────────────────────────────────────


def _fire_alerts(output: DiagnosticOutput) -> None:
    """Write diagnostic alerts to the alerts table for Slack dispatch.

    Uses the existing alerts table + slack_alerts.py pipeline.
    """
    if not output.alerts:
        return

    now = datetime.now(timezone.utc).isoformat()
    records = []

    for alert in output.alerts:
        records.append({
            "alert_id": f"diag-{output.project_code}-{alert.type}-{output.evaluation_date}",
            "project_code": output.project_code,
            "alert_type": f"diagnostic_{alert.type}",
            "severity": alert.severity.value,
            "title": f"Diagnostic: {alert.type.replace('_', ' ').title()}",
            "message": alert.message,
            "metric_value": None,
            "threshold_value": None,
            "is_resolved": False,
            "created_at": now,
        })

    if not records:
        return

    target = f"{settings.gcp_project_id}.{settings.bigquery_dataset}.alerts"
    client = bqmod.Client(
        project=settings.gcp_project_id,
        location=settings.gcp_region,
    )
    try:
        cfg = bqmod.LoadJobConfig(
            source_format=bqmod.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bqmod.WriteDisposition.WRITE_APPEND,
        )
        client.load_table_from_json(records, target, job_config=cfg).result()
        logger.info("Fired %d diagnostic alerts for %s",
                     len(records), output.project_code)
    except Exception:
        logger.error("Failed to fire diagnostic alerts", exc_info=True)
    finally:
        client.close()
