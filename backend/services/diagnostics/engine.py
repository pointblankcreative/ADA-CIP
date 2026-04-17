"""Diagnostic engine orchestrator — mixed-campaign aware.

For each active project:
    1. Query the media plan and partition lines by campaign type (per line).
    2. Query platform / daily / GA4 / budget data, partitioning metric rows
       by their campaign_objective (or campaign_name fallback).
    3. Build one CampaignData subset per campaign type present.
    4. Run the matching health computation for each subset (persuasion /
       conversion). Projects with both kinds of lines produce TWO
       DiagnosticOutput objects — one per type.
    5. Store all outputs in fact_diagnostic_signals (already keyed on
       campaign_type, so two rows per project-date is natural).
    6. Fire critical alerts per output through the existing alerts pipeline.

Designed to be called from daily_job.py after the pacing stage, or manually
via /api/diagnostics/{code}/run. See Build Plan §12 for the mixed-campaign
design rationale.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from datetime import date, datetime, timezone

from google.cloud import bigquery as bqmod

from backend.config import settings
from backend.services import bigquery_client as bq
from backend.services.diagnostics.line_classifier import (
    classify_campaign_name,
    classify_objective_string,
    partition_lines,
)
from backend.services.diagnostics.models import (
    AudienceType,
    CampaignData,
    CampaignType,
    DailyMetrics,
    DiagnosticOutput,
    FlightContext,
    GA4Metrics,
    MediaPlanLine,
    PlatformMetrics,
    StatusBand,
)
from backend.services.diagnostics.persuasion.health import compute_persuasion_health
from backend.services.diagnostics.conversion.health import compute_conversion_health
from backend.services.diagnostics.shared.alerts import build_regression_alert

logger = logging.getLogger(__name__)


def _health_computer_for(campaign_type: CampaignType):
    """Resolve the health-computation function for a campaign type at CALL time.

    Deliberately NOT cached into a module-level dict — doing so would capture
    the original function references at import and make the health computers
    unpatchable in unit tests. The per-call lookup cost is negligible.
    """
    if campaign_type == CampaignType.PERSUASION:
        return compute_persuasion_health
    return compute_conversion_health


# ── Public API ──────────────────────────────────────────────────────


def run_diagnostics_for_project(
    project_code: str,
    evaluation_date: date | None = None,
) -> list[DiagnosticOutput]:
    """Run diagnostic evaluation for a single project.

    Returns a list of DiagnosticOutput objects — one per campaign type present
    in the project's media plan. Pure projects return a single-element list;
    mixed projects return two elements (persuasion + conversion).
    Returns an empty list if the project has no media plan or no derivable flight.
    """
    eval_date = evaluation_date or date.today()
    results: list[DiagnosticOutput] = []

    # Step 1: Query the media plan and partition lines by campaign type.
    media_plan = _query_media_plan(project_code)
    if not media_plan:
        logger.warning("No media plan lines for %s", project_code)
        return results

    lines_by_type = partition_lines(media_plan)

    # Step 2: Query all data sources ONCE. Queries return per-type partitions
    # (dict[CampaignType, ...]) for platform_metrics and daily_metrics.
    # GA4 goes to both subsets unchanged (per Build Plan §12). Budget pacing
    # is re-queried per subset using the line_ids of that subset.
    overall_flight = _derive_flight(media_plan, eval_date)
    if overall_flight is None:
        logger.warning("Could not derive any flight dates for %s", project_code)
        return results

    platform_by_type = _query_platform_metrics_by_type(
        project_code, overall_flight.flight_start, eval_date
    )
    daily_by_type = _query_daily_metrics_by_type(
        project_code, overall_flight.flight_start, eval_date
    )
    ga4 = _query_ga4(project_code, overall_flight.flight_start, eval_date)

    # Step 3–4: For each campaign type with lines, build CampaignData + compute.
    for campaign_type, lines in lines_by_type.items():
        if not lines:
            continue

        flight = _derive_flight(lines, eval_date) or overall_flight
        platform_metrics = platform_by_type.get(campaign_type, [])
        daily_metrics = daily_by_type.get(campaign_type, [])
        line_ids = {l.line_id for l in lines}
        pacing_pct = _query_budget_pacing(project_code, eval_date, line_ids=line_ids)

        data = CampaignData(
            project_code=project_code,
            campaign_type=campaign_type,
            flight=flight,
            platform_metrics=platform_metrics,
            daily_metrics=daily_metrics,
            media_plan=lines,
            ga4=ga4,
            budget_pacing_pct=pacing_pct,
        )

        compute = _health_computer_for(campaign_type)
        output = compute(data)
        results.append(output)

    if not results:
        logger.warning("No classifiable media plan lines for %s", project_code)
        return results

    # Step 4.5: Health-regression alerts require the prior evaluation,
    # so they're added here — after per-signal alerts (populated inside
    # the health modules) but before storage, so the stored row reflects
    # the full alert set. See docs/diagnostics/alert-rules.md.
    for output in results:
        _populate_regression_alert(output)

    # Step 5: Store all outputs in a single load job.
    _store_results(results)

    # Step 6: Fire alerts per output.
    for output in results:
        _fire_alerts(output)

    for output in results:
        logger.info(
            "Diagnostics complete for %s [%s]: health=%s status=%s",
            project_code,
            output.campaign_type.value,
            output.health_score,
            output.health_status,
        )

    return results


def run_all_diagnostics(evaluation_date: date | None = None) -> dict:
    """Run diagnostics for all active projects.

    Called from daily_job.py. Returns a summary dict.
    """
    eval_date = evaluation_date or date.today()
    projects = _get_active_projects()
    summary = {
        "projects_processed": 0,
        "projects_skipped": 0,
        "total_outputs": 0,
        "total_alerts": 0,
        "errors": [],
    }

    for project_code in projects:
        try:
            outputs = run_diagnostics_for_project(project_code, eval_date)
            if outputs:
                summary["projects_processed"] += 1
                summary["total_outputs"] += len(outputs)
                summary["total_alerts"] += sum(len(o.alerts) for o in outputs)
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


def _parse_frequency_cap(value) -> float:
    """Parse frequency_cap — stored as STRING (e.g. '3/7d', '5') or None."""
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    s = str(value).strip()
    if not s:
        return 0.0
    # Pull the first numeric chunk (handles '3/7d', '3 per 7 days', '5', etc.)
    import re
    m = re.search(r"[\d.]+", s)
    try:
        return float(m.group(0)) if m else 0.0
    except (TypeError, ValueError):
        return 0.0


def _query_media_plan(project_code: str) -> list[MediaPlanLine]:
    """Query media plan lines for a project.

    Column names in BQ: `budget` (not planned_budget), `estimated_impressions`
    (not planned_impressions), no `planned_reach` column, and `frequency_cap`
    is STRING. We alias and coerce here so the engine sees a clean shape.
    """
    sql = f"""
        SELECT
            line_id,
            platform_id,
            channel_category,
            audience_name,
            audience_type,
            COALESCE(budget, 0) as planned_budget,
            COALESCE(estimated_impressions, 0) as planned_impressions,
            frequency_cap,
            flight_start,
            flight_end,
            ffs_score,
            objective
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY line_id ORDER BY sync_version DESC
                   ) AS _rn
            FROM {bq.table('media_plan_lines')}
            WHERE project_code = @project_code
        ) WHERE _rn = 1
    """
    rows = bq.run_query(sql, [bq.string_param("project_code", project_code)])

    lines = []
    for r in rows:
        audience = r.get("audience_type")
        try:
            audience_type = AudienceType(audience) if audience else None
        except ValueError:
            audience_type = None

        freq_cap = _parse_frequency_cap(r.get("frequency_cap"))
        planned_impressions = int(r.get("planned_impressions") or 0)
        # Derive planned_reach from impressions / frequency_cap when possible;
        # there is no planned_reach column in BigQuery yet.
        planned_reach = int(planned_impressions / freq_cap) if freq_cap > 0 else 0

        lines.append(MediaPlanLine(
            line_id=str(r["line_id"]),
            platform_id=r.get("platform_id"),
            channel_category=r.get("channel_category"),
            audience_name=r.get("audience_name"),
            audience_type=audience_type,
            planned_budget=float(r.get("planned_budget") or 0),
            planned_impressions=planned_impressions,
            planned_reach=planned_reach,
            frequency_cap=freq_cap,
            flight_start=r.get("flight_start"),
            flight_end=r.get("flight_end"),
            ffs_score=float(r["ffs_score"]) if r.get("ffs_score") else None,
            objective=r.get("objective"),
        ))

    return lines


def _derive_flight(
    media_plan: list[MediaPlanLine], eval_date: date
) -> FlightContext | None:
    """Derive flight start/end from media plan lines.

    For partitioned subsets, call this with the subset's lines only so each
    campaign-type diagnostic runs against its own flight calendar.
    """
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


def _query_platform_metrics_by_type(
    project_code: str, flight_start: date, eval_date: date
) -> dict[CampaignType, list[PlatformMetrics]]:
    """Aggregate platform metrics per (platform_id, campaign_type) and return
    a dict keyed by CampaignType.

    We pull fact_digital_daily grouped by (platform_id, campaign_objective) —
    `campaign_objective` is a first-class column there. Each resulting row is
    classified (persuasion/conversion) and merged into the matching bucket.

    Reach / frequency live in fact_adset_daily, which does NOT carry
    campaign_objective. We classify those rows by `campaign_name` via the
    shared keyword classifier and bucket them the same way.

    When a platform contributes to both buckets (e.g. Meta runs awareness +
    retargeting in the same project), each bucket gets its own PlatformMetrics
    row for that platform, with the correct subset of spend/impressions/reach.
    """
    # ── daily (digital) rows — grouped by (platform_id, campaign_objective) ──
    daily_sql = f"""
        SELECT
            platform_id,
            campaign_objective,
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
            SUM(viewability_measured) as viewability_measured,
            SUM(viewability_viewed) as viewability_viewed
        FROM {bq.table('fact_digital_daily')}
        WHERE project_code = @project_code
          AND date BETWEEN @flight_start AND @eval_date
        GROUP BY platform_id, campaign_objective
    """
    params = [
        bq.string_param("project_code", project_code),
        bq.date_param("flight_start", flight_start),
        bq.date_param("eval_date", eval_date),
    ]
    daily_rows = bq.run_query(daily_sql, params)

    # ── adset rows — grouped by (platform_id, campaign_name, reach_window) ──
    # fact_adset_daily has no campaign_objective, so we group by campaign_name
    # and classify each campaign_name in Python.
    #
    # Reach-window handling: platforms report reach/frequency against different
    # lookback windows (1d vs 7d) and those numbers are NOT comparable — a 7d
    # reach is almost always larger than a 1d reach for the same audience, and
    # mixing them in an average produces meaningless frequency. We therefore
    # GROUP BY reach_window as well and pick one window per (platform, campaign)
    # in Python: prefer 7d (more complete coverage), fall back to 1d, then to
    # null/unspecified. This is the conservative default most platforms already
    # report natively.
    #
    # MAX(reach) across the flight is a conservative floor for true unique
    # reach within a single window — a daily MAX will understate true unique
    # reach across the flight, but it is strictly a lower bound and never
    # double-counts.
    adset_sql = f"""
        SELECT
            platform_id,
            campaign_name,
            reach_window,
            MAX(reach) as reach,
            COALESCE(
                NULLIF(AVG(frequency), 0),
                SAFE_DIVIDE(SUM(impressions), NULLIF(SUM(reach), 0))
            ) as frequency,
            SUM(impressions) as adset_impressions
        FROM {bq.table('fact_adset_daily')}
        WHERE project_code = @project_code
          AND date BETWEEN @flight_start AND @eval_date
          AND (reach_window IS NULL OR reach_window IN ('7d','7day','7_day','1d','1day','1_day'))
        GROUP BY platform_id, campaign_name, reach_window
    """
    adset_rows = bq.run_query(adset_sql, params)

    # ── pick one reach_window per (platform_id, campaign_name): 7d > 1d > null
    # Doing this BEFORE bucketing by campaign type ensures each campaign
    # contributes exactly one reach/frequency pair, measured against a
    # consistent lookback window.
    _SEVEN_D = {"7d", "7day", "7_day"}
    _ONE_D = {"1d", "1day", "1_day"}

    def _window_priority(rw) -> int:
        """Higher wins. 7d=2, 1d=1, other/NULL=0."""
        if rw in _SEVEN_D:
            return 2
        if rw in _ONE_D:
            return 1
        return 0

    preferred: dict[tuple[str, str | None], dict] = {}
    for r in adset_rows:
        key = (r["platform_id"], r.get("campaign_name"))
        existing = preferred.get(key)
        if existing is None or _window_priority(r.get("reach_window")) > _window_priority(
            existing.get("reach_window")
        ):
            preferred[key] = r

    # ── bucket chosen adset rows by (type, platform_id) — aggregate reach (MAX)
    # and frequency (impression-weighted average across distinct campaign_names
    # within the same type/platform, all now measured on a single window per
    # campaign).
    adset_bucket: dict[
        tuple[CampaignType, str],
        dict[str, float],
    ] = defaultdict(lambda: {"reach": 0.0, "freq_num": 0.0, "freq_den": 0.0})
    for (platform_id, campaign_name), r in preferred.items():
        ctype = classify_campaign_name(campaign_name)
        key = (ctype, platform_id)
        entry = adset_bucket[key]
        reach = float(r.get("reach") or 0)
        freq = float(r.get("frequency") or 0)
        impr = float(r.get("adset_impressions") or 0)
        # NOTE: MAX across campaigns understates reach when audiences overlap
        # across multiple campaigns of the same type on the same platform (e.g.
        # two awareness campaigns targeting overlapping lookalikes). SUM would
        # overstate it. MAX is the conservative floor and matches prior
        # single-campaign-per-platform behaviour; revisit once we have a
        # dedupe-across-campaigns reach source.
        entry["reach"] = max(entry["reach"], reach)
        # impression-weighted frequency avg across campaign_names for same type/platform
        if freq > 0 and impr > 0:
            entry["freq_num"] += freq * impr
            entry["freq_den"] += impr

    # ── bucket daily rows by (type, platform_id) — classify campaign_objective ──
    daily_bucket: dict[tuple[CampaignType, str], dict] = {}
    for r in daily_rows:
        platform_id = r["platform_id"]
        campaign_objective = r.get("campaign_objective")
        ctype = classify_objective_string(campaign_objective)
        key = (ctype, platform_id)

        if key not in daily_bucket:
            daily_bucket[key] = {
                "spend": 0.0, "impressions": 0, "clicks": 0, "conversions": 0.0,
                "video_views_3s": 0, "thruplay": 0,
                "video_q25": 0, "video_q50": 0, "video_q75": 0, "video_q100": 0,
                "post_engagement": 0, "post_reactions": 0, "post_comments": 0,
                "outbound_clicks": 0, "landing_page_views": 0,
                "registrations": 0.0, "leads": 0.0, "on_platform_leads": 0.0,
                "contacts": 0.0, "donations": 0.0,
                "viewability_measured": 0, "viewability_viewed": 0,
                "campaign_objective": campaign_objective,  # keep first seen
            }
        b = daily_bucket[key]
        b["spend"] += float(r.get("spend") or 0)
        b["impressions"] += int(r.get("impressions") or 0)
        b["clicks"] += int(r.get("clicks") or 0)
        b["conversions"] += float(r.get("conversions") or 0)
        b["video_views_3s"] += int(r.get("video_views_3s") or 0)
        b["thruplay"] += int(r.get("thruplay") or 0)
        b["video_q25"] += int(r.get("video_q25") or 0)
        b["video_q50"] += int(r.get("video_q50") or 0)
        b["video_q75"] += int(r.get("video_q75") or 0)
        b["video_q100"] += int(r.get("video_q100") or 0)
        b["post_engagement"] += int(r.get("post_engagement") or 0)
        b["post_reactions"] += int(r.get("post_reactions") or 0)
        b["post_comments"] += int(r.get("post_comments") or 0)
        b["outbound_clicks"] += int(r.get("outbound_clicks") or 0)
        b["landing_page_views"] += int(r.get("landing_page_views") or 0)
        b["registrations"] += float(r.get("registrations") or 0)
        b["leads"] += float(r.get("leads") or 0)
        b["on_platform_leads"] += float(r.get("on_platform_leads") or 0)
        b["contacts"] += float(r.get("contacts") or 0)
        b["donations"] += float(r.get("donations") or 0)
        b["viewability_measured"] += int(r.get("viewability_measured") or 0)
        b["viewability_viewed"] += int(r.get("viewability_viewed") or 0)

    # ── merge daily + adset buckets into PlatformMetrics per type/platform ──
    # A platform can appear in the daily bucket but not the adset bucket
    # (no reach data for that type on that platform) — we still emit it with
    # reach=0. The reverse case (adset data but no daily data) is also
    # possible, though rare; we emit a near-empty PlatformMetrics for it.
    all_keys = set(daily_bucket.keys()) | set(adset_bucket.keys())

    result: dict[CampaignType, list[PlatformMetrics]] = {
        CampaignType.PERSUASION: [],
        CampaignType.CONVERSION: [],
    }
    for ctype, platform_id in all_keys:
        daily_b = daily_bucket.get((ctype, platform_id), {})
        adset_b = adset_bucket.get((ctype, platform_id), {})
        freq_num = adset_b.get("freq_num", 0.0)
        freq_den = adset_b.get("freq_den", 0.0)
        if freq_den > 0:
            frequency = freq_num / freq_den
        elif daily_b.get("impressions") and adset_b.get("reach"):
            frequency = daily_b["impressions"] / adset_b["reach"] if adset_b["reach"] else 0
        else:
            frequency = 0.0

        pm = PlatformMetrics(
            platform_id=platform_id,
            spend=daily_b.get("spend", 0.0),
            impressions=daily_b.get("impressions", 0),
            clicks=daily_b.get("clicks", 0),
            conversions=daily_b.get("conversions", 0.0),
            reach=int(adset_b.get("reach", 0)),
            frequency=float(frequency),
            video_views_3s=daily_b.get("video_views_3s", 0),
            thruplay=daily_b.get("thruplay", 0),
            video_q25=daily_b.get("video_q25", 0),
            video_q50=daily_b.get("video_q50", 0),
            video_q75=daily_b.get("video_q75", 0),
            video_q100=daily_b.get("video_q100", 0),
            post_engagement=daily_b.get("post_engagement", 0),
            post_reactions=daily_b.get("post_reactions", 0),
            post_comments=daily_b.get("post_comments", 0),
            outbound_clicks=daily_b.get("outbound_clicks", 0),
            landing_page_views=daily_b.get("landing_page_views", 0),
            registrations=daily_b.get("registrations", 0.0),
            leads=daily_b.get("leads", 0.0),
            on_platform_leads=daily_b.get("on_platform_leads", 0.0),
            contacts=daily_b.get("contacts", 0.0),
            donations=daily_b.get("donations", 0.0),
            campaign_objective=daily_b.get("campaign_objective"),
            viewability_measured=daily_b.get("viewability_measured", 0),
            viewability_viewed=daily_b.get("viewability_viewed", 0),
        )
        result[ctype].append(pm)

    return result


def _query_daily_metrics_by_type(
    project_code: str, flight_start: date, eval_date: date
) -> dict[CampaignType, list[DailyMetrics]]:
    """Query per-day, per-platform, per-type daily rows for trend signals.

    Returns {campaign_type: [DailyMetrics, ...]}. Each input row is classified
    by its `campaign_objective` and aggregated into the matching bucket keyed
    on (type, date, platform_id).
    """
    sql = f"""
        SELECT
            date,
            platform_id,
            campaign_objective,
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
        GROUP BY date, platform_id, campaign_objective
        ORDER BY date
    """
    params = [
        bq.string_param("project_code", project_code),
        bq.date_param("flight_start", flight_start),
        bq.date_param("eval_date", eval_date),
    ]
    rows = bq.run_query(sql, params)

    # Bucket by (type, date, platform_id) — collapse multiple campaign_objectives
    # that classify to the same CampaignType onto a single row per day/platform.
    buckets: dict[tuple[CampaignType, date, str], dict] = {}
    for r in rows:
        ctype = classify_objective_string(r.get("campaign_objective"))
        key = (ctype, r["date"], r["platform_id"])
        if key not in buckets:
            buckets[key] = {
                "spend": 0.0, "impressions": 0, "clicks": 0, "conversions": 0.0,
                "video_views_3s": 0, "thruplay": 0, "post_engagement": 0,
            }
        b = buckets[key]
        b["spend"] += float(r.get("spend") or 0)
        b["impressions"] += int(r.get("impressions") or 0)
        b["clicks"] += int(r.get("clicks") or 0)
        b["conversions"] += float(r.get("conversions") or 0)
        b["video_views_3s"] += int(r.get("video_views_3s") or 0)
        b["thruplay"] += int(r.get("thruplay") or 0)
        b["post_engagement"] += int(r.get("post_engagement") or 0)

    result: dict[CampaignType, list[DailyMetrics]] = {
        CampaignType.PERSUASION: [],
        CampaignType.CONVERSION: [],
    }
    for (ctype, d, platform_id), b in buckets.items():
        result[ctype].append(DailyMetrics(
            date=d,
            platform_id=platform_id,
            spend=b["spend"],
            impressions=b["impressions"],
            clicks=b["clicks"],
            conversions=b["conversions"],
            video_views_3s=b["video_views_3s"],
            thruplay=b["thruplay"],
            post_engagement=b["post_engagement"],
        ))

    # Preserve the prior ordering: daily lists sorted by date.
    for ctype in result:
        result[ctype].sort(key=lambda dm: (dm.date, dm.platform_id))
    return result


def _query_ga4(
    project_code: str, flight_start: date, eval_date: date
) -> GA4Metrics:
    """Query GA4 session data by joining project_ga4_urls → fact_ga4_daily.

    `fact_ga4_daily` has no project_code column; attribution is via the
    `project_ga4_urls` mapping table (project_code ↔ ga4_property_id +
    url_pattern). If the project has no GA4 URLs configured, we return
    empty metrics — this is expected and must not break Distribution signals.

    Note: GA4 data is NOT partitioned by campaign type (session data isn't
    objective-tagged). Per Build Plan §12, the same GA4Metrics feeds both the
    persuasion subset (for R3 landing page depth) and the conversion subset
    (for F3–F5 funnel signals).
    """
    try:
        url_rows = bq.run_query(
            f"""
                SELECT ga4_property_id, url_pattern
                FROM {bq.table('project_ga4_urls')}
                WHERE project_code = @project_code
            """,
            [bq.string_param("project_code", project_code)],
        )
    except Exception:
        logger.debug("GA4 url lookup failed for %s", project_code, exc_info=True)
        return GA4Metrics()

    if not url_rows:
        return GA4Metrics()

    # Build an OR clause across (property_id, url_pattern) tuples.
    clauses = []
    params: list = [
        bq.date_param("flight_start", flight_start),
        bq.date_param("eval_date", eval_date),
    ]
    for i, r in enumerate(url_rows):
        pid_name = f"ga4_pid_{i}"
        url_name = f"ga4_url_{i}"
        clauses.append(
            f"(ga4_property_id = @{pid_name} AND session_campaign LIKE @{url_name})"
        )
        params.append(bq.string_param(pid_name, str(r["ga4_property_id"])))
        params.append(bq.string_param(url_name, f"%{r['url_pattern']}%"))

    where_urls = " OR ".join(clauses) if clauses else "FALSE"
    sql = f"""
        SELECT
            COALESCE(SUM(sessions), 0) as sessions,
            COALESCE(SUM(scroll_events), 0) as scrolls,
            -- engaged_sessions isn't yet mapped in fact_ga4_daily; fall back
            -- to user_engagements (Build Plan §10 open gap).
            COALESCE(SUM(user_engagements), 0) as engaged_sessions,
            COALESCE(SUM(form_starts), 0) as form_starts,
            COALESCE(SUM(form_submits), 0) as form_submits,
            COALESCE(SUM(key_events), 0) as key_events
        FROM {bq.table('fact_ga4_daily')}
        WHERE date BETWEEN @flight_start AND @eval_date
          AND ({where_urls})
    """
    try:
        rows = bq.run_query(sql, params)
    except Exception:
        logger.debug("GA4 query failed for %s", project_code, exc_info=True)
        return GA4Metrics()

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


def _query_budget_pacing(
    project_code: str,
    eval_date: date,
    line_ids: set[str] | None = None,
) -> float | None:
    """Get project-level budget pacing percentage from budget_tracking.

    budget_tracking is stored per-line, so we roll it up to a single pacing
    figure: SUM(actual_spend_to_date) / SUM(planned_spend_to_date) * 100.
    Falls back to the latest prior date if the target date has no rows yet
    (e.g. the daily budget_tracking refresh hasn't run for today).

    When `line_ids` is provided, the rollup is restricted to those lines only
    — used by the mixed-campaign engine to compute pacing per subset.
    """
    line_filter = ""
    params = [
        bq.string_param("project_code", project_code),
        bq.date_param("eval_date", eval_date),
    ]
    if line_ids:
        # BigQuery parameterised IN-clause via UNNEST
        line_filter = "AND bt.line_id IN UNNEST(@line_ids)"
        params.append(bq.array_param("line_ids", "STRING", sorted(line_ids)))

    sql = f"""
        WITH target AS (
          SELECT MAX(date) AS d
          FROM {bq.table('budget_tracking')}
          WHERE project_code = @project_code
            AND date <= @eval_date
        )
        SELECT
          SAFE_DIVIDE(
            SUM(actual_spend_to_date),
            NULLIF(SUM(planned_spend_to_date), 0)
          ) * 100 AS pacing_percentage
        FROM {bq.table('budget_tracking')} bt
        CROSS JOIN target
        WHERE bt.project_code = @project_code
          AND bt.date = target.d
          {line_filter}
    """
    try:
        rows = bq.run_query(sql, params)
        if rows and rows[0].get("pacing_percentage") is not None:
            return float(rows[0]["pacing_percentage"])
    except Exception:
        logger.debug("Budget pacing query failed for %s", project_code, exc_info=True)
    return None


# ── Storage ─────────────────────────────────────────────────────────


# ── Regression alert ────────────────────────────────────────────────


def _populate_regression_alert(output: DiagnosticOutput) -> None:
    """Append a health-regression alert if the campaign just entered ACTION.

    Queries fact_diagnostic_signals for the prior evaluation on the same
    (project_code, campaign_type) and delegates the firing decision to
    shared.alerts.build_regression_alert.

    Failures to query prior history are logged and silently skip the alert
    — a missing regression alert is a non-critical degradation; nothing
    else depends on it.
    """
    if output.health_status != StatusBand.ACTION:
        return

    try:
        prev_status, prev_score = _query_prior_health(
            output.project_code,
            output.campaign_type,
            output.evaluation_date,
        )
    except Exception:
        logger.warning(
            "Could not query prior health for %s [%s]; skipping regression alert",
            output.project_code,
            output.campaign_type.value,
            exc_info=True,
        )
        return

    alert = build_regression_alert(output, prev_status, prev_score)
    if alert is not None:
        output.alerts.append(alert)


def _query_prior_health(
    project_code: str,
    campaign_type: CampaignType,
    evaluation_date: date,
) -> tuple[StatusBand | None, float | None]:
    """Return (status, score) of the most recent evaluation before today.

    Returns (None, None) if no prior row exists. The table is clustered
    on (project_code, campaign_type) so this is a cheap lookup.
    """
    sql = f"""
        SELECT health_status, health_score
        FROM {bq.table('fact_diagnostic_signals')}
        WHERE project_code = @project_code
          AND campaign_type = @campaign_type
          AND evaluation_date < @evaluation_date
        ORDER BY evaluation_date DESC
        LIMIT 1
    """
    rows = bq.run_query(
        sql,
        [
            bq.string_param("project_code", project_code),
            bq.string_param("campaign_type", campaign_type.value),
            bq.date_param("evaluation_date", evaluation_date),
        ],
    )
    if not rows:
        return None, None

    row = rows[0]
    raw_status = row.get("health_status")
    try:
        status = StatusBand(raw_status) if raw_status else None
    except ValueError:
        status = None

    raw_score = row.get("health_score")
    score = float(raw_score) if raw_score is not None else None
    return status, score


def _store_results(outputs: list[DiagnosticOutput]) -> None:
    """Write diagnostic results to fact_diagnostic_signals.

    Accepts one or more outputs (typically 1 for pure projects, 2 for mixed).
    fact_diagnostic_signals is clustered by (project_code, campaign_type),
    so multiple outputs per project-date is the intended shape.
    """
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

    Alert IDs are namespaced by campaign_type so persuasion and conversion
    diagnostics for the same project-date don't collide. Before inserting,
    a 24h dedup pass matches the pattern used by services/pacing — keyed on
    (project_code, alert_type, severity). See docs/diagnostics/alert-rules.md.
    """
    if not output.alerts:
        return

    now = datetime.now(timezone.utc).isoformat()
    records = []

    for alert in output.alerts:
        alert_type = f"diagnostic_{alert.type}"
        title = _alert_title(output, alert)
        records.append({
            "alert_id": (
                f"diag-{output.project_code}-{output.campaign_type.value}"
                f"-{alert.type}-{output.evaluation_date}"
            ),
            "project_code": output.project_code,
            "alert_type": alert_type,
            "severity": alert.severity.value,
            "title": title,
            "message": alert.message,
            "metric_value": None,
            "threshold_value": None,
            "is_resolved": False,
            "created_at": now,
        })

    # 24h dedup — suppress alerts already fired for the same
    # (project, type, severity) within the last day. Mirrors
    # services/pacing._deduplicate_alerts.
    records = _deduplicate_diagnostic_alerts(records)
    if not records:
        logger.info(
            "No new diagnostic alerts for %s [%s] after dedup",
            output.project_code,
            output.campaign_type.value,
        )
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
        logger.info("Fired %d diagnostic alerts for %s [%s]",
                     len(records), output.project_code, output.campaign_type.value)
    except Exception:
        logger.error("Failed to fire diagnostic alerts", exc_info=True)
    finally:
        client.close()


def _alert_title(output: DiagnosticOutput, alert) -> str:
    """Human-readable title per docs/diagnostics/alert-rules.md."""
    prefix = f"{output.project_code} [{output.campaign_type.value}]"
    score_fmt = _format_alert_score(output.health_score)

    if alert.type == "health_regression":
        return f"{prefix} \u00b7 Health dropped to ACTION ({score_fmt})"

    if alert.type.startswith("signal_") and alert.signal_id:
        signal = _find_signal(output, alert.signal_id)
        sig_score = _format_alert_score(signal.score if signal else None)
        sig_name = signal.name if signal else alert.signal_id
        return (
            f"{prefix} \u00b7 {alert.signal_id} {sig_name} "
            f"\u2014 ACTION ({sig_score})"
        )

    # Fallback — should not happen given current alert types, but keep
    # the old shape so nothing downstream breaks silently.
    return f"Diagnostic: {alert.type.replace('_', ' ').title()}"


def _find_signal(output: DiagnosticOutput, signal_id: str):
    for pillar in output.pillars:
        for signal in pillar.signals:
            if signal.id == signal_id:
                return signal
    return None


def _format_alert_score(score) -> str:
    if score is None:
        return "\u2014"
    try:
        score_f = float(score)
    except (TypeError, ValueError):
        return str(score)
    if abs(score_f - round(score_f)) < 0.05:
        return f"{int(round(score_f))}"
    return f"{score_f:.1f}"


def _deduplicate_diagnostic_alerts(records: list[dict]) -> list[dict]:
    """Suppress alerts that already exist in the last 24h.

    Mirrors services/pacing._deduplicate_alerts — keyed on
    (project_code, alert_type, severity), 24h window, ignoring
    already-resolved rows.
    """
    if not records:
        return []

    project_codes = list({r["project_code"] for r in records if r.get("project_code")})
    if not project_codes:
        return records

    params = []
    conditions = []
    for i, pc in enumerate(project_codes):
        pname = f"pc_{i}"
        conditions.append(f"@{pname}")
        params.append(bq.string_param(pname, pc))

    sql = f"""
        SELECT project_code, alert_type, severity
        FROM {bq.table('alerts')}
        WHERE project_code IN ({", ".join(conditions)})
          AND created_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
          AND resolved_at IS NULL
    """
    try:
        existing_rows = bq.run_query(sql, params)
    except Exception:
        # If the dedup query fails (e.g. resolved_at column missing on a
        # fresh table), fall back to inserting everything. A duplicate
        # beats a silent miss.
        logger.warning(
            "Diagnostic alert dedup query failed; inserting without dedup",
            exc_info=True,
        )
        return records

    existing_keys = {
        (r["project_code"], r["alert_type"], r["severity"])
        for r in existing_rows
    }

    deduped = []
    for r in records:
        key = (r["project_code"], r["alert_type"], r["severity"])
        if key in existing_keys:
            logger.debug("Skipping duplicate diagnostic alert: %s", key)
            continue
        deduped.append(r)

    skipped = len(records) - len(deduped)
    if skipped:
        logger.info(
            "  Diagnostic dedup: skipped %d duplicate alerts out of %d",
            skipped,
            len(records),
        )
    return deduped
