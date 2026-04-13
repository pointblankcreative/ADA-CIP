import logging
from datetime import date, timedelta

from fastapi import APIRouter, HTTPException, Query
from google.cloud import exceptions as gcp_exceptions

from backend.models.performance import (
    AdPerformanceResponse,
    AdRow,
    AdSetPerformanceResponse,
    AdSetRow,
    CampaignRow,
    CreativeVariantResponse,
    CreativeVariantRow,
    DailyMetric,
    PerformanceResponse,
    PlatformBreakdown,
)
from backend.services import bigquery_client as bq
from backend.services.objective_classifier import classify_objective, classify_project

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/performance", tags=["performance"])


def _float(v, default=0.0) -> float:
    return float(v) if v is not None else default

def _int(v, default=0) -> int:
    return int(v) if v is not None else default

def _float_or_none(v) -> float | None:
    return float(v) if v is not None else None

def _int_or_none(v) -> int | None:
    return int(v) if v is not None else None


PLATFORM_NAMES = {
    "meta": "Meta",
    "google_ads": "Google Ads",
    "linkedin": "LinkedIn",
    "stackadapt": "StackAdapt",
    "tiktok": "TikTok",
    "snapchat": "Snapchat",
    "perion": "Perion DOOH",
    "reddit": "Reddit",
    "pinterest": "Pinterest",
}


def _date_filter(start_date: str | None, end_date: str | None) -> tuple[str, list]:
    clauses: list[str] = []
    params = []
    if start_date:
        clauses.append("date >= @start_date")
        params.append(bq.date_param("start_date", date.fromisoformat(start_date)))
    if end_date:
        clauses.append("date <= @end_date")
        params.append(bq.date_param("end_date", date.fromisoformat(end_date)))
    return (" AND ".join(clauses) if clauses else "1=1"), params


def _load_media_plan_objectives(project_code: str) -> dict[str, list[str]]:
    """Load objectives from media_plan_lines for a project, keyed by platform.

    Returns a list of objective strings per platform because a single platform
    can have multiple lines with different objectives (e.g. Meta running both
    an awareness flight and a conversion flight).
    """
    try:
        sql = f"""
            SELECT platform_id, objective
            FROM {bq.table('media_plan_lines')}
            WHERE project_code = @project_code
              AND objective IS NOT NULL
        """
        rows = bq.run_query(sql, [bq.string_param("project_code", project_code)])
        result: dict[str, list[str]] = {}
        for r in rows:
            pid = r.get("platform_id")
            obj = r.get("objective")
            if pid and obj:
                result.setdefault(pid, []).append(obj)
        return result
    except (gcp_exceptions.GoogleCloudError, gcp_exceptions.NotFound) as e:
        logger.warning("Failed to fetch objectives for project %s: %s", project_code, e, exc_info=True)
        return {}


def _resolve_perf_dates(
    start_date: str | None,
    end_date: str | None,
    days: int | None,
) -> tuple[str | None, str | None]:
    if days and not start_date:
        end_date = end_date or date.today().isoformat()
        start_date = (date.fromisoformat(end_date) - timedelta(days=days)).isoformat()
    return start_date, end_date


@router.get("/{project_code}/adsets", response_model=AdSetPerformanceResponse)
async def get_adset_performance(
    project_code: str,
    start_date: str | None = Query(None),
    end_date: str | None = Query(None),
    days: int | None = Query(None),
    platform: str | None = Query(None),
):
    start_date, end_date = _resolve_perf_dates(start_date, end_date, days)
    date_clause, date_params = _date_filter(start_date, end_date)
    plat = "AND f.platform_id = @platform" if platform else ""
    params = [bq.string_param("project_code", project_code)] + date_params
    if platform:
        params.append(bq.string_param("platform", platform))

    reach_plat = "AND platform_id = @platform" if platform else ""
    sql = f"""
        WITH ad_metrics AS (
            SELECT
                f.campaign_id,
                ANY_VALUE(f.campaign_name) AS campaign_name,
                f.ad_set_id,
                ANY_VALUE(f.ad_set_name) AS ad_set_name,
                f.platform_id,
                SUM(f.spend) AS spend,
                SUM(f.impressions) AS impressions,
                SUM(f.clicks) AS clicks,
                SUM(f.conversions) AS conversions,
                SUM(f.engagements) AS engagements,
                SUM(f.video_views) AS video_views,
                SUM(f.video_completions) AS video_completions,
                COUNT(DISTINCT f.ad_id) AS ad_count
            FROM {bq.table('fact_digital_daily')} f
            WHERE f.project_code = @project_code AND {date_clause} {plat}
            GROUP BY f.campaign_id, f.ad_set_id, f.ad_set_name, f.platform_id
        ),
        reach AS (
            SELECT
                platform_id,
                campaign_id,
                MAX(reach) AS reach,
                MAX(frequency) AS frequency,
                ANY_VALUE(reach_window) AS reach_window
            FROM {bq.table('fact_adset_daily')}
            WHERE project_code = @project_code AND {date_clause} {reach_plat}
            GROUP BY platform_id, campaign_id
        )
        SELECT
            a.ad_set_id,
            a.ad_set_name,
            a.platform_id,
            a.campaign_name,
            a.spend,
            a.impressions,
            a.clicks,
            a.conversions,
            a.engagements,
            a.video_views,
            a.video_completions,
            a.ad_count,
            SAFE_DIVIDE(a.spend, NULLIF(a.impressions, 0)) * 1000 AS cpm,
            SAFE_DIVIDE(a.spend, NULLIF(a.clicks, 0)) AS cpc,
            SAFE_DIVIDE(a.clicks, NULLIF(a.impressions, 0)) AS ctr,
            SAFE_DIVIDE(a.video_completions, NULLIF(a.video_views, 0)) AS vcr,
            SAFE_DIVIDE(a.engagements, NULLIF(a.impressions, 0)) AS engagement_rate,
            r.reach,
            r.frequency,
            r.reach_window,
            SAFE_DIVIDE(a.spend, NULLIF(r.reach, 0)) * 1000 AS cost_per_reach
        FROM ad_metrics a
        LEFT JOIN reach r
            ON a.platform_id = r.platform_id
            AND a.campaign_id = r.campaign_id
        ORDER BY a.spend DESC
    """

    rows = bq.run_query(sql, params)
    no_reach = {"google_ads", "pinterest"}
    reach_ok = sorted({r["platform_id"] for r in rows if r.get("reach")}) if rows else []
    note = None
    if reach_ok:
        names = [PLATFORM_NAMES.get(p, p) for p in reach_ok if p not in no_reach]
        if names:
            note = "Reach from " + ", ".join(names) + ". Not additive across audiences."
    return AdSetPerformanceResponse(
        project_code=project_code,
        start_date=date.fromisoformat(start_date) if start_date else None,
        end_date=date.fromisoformat(end_date) if end_date else None,
        ad_sets=[
            AdSetRow(
                ad_set_id=r.get("ad_set_id"),
                ad_set_name=r.get("ad_set_name"),
                platform_id=r["platform_id"],
                campaign_name=r.get("campaign_name"),
                spend=_float(r.get("spend")),
                impressions=_int(r.get("impressions")),
                clicks=_int(r.get("clicks")),
                conversions=_float(r.get("conversions")),
                engagements=_int(r.get("engagements")),
                video_views=_int(r.get("video_views")),
                video_completions=_int(r.get("video_completions")),
                cpm=_float_or_none(r.get("cpm")),
                cpc=_float_or_none(r.get("cpc")),
                ctr=_float_or_none(r.get("ctr")),
                vcr=_float_or_none(r.get("vcr")),
                engagement_rate=_float_or_none(r.get("engagement_rate")),
                reach=_int_or_none(r.get("reach")),
                frequency=_float_or_none(r.get("frequency")),
                reach_window=r.get("reach_window"),
                cost_per_reach=_float_or_none(r.get("cost_per_reach")),
                ad_count=_int(r.get("ad_count")),
            )
            for r in rows
        ],
        total_reach_note=note,
    )


@router.get("/{project_code}/creatives", response_model=CreativeVariantResponse)
async def get_creative_performance(
    project_code: str,
    start_date: str | None = Query(None),
    end_date: str | None = Query(None),
    days: int | None = Query(None),
    platform: str | None = Query(None),
):
    """Aggregate ad performance by normalized creative variant name across platforms."""
    start_date, end_date = _resolve_perf_dates(start_date, end_date, days)
    date_clause, date_params = _date_filter(start_date, end_date)
    plat = "AND f.platform_id = @platform" if platform else ""
    params = [bq.string_param("project_code", project_code)] + date_params
    if platform:
        params.append(bq.string_param("platform", platform))

    # Ensure alias table exists (may not have been created yet)
    try:
        bq.run_query(f"SELECT 1 FROM {bq.table('creative_variant_aliases')} LIMIT 0", [])
        alias_join = f"""
            LEFT JOIN {bq.table('creative_variant_aliases')} cva
                ON cva.project_code = @project_code
                AND (ad_agg.ad_name = cva.ad_name_pattern OR ad_agg.ad_name LIKE cva.ad_name_pattern)
                AND (cva.platform_id IS NULL OR cva.platform_id = '' OR cva.platform_id = ad_agg.platform_id)
        """
        alias_col = "cva.creative_variant"
    except (gcp_exceptions.GoogleCloudError, gcp_exceptions.NotFound) as e:
        logger.warning("Creative aliases table not found or query failed; proceeding with ads without aliases: %s", e, exc_info=True)
        alias_join = ""
        alias_col = "NULL"

    sql = f"""
        WITH ad_agg AS (
            SELECT
                f.ad_id,
                ANY_VALUE(f.ad_name) AS ad_name,
                ANY_VALUE(f.ad_set_name) AS ad_set_name,
                f.platform_id,
                SUM(f.spend) AS spend,
                SUM(f.impressions) AS impressions,
                SUM(f.clicks) AS clicks,
                SUM(f.conversions) AS conversions,
                SUM(f.engagements) AS engagements,
                SUM(f.video_views) AS video_views,
                SUM(f.video_completions) AS video_completions
            FROM {bq.table('fact_digital_daily')} f
            WHERE f.project_code = @project_code AND {date_clause} {plat}
                AND f.ad_name IS NOT NULL AND f.ad_name != ''
            GROUP BY f.ad_id, f.platform_id
        ),
        aliased AS (
            SELECT ad_agg.*,
                COALESCE(
                    {alias_col},
                    TRIM(REGEXP_REPLACE(
                        REGEXP_REPLACE(ad_agg.ad_name,
                            r'^\\d{{5}}\\s*[-_]\\s*', ''),
                        r'\\s*[-_]?\\s*\\d+x\\d+\\s*$', ''
                    ))
                ) AS creative_variant
            FROM ad_agg
            {alias_join}
        )
        SELECT
            creative_variant,
            ARRAY_AGG(DISTINCT ad_name IGNORE NULLS) AS ad_names,
            ARRAY_AGG(DISTINCT platform_id) AS platforms,
            ARRAY_AGG(DISTINCT ad_set_name IGNORE NULLS) AS ad_set_names,
            COUNT(DISTINCT ad_id) AS ad_count,
            SUM(spend) AS spend,
            SUM(impressions) AS impressions,
            SUM(clicks) AS clicks,
            SUM(conversions) AS conversions,
            SUM(engagements) AS engagements,
            SUM(video_views) AS video_views,
            SUM(video_completions) AS video_completions,
            SAFE_DIVIDE(SUM(spend), NULLIF(SUM(impressions), 0)) * 1000 AS cpm,
            SAFE_DIVIDE(SUM(spend), NULLIF(SUM(clicks), 0)) AS cpc,
            SAFE_DIVIDE(SUM(clicks), NULLIF(SUM(impressions), 0)) AS ctr,
            SAFE_DIVIDE(SUM(video_completions), NULLIF(SUM(video_views), 0)) AS vcr,
            SAFE_DIVIDE(SUM(engagements), NULLIF(SUM(impressions), 0)) AS engagement_rate
        FROM aliased
        GROUP BY creative_variant
        ORDER BY spend DESC
    """
    rows = bq.run_query(sql, params)
    return CreativeVariantResponse(
        project_code=project_code,
        start_date=date.fromisoformat(start_date) if start_date else None,
        end_date=date.fromisoformat(end_date) if end_date else None,
        creatives=[
            CreativeVariantRow(
                creative_variant=r.get("creative_variant") or "Unknown",
                ad_names=list(r.get("ad_names") or []),
                platforms=list(r.get("platforms") or []),
                ad_set_names=list(r.get("ad_set_names") or []),
                ad_count=_int(r.get("ad_count")),
                spend=_float(r.get("spend")),
                impressions=_int(r.get("impressions")),
                clicks=_int(r.get("clicks")),
                conversions=_float(r.get("conversions")),
                engagements=_int(r.get("engagements")),
                video_views=_int(r.get("video_views")),
                video_completions=_int(r.get("video_completions")),
                cpm=_float_or_none(r.get("cpm")),
                cpc=_float_or_none(r.get("cpc")),
                ctr=_float_or_none(r.get("ctr")),
                vcr=_float_or_none(r.get("vcr")),
                engagement_rate=_float_or_none(r.get("engagement_rate")),
            )
            for r in rows
        ],
    )


@router.get("/{project_code}/ads", response_model=AdPerformanceResponse)
async def get_ad_performance(
    project_code: str,
    start_date: str | None = Query(None),
    end_date: str | None = Query(None),
    days: int | None = Query(None),
    platform: str | None = Query(None),
):
    start_date, end_date = _resolve_perf_dates(start_date, end_date, days)
    date_clause, date_params = _date_filter(start_date, end_date)
    plat = "AND f.platform_id = @platform" if platform else ""
    params = [bq.string_param("project_code", project_code)] + date_params
    if platform:
        params.append(bq.string_param("platform", platform))

    sql = f"""
        SELECT
            f.ad_id,
            ANY_VALUE(f.ad_name) AS ad_name,
            ANY_VALUE(f.ad_set_name) AS ad_set_name,
            f.platform_id,
            ANY_VALUE(f.campaign_name) AS campaign_name,
            SUM(f.spend) AS spend,
            SUM(f.impressions) AS impressions,
            SUM(f.clicks) AS clicks,
            SUM(f.conversions) AS conversions,
            SUM(f.engagements) AS engagements,
            SUM(f.video_views) AS video_views,
            SUM(f.video_completions) AS video_completions,
            SAFE_DIVIDE(SUM(f.spend), NULLIF(SUM(f.impressions), 0)) * 1000 AS cpm,
            SAFE_DIVIDE(SUM(f.spend), NULLIF(SUM(f.clicks), 0)) AS cpc,
            SAFE_DIVIDE(SUM(f.clicks), NULLIF(SUM(f.impressions), 0)) AS ctr,
            SAFE_DIVIDE(SUM(f.video_completions), NULLIF(SUM(f.video_views), 0)) AS vcr,
            SAFE_DIVIDE(SUM(f.engagements), NULLIF(SUM(f.impressions), 0)) AS engagement_rate
        FROM {bq.table('fact_digital_daily')} f
        WHERE f.project_code = @project_code AND {date_clause} {plat}
        GROUP BY f.ad_id, f.platform_id
        ORDER BY engagement_rate DESC NULLS LAST
    """
    rows = bq.run_query(sql, params)
    return AdPerformanceResponse(
        project_code=project_code,
        start_date=date.fromisoformat(start_date) if start_date else None,
        end_date=date.fromisoformat(end_date) if end_date else None,
        ads=[
            AdRow(
                ad_id=r.get("ad_id"),
                ad_name=r.get("ad_name"),
                ad_set_name=r.get("ad_set_name"),
                platform_id=r["platform_id"],
                campaign_name=r.get("campaign_name"),
                spend=_float(r.get("spend")),
                impressions=_int(r.get("impressions")),
                clicks=_int(r.get("clicks")),
                conversions=_float(r.get("conversions")),
                engagements=_int(r.get("engagements")),
                video_views=_int(r.get("video_views")),
                video_completions=_int(r.get("video_completions")),
                cpm=_float_or_none(r.get("cpm")),
                cpc=_float_or_none(r.get("cpc")),
                ctr=_float_or_none(r.get("ctr")),
                vcr=_float_or_none(r.get("vcr")),
                engagement_rate=_float_or_none(r.get("engagement_rate")),
            )
            for r in rows
        ],
    )


@router.get("/{project_code}", response_model=PerformanceResponse)
async def get_performance(
    project_code: str,
    start_date: str | None = Query(None, description="YYYY-MM-DD"),
    end_date: str | None = Query(None, description="YYYY-MM-DD"),
    days: int | None = Query(None, description="Shorthand: last N days"),
    platform: str | None = Query(None, description="Filter to single platform_id"),
):
    if days and not start_date:
        end_date = end_date or date.today().isoformat()
        start_date = (date.fromisoformat(end_date) - timedelta(days=days)).isoformat()

    date_clause, date_params = _date_filter(start_date, end_date)
    platform_clause = "AND f.platform_id = @platform" if platform else ""
    base_params = [bq.string_param("project_code", project_code)] + date_params
    if platform:
        base_params.append(bq.string_param("platform", platform))

    base_where = f"f.project_code = @project_code AND {date_clause} {platform_clause}"

    # ── totals ──────────────────────────────────────────────────────
    totals_sql = f"""
        SELECT
            MIN(f.date) AS min_date,
            MAX(f.date) AS max_date,
            COALESCE(SUM(f.spend), 0) AS total_spend,
            COALESCE(SUM(f.impressions), 0) AS total_impressions,
            COALESCE(SUM(f.clicks), 0) AS total_clicks,
            COALESCE(SUM(f.conversions), 0) AS total_conversions,
            MAX(f.reach) AS total_reach,
            AVG(NULLIF(f.frequency, 0)) AS total_frequency,
            SUM(f.video_views) AS total_video_views,
            SUM(f.video_completions) AS total_video_completions,
            SAFE_DIVIDE(SUM(f.video_completions), NULLIF(SUM(f.video_views), 0)) AS total_vcr,
            SUM(f.engagements) AS total_engagements,
            SAFE_DIVIDE(SUM(f.spend), NULLIF(SUM(f.conversions), 0)) AS total_cpa,
            SAFE_DIVIDE(SUM(f.conversions), NULLIF(SUM(f.clicks), 0)) AS total_conversion_rate
        FROM {bq.table('fact_digital_daily')} f
        WHERE {base_where}
    """
    totals = bq.run_query(totals_sql, base_params)
    if not totals or totals[0]["min_date"] is None:
        raise HTTPException(
            404,
            f"No performance data found for project {project_code}",
        )
    t = totals[0]

    # ── daily aggregation ───────────────────────────────────────────
    daily_sql = f"""
        SELECT
            f.date,
            SUM(f.spend) AS spend,
            SUM(f.impressions) AS impressions,
            SUM(f.clicks) AS clicks,
            SUM(f.conversions) AS conversions,
            SAFE_DIVIDE(SUM(f.spend), NULLIF(SUM(f.impressions), 0)) * 1000 AS cpm,
            SAFE_DIVIDE(SUM(f.spend), NULLIF(SUM(f.clicks), 0)) AS cpc,
            SAFE_DIVIDE(SUM(f.clicks), NULLIF(SUM(f.impressions), 0)) AS ctr,
            MAX(f.reach) AS reach,
            AVG(NULLIF(f.frequency, 0)) AS frequency,
            SUM(f.video_views) AS video_views,
            SUM(f.video_completions) AS video_completions,
            SAFE_DIVIDE(SUM(f.video_completions), NULLIF(SUM(f.video_views), 0)) AS vcr,
            SUM(f.engagements) AS engagements,
            SAFE_DIVIDE(SUM(f.spend), NULLIF(SUM(f.conversions), 0)) AS cpa,
            SAFE_DIVIDE(SUM(f.conversions), NULLIF(SUM(f.clicks), 0)) AS conversion_rate
        FROM {bq.table('fact_digital_daily')} f
        WHERE {base_where}
        GROUP BY f.date
        ORDER BY f.date
    """
    daily_rows = bq.run_query(daily_sql, base_params)

    adset_by_date: dict[str, dict] = {}
    total_reach_adset: int | None = None
    avg_frequency_adset: float | None = None
    reach_platforms: list[str] = []
    reach_note: str | None = None
    high_frequency_warning: str | None = None
    try:
        ap_ad = [bq.string_param("project_code", project_code)] + date_params
        adset_daily_sql = f"""
            SELECT date, MAX(reach) AS reach, MAX(frequency) AS frequency
            FROM {bq.table('fact_adset_daily')}
            WHERE project_code = @project_code AND {date_clause}
            GROUP BY date
        """
        for ar in bq.run_query(adset_daily_sql, ap_ad):
            dk = ar["date"].isoformat() if hasattr(ar["date"], "isoformat") else str(ar["date"])
            adset_by_date[dk] = ar
        sum_sql = f"""
            SELECT MAX(reach) AS max_reach, AVG(frequency) AS avg_freq
            FROM {bq.table('fact_adset_daily')}
            WHERE project_code = @project_code AND {date_clause}
        """
        sr = bq.run_query(sum_sql, ap_ad)
        if sr:
            total_reach_adset = _int_or_none(sr[0].get("max_reach"))
            avg_frequency_adset = _float_or_none(sr[0].get("avg_freq"))
        plat_sql = f"""
            SELECT DISTINCT platform_id
            FROM {bq.table('fact_adset_daily')}
            WHERE project_code = @project_code AND {date_clause}
              AND reach IS NOT NULL AND reach > 0
        """
        reach_platforms = sorted({r["platform_id"] for r in bq.run_query(plat_sql, ap_ad)})
        if reach_platforms:
            reach_note = "Reach from " + ", ".join(
                PLATFORM_NAMES.get(p, p) for p in reach_platforms
            ) + "."
        warn_sql = f"""
            SELECT ad_set_name, platform_id, MAX(frequency) AS frequency
            FROM {bq.table('fact_adset_daily')}
            WHERE project_code = @project_code AND {date_clause} AND frequency > 5
            GROUP BY ad_set_name, platform_id
            ORDER BY frequency DESC
            LIMIT 1
        """
        wr = bq.run_query(warn_sql, ap_ad)
        if wr:
            w = wr[0]
            pn = PLATFORM_NAMES.get(w["platform_id"], w["platform_id"])
            nm = w.get("ad_set_name") or "An audience"
            high_frequency_warning = (
                f"{nm} on {pn} reached {float(w['frequency']):.1f} frequency — consider refreshing creative."
            )
    except (gcp_exceptions.GoogleCloudError, gcp_exceptions.NotFound, ValueError) as e:
        logger.warning("Failed to fetch high frequency data: %s", e, exc_info=True)

    # ── platform breakdown ──────────────────────────────────────────
    platform_sql = f"""
        SELECT
            f.platform_id,
            SUM(f.spend) AS spend,
            SUM(f.impressions) AS impressions,
            SUM(f.clicks) AS clicks,
            SUM(f.conversions) AS conversions,
            MAX(f.reach) AS reach,
            AVG(NULLIF(f.frequency, 0)) AS frequency,
            SUM(f.video_views) AS video_views,
            SUM(f.video_completions) AS video_completions,
            SUM(f.engagements) AS engagements
        FROM {bq.table('fact_digital_daily')} f
        WHERE {base_where}
        GROUP BY f.platform_id
        ORDER BY spend DESC
    """
    platform_rows = bq.run_query(platform_sql, base_params)

    # ── campaign-level detail ───────────────────────────────────────
    campaign_sql = f"""
        SELECT
            f.campaign_id,
            f.campaign_name,
            f.platform_id,
            SUM(f.spend) AS spend,
            SUM(f.impressions) AS impressions,
            SUM(f.clicks) AS clicks,
            SUM(f.conversions) AS conversions,
            SAFE_DIVIDE(SUM(f.spend), NULLIF(SUM(f.impressions), 0)) * 1000 AS cpm,
            SAFE_DIVIDE(SUM(f.spend), NULLIF(SUM(f.clicks), 0)) AS cpc,
            SAFE_DIVIDE(SUM(f.clicks), NULLIF(SUM(f.impressions), 0)) AS ctr,
            MAX(f.reach) AS reach,
            AVG(NULLIF(f.frequency, 0)) AS frequency,
            SUM(f.video_views) AS video_views,
            SUM(f.video_completions) AS video_completions,
            SAFE_DIVIDE(SUM(f.video_completions), NULLIF(SUM(f.video_views), 0)) AS vcr,
            SUM(f.engagements) AS engagements,
            SAFE_DIVIDE(SUM(f.spend), NULLIF(SUM(f.conversions), 0)) AS cpa,
            SAFE_DIVIDE(SUM(f.conversions), NULLIF(SUM(f.clicks), 0)) AS conversion_rate
        FROM {bq.table('fact_digital_daily')} f
        WHERE {base_where}
        GROUP BY f.campaign_id, f.campaign_name, f.platform_id
        ORDER BY spend DESC
    """
    campaign_rows = bq.run_query(campaign_sql, base_params)

    # ── objective classification ────────────────────────────────────
    media_plan_objectives = _load_media_plan_objectives(project_code)
    campaign_objectives: list[str] = []
    for r in campaign_rows:
        pid = r.get("platform_id", "")
        mp_objs = media_plan_objectives.get(pid, [])
        if mp_objs:
            # Classify each media-plan line objective for this platform,
            # then pick the most specific one for this campaign.
            line_classifications = [
                classify_objective(mp_obj, r.get("campaign_name"))
                for mp_obj in mp_objs
            ]
            # If any line matches the campaign name keywords, prefer that;
            # otherwise use the first non-mixed classification.
            obj = classify_objective(None, r.get("campaign_name"))
            if obj == "mixed" and line_classifications:
                # Fall back to the platform-level consensus
                obj = classify_project(line_classifications)
        else:
            obj = classify_objective(None, r.get("campaign_name"))
        campaign_objectives.append(obj)

    project_objective = classify_project(campaign_objectives)

    # ── metric availability ─────────────────────────────────────────
    available: list[str] = ["spend", "impressions", "clicks", "cpm", "cpc", "ctr"]
    metric_platforms: dict[str, list[str]] = {}

    metric_checks = {
        "reach": lambda r: r.get("reach") and int(r["reach"]) > 0,
        "frequency": lambda r: r.get("frequency") and float(r["frequency"]) > 0,
        "video_views": lambda r: r.get("video_views") and int(r["video_views"]) > 0,
        "video_completions": lambda r: r.get("video_completions") and int(r["video_completions"]) > 0,
        "engagements": lambda r: r.get("engagements") and int(r["engagements"]) > 0,
        "conversions": lambda r: r.get("conversions") and float(r["conversions"]) > 0,
    }

    for metric_name, check_fn in metric_checks.items():
        platforms_with = [
            PLATFORM_NAMES.get(r["platform_id"], r["platform_id"])
            for r in platform_rows if check_fn(r)
        ]
        if platforms_with:
            available.append(metric_name)
            metric_platforms[metric_name] = platforms_with

    if "video_views" in available and "video_completions" in available:
        available.append("vcr")
    if "conversions" in available:
        available.extend(["cpa", "conversion_rate"])

    # ── build response ──────────────────────────────────────────────
    return PerformanceResponse(
        project_code=project_code,
        objective_type=project_objective,
        start_date=t["min_date"],
        end_date=t["max_date"],
        total_spend=_float(t["total_spend"]),
        total_impressions=_int(t["total_impressions"]),
        total_clicks=_int(t["total_clicks"]),
        total_conversions=_float(t["total_conversions"]),
        total_reach=_int_or_none(t.get("total_reach")),
        total_frequency=_float_or_none(t.get("total_frequency")),
        total_video_views=_int_or_none(t.get("total_video_views")),
        total_video_completions=_int_or_none(t.get("total_video_completions")),
        total_vcr=_float_or_none(t.get("total_vcr")),
        total_engagements=_int_or_none(t.get("total_engagements")),
        total_cpa=_float_or_none(t.get("total_cpa")),
        total_conversion_rate=_float_or_none(t.get("total_conversion_rate")),
        total_reach_adset=total_reach_adset,
        avg_frequency_adset=avg_frequency_adset,
        reach_platforms=reach_platforms,
        reach_note=reach_note,
        high_frequency_warning=high_frequency_warning,
        available_metrics=available,
        metric_platforms=metric_platforms,
        daily=[
            DailyMetric(
                date=r["date"],
                spend=_float(r["spend"]),
                impressions=_int(r["impressions"]),
                clicks=_int(r["clicks"]),
                conversions=_float(r["conversions"]),
                cpm=_float_or_none(r.get("cpm")),
                cpc=_float_or_none(r.get("cpc")),
                ctr=_float_or_none(r.get("ctr")),
                reach=_int_or_none(r.get("reach")),
                frequency=_float_or_none(r.get("frequency")),
                reach_adset=_int_or_none(
                    adset_by_date.get(
                        r["date"].isoformat() if hasattr(r["date"], "isoformat") else str(r["date"]),
                        {},
                    ).get("reach")
                ),
                frequency_adset=_float_or_none(
                    adset_by_date.get(
                        r["date"].isoformat() if hasattr(r["date"], "isoformat") else str(r["date"]),
                        {},
                    ).get("frequency")
                ),
                video_views=_int_or_none(r.get("video_views")),
                video_completions=_int_or_none(r.get("video_completions")),
                vcr=_float_or_none(r.get("vcr")),
                engagements=_int_or_none(r.get("engagements")),
                cpa=_float_or_none(r.get("cpa")),
                conversion_rate=_float_or_none(r.get("conversion_rate")),
            )
            for r in daily_rows
        ],
        by_platform=[
            PlatformBreakdown(
                platform_id=r["platform_id"],
                platform_name=PLATFORM_NAMES.get(r["platform_id"], r["platform_id"]),
                spend=_float(r["spend"]),
                impressions=_int(r["impressions"]),
                clicks=_int(r["clicks"]),
                conversions=_float(r["conversions"]),
                reach=_int_or_none(r.get("reach")),
                frequency=_float_or_none(r.get("frequency")),
                video_views=_int_or_none(r.get("video_views")),
                video_completions=_int_or_none(r.get("video_completions")),
                engagements=_int_or_none(r.get("engagements")),
            )
            for r in platform_rows
        ],
        campaigns=[
            CampaignRow(
                campaign_id=r["campaign_id"],
                campaign_name=r["campaign_name"],
                platform_id=r["platform_id"],
                objective=campaign_objectives[i] if i < len(campaign_objectives) else None,
                spend=_float(r["spend"]),
                impressions=_int(r["impressions"]),
                clicks=_int(r["clicks"]),
                conversions=_float(r["conversions"]),
                cpm=_float_or_none(r.get("cpm")),
                cpc=_float_or_none(r.get("cpc")),
                ctr=_float_or_none(r.get("ctr")),
                reach=_int_or_none(r.get("reach")),
                frequency=_float_or_none(r.get("frequency")),
                video_views=_int_or_none(r.get("video_views")),
                video_completions=_int_or_none(r.get("video_completions")),
                vcr=_float_or_none(r.get("vcr")),
                engagements=_int_or_none(r.get("engagements")),
                cpa=_float_or_none(r.get("cpa")),
                conversion_rate=_float_or_none(r.get("conversion_rate")),
            )
            for i, r in enumerate(campaign_rows)
        ],
    )
