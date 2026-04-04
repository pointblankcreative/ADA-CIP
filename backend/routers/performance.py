from datetime import date, timedelta

from fastapi import APIRouter, HTTPException, Query

from backend.models.performance import (
    CampaignRow,
    DailyMetric,
    PerformanceResponse,
    PlatformBreakdown,
)
from backend.services import bigquery_client as bq
from backend.services.objective_classifier import classify_objective, classify_project

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


def _load_media_plan_objectives(project_code: str) -> dict[str, str]:
    """Load objective from media_plan_lines for a project, keyed by normalised platform."""
    try:
        sql = f"""
            SELECT platform_id, objective
            FROM {bq.table('media_plan_lines')}
            WHERE project_code = @project_code
              AND objective IS NOT NULL
        """
        rows = bq.run_query(sql, [bq.string_param("project_code", project_code)])
        result: dict[str, str] = {}
        for r in rows:
            pid = r.get("platform_id")
            obj = r.get("objective")
            if pid and obj:
                result[pid] = obj
        return result
    except Exception:
        return {}


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
        mp_obj = media_plan_objectives.get(pid)
        obj = classify_objective(mp_obj, r.get("campaign_name"))
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
