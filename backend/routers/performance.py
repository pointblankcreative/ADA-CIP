from datetime import date, timedelta

from fastapi import APIRouter, HTTPException, Query

from backend.models.performance import (
    CampaignRow,
    DailyMetric,
    PerformanceResponse,
    PlatformBreakdown,
)
from backend.services import bigquery_client as bq

router = APIRouter(prefix="/api/performance", tags=["performance"])


def _float(v, default=0.0) -> float:
    return float(v) if v is not None else default

def _int(v, default=0) -> int:
    return int(v) if v is not None else default

def _float_or_none(v) -> float | None:
    return float(v) if v is not None else None


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
    """Build WHERE clauses and params for optional date range."""
    clauses: list[str] = []
    params = []
    if start_date:
        clauses.append("date >= @start_date")
        params.append(bq.date_param("start_date", date.fromisoformat(start_date)))
    if end_date:
        clauses.append("date <= @end_date")
        params.append(bq.date_param("end_date", date.fromisoformat(end_date)))
    return (" AND ".join(clauses) if clauses else "1=1"), params


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
            COALESCE(SUM(f.conversions), 0) AS total_conversions
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
            SAFE_DIVIDE(SUM(f.clicks), NULLIF(SUM(f.impressions), 0)) AS ctr
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
            SUM(f.conversions) AS conversions
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
            SAFE_DIVIDE(SUM(f.clicks), NULLIF(SUM(f.impressions), 0)) AS ctr
        FROM {bq.table('fact_digital_daily')} f
        WHERE {base_where}
        GROUP BY f.campaign_id, f.campaign_name, f.platform_id
        ORDER BY spend DESC
    """
    campaign_rows = bq.run_query(campaign_sql, base_params)

    return PerformanceResponse(
        project_code=project_code,
        start_date=t["min_date"],
        end_date=t["max_date"],
        total_spend=_float(t["total_spend"]),
        total_impressions=_int(t["total_impressions"]),
        total_clicks=_int(t["total_clicks"]),
        total_conversions=_float(t["total_conversions"]),
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
            )
            for r in platform_rows
        ],
        campaigns=[
            CampaignRow(
                campaign_id=r["campaign_id"],
                campaign_name=r["campaign_name"],
                platform_id=r["platform_id"],
                spend=_float(r["spend"]),
                impressions=_int(r["impressions"]),
                clicks=_int(r["clicks"]),
                conversions=_float(r["conversions"]),
                cpm=_float_or_none(r.get("cpm")),
                cpc=_float_or_none(r.get("cpc")),
                ctr=_float_or_none(r.get("ctr")),
            )
            for r in campaign_rows
        ],
    )
