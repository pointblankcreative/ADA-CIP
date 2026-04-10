"""GA4 URL management and web analytics endpoints.

Web analytics are read from `fact_ga4_daily` (Funnel.io → BigQuery), not GA4 BigQuery export.
"""

import logging
import uuid
from datetime import date

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from backend.services import bigquery_client as bq

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/ga4", tags=["ga4"])

_TABLE_ENSURED = False


def _ensure_table():
    """Create project_ga4_urls table if it doesn't exist."""
    global _TABLE_ENSURED
    if _TABLE_ENSURED:
        return
    try:
        ddl = f"""
            CREATE TABLE IF NOT EXISTS {bq.table('project_ga4_urls')} (
                id STRING NOT NULL,
                project_code STRING NOT NULL,
                ga4_property_id STRING NOT NULL,
                url_pattern STRING NOT NULL,
                label STRING,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
                created_by STRING
            )
        """
        bq.run_query(ddl)
        _TABLE_ENSURED = True
    except Exception:
        logger.warning("Could not ensure project_ga4_urls table", exc_info=True)


class GA4UrlCreate(BaseModel):
    ga4_property_id: str
    url_pattern: str
    label: str | None = None


class GA4UrlResponse(BaseModel):
    id: str
    project_code: str
    ga4_property_id: str
    url_pattern: str
    label: str | None = None
    created_at: str | None = None


class GA4PropertyResponse(BaseModel):
    property_id: str
    property_name: str | None = None


class GA4WebAnalytics(BaseModel):
    date: date
    sessions: int = 0
    conversions: int = 0
    bounce_rate: float | None = None
    avg_session_duration: float | None = None
    pages_per_session: float | None = None


class GA4PerformanceResponse(BaseModel):
    has_ga4: bool = False
    urls: list[GA4UrlResponse] = []
    daily: list[GA4WebAnalytics] = []
    total_sessions: int = 0
    total_conversions: int = 0
    avg_bounce_rate: float | None = None
    avg_session_duration: float | None = None


@router.get("/properties", response_model=list[GA4PropertyResponse])
async def list_ga4_properties():
    """List GA4 properties that appear in fact_ga4_daily (Funnel-fed warehouse data)."""
    try:
        sql = f"""
            SELECT
                ga4_property_id AS property_id,
                ANY_VALUE(property_name) AS property_name
            FROM {bq.table('fact_ga4_daily')}
            GROUP BY ga4_property_id
            ORDER BY property_name NULLS LAST, property_id
        """
        rows = bq.run_query(sql)
        return [
            GA4PropertyResponse(
                property_id=r["property_id"],
                property_name=r.get("property_name"),
            )
            for r in rows
        ]
    except Exception:
        logger.warning("list_ga4_properties failed", exc_info=True)
        return []


@router.get("/{project_code}/urls", response_model=list[GA4UrlResponse])
async def list_ga4_urls(project_code: str):
    _ensure_table()
    try:
        sql = f"""
            SELECT id, project_code, ga4_property_id, url_pattern, label,
                   CAST(created_at AS STRING) AS created_at
            FROM {bq.table('project_ga4_urls')}
            WHERE project_code = @project_code
            ORDER BY created_at
        """
        rows = bq.run_query(sql, [bq.string_param("project_code", project_code)])
        return [GA4UrlResponse(**r) for r in rows]
    except Exception:
        logger.warning("Failed to list GA4 URLs for %s", project_code, exc_info=True)
        return []


@router.post("/{project_code}/urls", response_model=GA4UrlResponse)
async def add_ga4_url(project_code: str, body: GA4UrlCreate):
    _ensure_table()
    url_id = str(uuid.uuid4())
    try:
        sql = f"""
            INSERT INTO {bq.table('project_ga4_urls')}
                (id, project_code, ga4_property_id, url_pattern, label)
            VALUES (@id, @project_code, @ga4_property_id, @url_pattern, @label)
        """
        params = [
            bq.string_param("id", url_id),
            bq.string_param("project_code", project_code),
            bq.string_param("ga4_property_id", body.ga4_property_id),
            bq.string_param("url_pattern", body.url_pattern),
            bq.string_param("label", body.label or ""),
        ]
        bq.run_query(sql, params)
    except Exception as e:
        logger.error("Failed to add GA4 URL for %s: %s", project_code, e, exc_info=True)
        raise HTTPException(500, f"Failed to save GA4 URL: {e}")
    return GA4UrlResponse(
        id=url_id,
        project_code=project_code,
        ga4_property_id=body.ga4_property_id,
        url_pattern=body.url_pattern,
        label=body.label,
    )


@router.delete("/{project_code}/urls/{url_id}")
async def delete_ga4_url(project_code: str, url_id: str):
    _ensure_table()
    try:
        sql = f"""
            DELETE FROM {bq.table('project_ga4_urls')}
            WHERE id = @id AND project_code = @project_code
        """
        bq.run_query(sql, [
            bq.string_param("id", url_id),
            bq.string_param("project_code", project_code),
        ])
    except Exception as e:
        logger.error("Failed to delete GA4 URL %s: %s", url_id, e, exc_info=True)
        raise HTTPException(500, f"Failed to delete GA4 URL: {e}")
    return {"status": "deleted"}


def _build_ga4_url_filter_sql(url_rows: list[dict]) -> tuple[str, list]:
    """OR of (property_id + optional pattern on campaign/source/medium/property_name)."""
    parts: list[str] = []
    params: list = []
    for i, row in enumerate(url_rows):
        pid = row["ga4_property_id"]
        pattern = (row.get("url_pattern") or "").strip()
        p_pid = f"ga4_pid_{i}"
        params.append(bq.string_param(p_pid, pid))
        if not pattern:
            parts.append(f"ga4_property_id = @{p_pid}")
            continue
        like_val = pattern if "%" in pattern else f"%{pattern}%"
        p_like = f"ga4_like_{i}"
        params.append(bq.string_param(p_like, like_val))
        parts.append(f"""
            (ga4_property_id = @{p_pid} AND (
                LOWER(session_campaign) LIKE LOWER(@{p_like})
                OR LOWER(session_source) LIKE LOWER(@{p_like})
                OR LOWER(session_medium) LIKE LOWER(@{p_like})
                OR LOWER(IFNULL(property_name, '')) LIKE LOWER(@{p_like})
            ))
        """.strip())
    joined = " OR ".join(parts) if parts else "FALSE"
    return f"({joined})", params


@router.get("/{project_code}/analytics", response_model=GA4PerformanceResponse)
async def get_ga4_analytics(
    project_code: str,
    start_date: str | None = Query(None, description="YYYY-MM-DD inclusive"),
    end_date: str | None = Query(None, description="YYYY-MM-DD inclusive"),
):
    """Return GA4 web analytics from fact_ga4_daily for configured property + URL patterns."""
    _ensure_table()
    try:
        url_rows = bq.run_query(
            f"SELECT ga4_property_id, url_pattern, id, label FROM {bq.table('project_ga4_urls')} WHERE project_code = @pc",
            [bq.string_param("pc", project_code)],
        )
    except Exception:
        return GA4PerformanceResponse(has_ga4=False)

    if not url_rows:
        return GA4PerformanceResponse(has_ga4=False)

    urls = [
        GA4UrlResponse(
            id=r["id"], project_code=project_code,
            ga4_property_id=r["ga4_property_id"],
            url_pattern=r["url_pattern"], label=r.get("label"),
        )
        for r in url_rows
    ]

    url_filter_sql, params = _build_ga4_url_filter_sql(url_rows)

    date_parts: list[str] = []
    if start_date:
        date_parts.append("date >= @ga4_start_date")
        params.append(bq.date_param("ga4_start_date", date.fromisoformat(start_date)))
    if end_date:
        date_parts.append("date <= @ga4_end_date")
        params.append(bq.date_param("ga4_end_date", date.fromisoformat(end_date)))
    date_sql = " AND ".join(date_parts) if date_parts else "TRUE"

    daily_sql = f"""
        SELECT
            date,
            SUM(sessions) AS sessions,
            SUM(page_views) AS page_views,
            SUM(key_events) AS conversions
        FROM {bq.table('fact_ga4_daily')}
        WHERE {url_filter_sql}
          AND {date_sql}
        GROUP BY date
        ORDER BY date
    """

    try:
        daily_rows = bq.run_query(daily_sql, params)
    except Exception:
        logger.warning("get_ga4_analytics query failed for %s", project_code, exc_info=True)
        return GA4PerformanceResponse(has_ga4=True, urls=urls)

    total_sessions = sum(int(r.get("sessions", 0) or 0) for r in daily_rows)
    total_conversions = sum(int(round(float(r.get("conversions", 0) or 0))) for r in daily_rows)

    daily = [
        GA4WebAnalytics(
            date=r["date"],
            sessions=int(r.get("sessions", 0) or 0),
            conversions=int(round(float(r.get("conversions", 0) or 0))),
        )
        for r in daily_rows
    ]

    return GA4PerformanceResponse(
        has_ga4=True,
        urls=urls,
        daily=daily,
        total_sessions=total_sessions,
        total_conversions=total_conversions,
    )
