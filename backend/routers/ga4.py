"""GA4 URL management and web analytics endpoints."""

import logging
import uuid
from datetime import date

from fastapi import APIRouter, HTTPException
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
    """List available GA4 properties from BigQuery datasets."""
    try:
        client = bq.get_client()
        datasets = list(client.list_datasets())
        properties = []
        for ds in datasets:
            ds_id = ds.dataset_id
            if ds_id.startswith("analytics_"):
                prop_id = ds_id.replace("analytics_", "")
                properties.append(GA4PropertyResponse(
                    property_id=prop_id,
                    property_name=f"GA4 Property {prop_id}",
                ))
        return properties
    except Exception:
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


@router.get("/{project_code}/analytics", response_model=GA4PerformanceResponse)
async def get_ga4_analytics(
    project_code: str,
    start_date: str | None = None,
    end_date: str | None = None,
):
    """Return GA4 web analytics data for a project's configured URLs."""
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

    ga4_prop = url_rows[0]["ga4_property_id"]
    url_patterns = [r["url_pattern"] for r in url_rows]
    events_table = f"`point-blank-ada.analytics_{ga4_prop}.events_*`"

    date_clause = "1=1"
    params: list = []
    if start_date:
        date_clause += " AND event_date >= @start_date"
        params.append(bq.string_param("start_date", start_date.replace("-", "")))
    if end_date:
        date_clause += " AND event_date <= @end_date"
        params.append(bq.string_param("end_date", end_date.replace("-", "")))

    url_filter_parts = []
    for i, pattern in enumerate(url_patterns):
        pname = f"url_{i}"
        url_filter_parts.append(f"page_location LIKE @{pname}")
        params.append(bq.string_param(pname, f"%{pattern}%"))
    url_filter = " OR ".join(url_filter_parts) if url_filter_parts else "1=1"

    try:
        daily_sql = f"""
            SELECT
                PARSE_DATE('%Y%m%d', event_date) AS date,
                COUNT(DISTINCT CONCAT(user_pseudo_id, CAST(ga_session_id AS STRING))) AS sessions,
                COUNTIF(event_name = 'conversion') AS conversions,
                AVG(IF(event_name = 'session_start',
                    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'engaged_session_event'), NULL)) AS engagement,
                AVG(IF(event_name = 'session_start',
                    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'session_engaged'), NULL)) AS session_engaged
            FROM {events_table},
                UNNEST(event_params) ep
            WHERE ep.key = 'ga_session_id'
              AND ep.value.int_value IS NOT NULL
              AND ({url_filter})
              AND {date_clause}
            GROUP BY event_date
            ORDER BY event_date
        """
        daily_rows = bq.run_query(daily_sql, params if params else None)
    except Exception:
        return GA4PerformanceResponse(has_ga4=True, urls=urls)

    total_sessions = sum(int(r.get("sessions", 0) or 0) for r in daily_rows)
    total_conversions = sum(int(r.get("conversions", 0) or 0) for r in daily_rows)

    daily = [
        GA4WebAnalytics(
            date=r["date"],
            sessions=int(r.get("sessions", 0) or 0),
            conversions=int(r.get("conversions", 0) or 0),
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
