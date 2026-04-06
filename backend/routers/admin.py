import logging
import re
import uuid

from fastapi import APIRouter, HTTPException, Query

from backend.models.projects import (
    AdminProjectResponse,
    ProjectCreateRequest,
    ProjectUpdateRequest,
)
from backend.services import bigquery_client as bq
from backend.services.daily_job import run_daily_pipeline
from backend.services.media_plan_sync import sync_media_plan
from backend.services.transformation import run_transformation

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/admin", tags=["admin"])


@router.post("/run-transformation")
async def api_run_transformation(mode: str = "daily"):
    """Trigger the Funnel.io → fact_digital_daily transformation.
    mode: "daily" (last 7 days, default) or "full" (all history).
    """
    result = run_transformation(mode)
    return result


@router.post("/sync-media-plan")
async def api_sync_media_plan(
    sheet_id: str = Query(..., description="Google Sheets document ID"),
    project_code: str = Query(..., description="YYNNN project code"),
):
    """Parse a Google Sheets media plan and populate BigQuery tables."""
    result = sync_media_plan(sheet_id, project_code)
    return result


@router.post("/daily-run")
async def daily_run():
    """Trigger the full daily pipeline: transform → pacing → alerts."""
    result = run_daily_pipeline()
    return result


@router.get("/data-freshness")
async def data_freshness():
    """Check when each platform's data was last loaded."""
    sql = f"""
        SELECT
            platform_id,
            MAX(date) AS latest_data_date,
            MAX(loaded_at) AS latest_loaded_at,
            COUNT(DISTINCT date) AS total_days,
            COUNT(*) AS total_rows
        FROM {bq.table('fact_digital_daily')}
        GROUP BY platform_id
        ORDER BY platform_id
    """
    rows = bq.run_query(sql)
    return {
        "platforms": [
            {
                "platform_id": r["platform_id"],
                "latest_data_date": str(r["latest_data_date"]) if r.get("latest_data_date") else None,
                "latest_loaded_at": str(r["latest_loaded_at"]) if r.get("latest_loaded_at") else None,
                "total_days": r.get("total_days", 0),
                "total_rows": r.get("total_rows", 0),
            }
            for r in rows
        ]
    }


def _extract_sheet_id(url_or_id: str) -> str:
    """Extract a Google Sheets ID from a full URL or return as-is."""
    m = re.search(r"/spreadsheets/d/([a-zA-Z0-9_-]+)", url_or_id)
    return m.group(1) if m else url_or_id


@router.get("/projects", response_model=list[AdminProjectResponse])
async def admin_list_projects():
    """List all projects with admin-level detail."""
    sql = f"""
        SELECT
            p.project_code,
            p.project_name,
            p.client_id,
            c.client_name,
            p.campaign_type,
            p.status,
            p.start_date,
            p.end_date,
            p.net_budget,
            p.currency,
            p.media_plan_sheet_id,
            p.slack_channel_id,
            COALESCE(s.total_spend, 0) AS total_spend,
            DATE_DIFF(p.end_date, CURRENT_DATE(), DAY) AS days_remaining,
            s.platforms_active,
            s.first_data_date,
            s.last_data_date,
            IF(mp.plan_id IS NOT NULL, TRUE, FALSE) AS media_plan_synced,
            COALESCE(al.alert_count, 0) AS alert_count,
            p.created_at,
            p.updated_at
        FROM {bq.table('dim_projects')} p
        LEFT JOIN {bq.table('dim_clients')} c USING (client_id)
        LEFT JOIN (
            SELECT project_code, SUM(spend) AS total_spend,
                   COUNT(DISTINCT platform_id) AS platforms_active,
                   MIN(date) AS first_data_date, MAX(date) AS last_data_date
            FROM {bq.table('fact_digital_daily')}
            GROUP BY project_code
        ) s USING (project_code)
        LEFT JOIN (
            SELECT project_code, plan_id
            FROM {bq.table('media_plans')}
            QUALIFY ROW_NUMBER() OVER (PARTITION BY project_code ORDER BY synced_at DESC) = 1
        ) mp USING (project_code)
        LEFT JOIN (
            SELECT project_code, COUNT(*) AS alert_count
            FROM {bq.table('alerts')}
            WHERE resolved_at IS NULL
            GROUP BY project_code
        ) al USING (project_code)
        ORDER BY p.start_date DESC
    """
    rows = bq.run_query(sql)
    return [
        AdminProjectResponse(
            project_code=r["project_code"],
            project_name=r["project_name"],
            client_id=r.get("client_id"),
            client_name=r.get("client_name"),
            campaign_type=r.get("campaign_type"),
            status=r.get("status") or "active",
            start_date=r.get("start_date"),
            end_date=r.get("end_date"),
            net_budget=float(r["net_budget"]) if r.get("net_budget") else None,
            currency=r.get("currency") or "CAD",
            total_spend=float(r.get("total_spend", 0)),
            days_remaining=r.get("days_remaining"),
            platforms_active=r.get("platforms_active") or 0,
            first_data_date=r.get("first_data_date"),
            last_data_date=r.get("last_data_date"),
            media_plan_sheet_id=r.get("media_plan_sheet_id"),
            media_plan_synced=bool(r.get("media_plan_synced")),
            slack_channel_id=r.get("slack_channel_id"),
            alert_count=r.get("alert_count") or 0,
            created_at=r.get("created_at"),
            updated_at=r.get("updated_at"),
        )
        for r in rows
    ]


@router.post("/projects")
async def admin_create_project(req: ProjectCreateRequest):
    """Create a new project + optionally sync its media plan."""
    # Upsert client
    client_id = re.sub(r"[^a-z0-9]+", "-", req.client_name.lower()).strip("-")
    bq.run_query(f"""
        MERGE {bq.table('dim_clients')} t
        USING (SELECT @client_id AS client_id) s ON t.client_id = s.client_id
        WHEN NOT MATCHED THEN
            INSERT (client_id, client_name, client_short_name)
            VALUES (@client_id, @client_name, @client_short)
    """, [
        bq.string_param("client_id", client_id),
        bq.string_param("client_name", req.client_name),
        bq.string_param("client_short", client_id),
    ])

    # Extract sheet_id from URL if provided
    sheet_id = _extract_sheet_id(req.media_plan_sheet_url) if req.media_plan_sheet_url else None

    # Upsert project
    bq.run_query(f"""
        MERGE {bq.table('dim_projects')} t
        USING (SELECT @pcode AS project_code) s ON t.project_code = s.project_code
        WHEN MATCHED THEN UPDATE SET
            project_name = @pname,
            client_id = @client_id,
            start_date = @start_date,
            end_date = @end_date,
            net_budget = @budget,
            media_plan_sheet_id = @sheet_id,
            slack_channel_id = @slack,
            status = 'active',
            updated_at = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN INSERT
            (project_code, project_name, client_id, start_date, end_date,
             net_budget, media_plan_sheet_id, slack_channel_id, status)
            VALUES (@pcode, @pname, @client_id, @start_date, @end_date,
                    @budget, @sheet_id, @slack, 'active')
    """, [
        bq.string_param("pcode", req.project_code),
        bq.string_param("pname", req.project_name),
        bq.string_param("client_id", client_id),
        bq.date_param("start_date", req.start_date),
        bq.date_param("end_date", req.end_date),
        bq.scalar_param("budget", "NUMERIC", req.net_budget),
        bq.string_param("sheet_id", sheet_id or ""),
        bq.string_param("slack", req.slack_channel_id or ""),
    ])

    # Optionally sync media plan
    if sheet_id:
        try:
            sync_result = sync_media_plan(sheet_id, req.project_code)
            sync_result.setdefault("status", "success")
        except Exception as e:
            logger.warning("Media plan sync failed for %s: %s", req.project_code, e)
            sync_result = {"status": "error", "message": str(e)}
    else:
        sync_result = {"status": "skipped"}

    return {
        "status": "created",
        "project_code": req.project_code,
        "client_id": client_id,
        "media_plan_sync": sync_result,
    }


@router.put("/projects/{project_code}")
async def admin_update_project(project_code: str, req: ProjectUpdateRequest):
    """Update an existing project's settings."""
    # Verify project exists
    existing = bq.run_query(f"""
        SELECT project_code FROM {bq.table('dim_projects')}
        WHERE project_code = @pcode
    """, [bq.string_param("pcode", project_code)])
    if not existing:
        raise HTTPException(404, f"Project {project_code} not found")

    # Build dynamic SET clauses from non-None fields
    updates = []
    params = [bq.string_param("pcode", project_code)]

    if req.project_name is not None:
        updates.append("project_name = @pname")
        params.append(bq.string_param("pname", req.project_name))
    if req.start_date is not None:
        updates.append("start_date = @start_date")
        params.append(bq.date_param("start_date", req.start_date))
    if req.end_date is not None:
        updates.append("end_date = @end_date")
        params.append(bq.date_param("end_date", req.end_date))
    if req.net_budget is not None:
        updates.append("net_budget = @budget")
        params.append(bq.scalar_param("budget", "NUMERIC", req.net_budget))
    if req.status is not None:
        updates.append("status = @status")
        params.append(bq.string_param("status", req.status))
    if req.slack_channel_id is not None:
        updates.append("slack_channel_id = @slack")
        params.append(bq.string_param("slack", req.slack_channel_id))
    if req.media_plan_sheet_url is not None:
        sheet_id = _extract_sheet_id(req.media_plan_sheet_url)
        updates.append("media_plan_sheet_id = @sheet_id")
        params.append(bq.string_param("sheet_id", sheet_id))

    if not updates:
        raise HTTPException(400, "No fields to update")

    updates.append("updated_at = CURRENT_TIMESTAMP()")
    set_clause = ", ".join(updates)

    bq.run_query(f"""
        UPDATE {bq.table('dim_projects')}
        SET {set_clause}
        WHERE project_code = @pcode
    """, params)

    # Trigger media plan sync if sheet URL was updated
    if req.media_plan_sheet_url:
        sheet_id = _extract_sheet_id(req.media_plan_sheet_url)
        try:
            sync_result = sync_media_plan(sheet_id, project_code)
            sync_result.setdefault("status", "success")
        except Exception as e:
            logger.warning("Media plan sync failed for %s: %s", project_code, e)
            sync_result = {"status": "error", "message": str(e)}
    else:
        sync_result = None

    return {
        "status": "updated",
        "project_code": project_code,
        "fields_updated": [u.split(" =")[0].strip() for u in updates if "updated_at" not in u],
        "media_plan_sync": sync_result,
    }


@router.get("/ingestion-log")
async def get_ingestion_log(limit: int = Query(20, ge=1, le=100)):
    """Return recent ingestion/transformation runs."""
    sql = f"""
        SELECT *
        FROM {bq.table('ingestion_log')}
        ORDER BY started_at DESC
        LIMIT @limit
    """
    rows = bq.run_query(sql, [bq.scalar_param("limit", "INT64", limit)])
    return {"runs": [dict(r) for r in rows]}


@router.put("/media-plan-lines/{line_id}")
async def update_media_plan_line(line_id: str, body: dict):
    """Update a media plan line's display name (audience_name)."""
    audience_name = body.get("audience_name")
    if audience_name is None:
        raise HTTPException(400, "audience_name is required")

    sql = f"""
        UPDATE {bq.table('media_plan_lines')}
        SET audience_name = @audience_name
        WHERE line_id = @line_id
    """
    bq.run_query(sql, [
        bq.string_param("audience_name", audience_name),
        bq.string_param("line_id", line_id),
    ])
    return {"status": "updated", "line_id": line_id, "audience_name": audience_name}
