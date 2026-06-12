import logging
import re
import uuid

from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel, field_validator

from backend.models.projects import (
    AdminProjectResponse,
    ProjectCreateRequest,
    ProjectUpdateRequest,
)
from backend.services import bigquery_client as bq
from backend.services.daily_job import run_daily_pipeline
from backend.services.media_plan_sync import (
    sync_all_for_project,
    sync_media_plan,
)
from backend.services.transformation import run_transformation
from ingestion.transformation.adset_transform import run_adset_transformation


def _ensure_plan_registered(project_code: str, sheet_id: str) -> None:
    """Make sure (project_code, sheet_id) exists in project_media_plans.

    Idempotent — uses MERGE so calling it for an existing row is a no-op.
    Backstops the legacy flow where the admin API only writes
    ``dim_projects.media_plan_sheet_id``: without this, the dedup guard
    (which JOINs through project_media_plans) would silently filter the
    project's lines out of pacing/diagnostics.
    """
    if not sheet_id:
        return
    bq.run_query(
        f"""
        MERGE {bq.table('project_media_plans')} t
        USING (
            SELECT @pc AS project_code, @sheet_id AS sheet_id
        ) s
          ON t.project_code = s.project_code
         AND t.sheet_id   = s.sheet_id
        WHEN NOT MATCHED THEN
            INSERT (project_code, sheet_id, phase_label, display_order, is_active, created_at)
            VALUES (s.project_code, s.sheet_id, NULL, 1, TRUE, CURRENT_TIMESTAMP())
        """,
        [
            bq.string_param("pc", project_code),
            bq.string_param("sheet_id", sheet_id),
        ],
    )


class MediaPlanLineUpdate(BaseModel):
    """Request body for updating a media plan line's audience_name."""
    audience_name: str

    @field_validator("audience_name")
    @classmethod
    def validate_audience_name(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("audience_name must not be empty")
        if len(v) > 500:
            raise ValueError("audience_name must be 500 characters or fewer")
        return v

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/admin", tags=["admin"])


@router.post("/run-transformation")
async def api_run_transformation(mode: str = "daily"):
    """Trigger the Funnel.io → fact_digital_daily transformation.
    mode: "daily" (last 7 days, default) or "full" (all history).
    """
    result = run_transformation(mode)
    return result


@router.post("/run-adset-transformation")
async def api_run_adset_transformation(mode: str = "daily"):
    """Trigger the Funnel.io → fact_adset_daily reach/frequency transformation.
    mode: "daily" (last 7 days, default) or "full" (all history).
    """
    result = run_adset_transformation(mode)
    return result


@router.post("/sync-media-plan")
async def api_sync_media_plan(
    sheet_id: str = Query(..., description="Google Sheets document ID"),
    project_code: str = Query(..., description="YYNNN project code"),
    tab_name: str | None = Query(None, description="Override tab name. If omitted, uses the project's stored media_plan_tab_name."),
):
    """Parse a Google Sheets media plan and populate BigQuery tables."""
    # If no explicit tab_name override, fall back to the project's stored preference
    if tab_name is None:
        rows = bq.run_query(f"""
            SELECT media_plan_tab_name FROM {bq.table('dim_projects')}
            WHERE project_code = @pcode
        """, [bq.string_param("pcode", project_code)])
        if rows and rows[0].get("media_plan_tab_name"):
            tab_name = rows[0]["media_plan_tab_name"]

    # Self-heal the join table so the dedup guard sees this sheet. Mirrors
    # what the project create/update flow does so manual /sync-media-plan
    # calls don't leave the data invisible to downstream queries.
    _ensure_plan_registered(project_code, sheet_id)
    result = sync_media_plan(sheet_id=sheet_id, project_code=project_code, tab_name=tab_name)
    return result


@router.post("/daily-run")
async def daily_run():
    """Trigger the full daily pipeline: transform → pacing → alerts."""
    result = run_daily_pipeline()
    return result


@router.post("/creative-assets/sync")
async def api_creative_assets_sync(force: bool = False):
    """Trigger the Phase 19 creative-image + ad-set targeting sync
    (Meta Graph API + StackAdapt GraphQL → GCS / BigQuery).

    Also runs inside the daily pipeline (Stage 1d); this endpoint is the
    manual lever. `force=true` retries no_match / fetch_failed image
    variants past the once-per-day guard (stored images are never
    refetched). Returns per-sync counts and per-source status; no-ops
    when the platform tokens aren't configured.
    """
    from backend.services.creative_assets import run_sync

    return run_sync(force=force)


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
            p.media_plan_tab_name,
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
        -- Multi-plan support (2026-04-25): media_plan_synced is TRUE when
        -- at least one of the project's registered, active sheets has a
        -- current media_plans row. Single-sheet projects keep the same
        -- semantics; multi-sheet projects light up as soon as any phase
        -- has been synced.
        LEFT JOIN (
            SELECT mp.project_code, ANY_VALUE(mp.plan_id) AS plan_id
            FROM {bq.table('media_plans')} mp
            JOIN {bq.table('project_media_plans')} pmp
              ON mp.project_code = pmp.project_code
             AND mp.sheet_id   = pmp.sheet_id
            WHERE mp.is_current = TRUE AND pmp.is_active = TRUE
            GROUP BY mp.project_code
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
            media_plan_tab_name=r.get("media_plan_tab_name"),
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
            media_plan_tab_name = @tab_name,
            slack_channel_id = @slack,
            status = 'active',
            updated_at = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN INSERT
            (project_code, project_name, client_id, start_date, end_date,
             net_budget, media_plan_sheet_id, media_plan_tab_name, slack_channel_id, status)
            VALUES (@pcode, @pname, @client_id, @start_date, @end_date,
                    @budget, @sheet_id, @tab_name, @slack, 'active')
    """, [
        bq.string_param("pcode", req.project_code),
        bq.string_param("pname", req.project_name),
        bq.string_param("client_id", client_id),
        bq.date_param("start_date", req.start_date),
        bq.date_param("end_date", req.end_date),
        bq.scalar_param("budget", "NUMERIC", req.net_budget),
        bq.string_param("sheet_id", sheet_id or ""),
        bq.string_param("tab_name", req.media_plan_tab_name or ""),
        bq.string_param("slack", req.slack_channel_id or ""),
    ])

    # Optionally sync media plan
    if sheet_id:
        # Register the sheet in project_media_plans before sync so the
        # downstream dedup guard sees it. Without this, even a successful
        # sync produces 0 visible lines in pacing / diagnostics.
        _ensure_plan_registered(req.project_code, sheet_id)
        try:
            sync_result = sync_media_plan(sheet_id, req.project_code, tab_name=req.media_plan_tab_name or None)
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
    if req.media_plan_tab_name is not None:
        updates.append("media_plan_tab_name = @tab_name")
        params.append(bq.string_param("tab_name", req.media_plan_tab_name))

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
        _ensure_plan_registered(project_code, sheet_id)
        try:
            sync_result = sync_media_plan(sheet_id, project_code, tab_name=req.media_plan_tab_name or None)
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


# ── Multi-plan: project_media_plans CRUD ─────────────────────────────
#
# Phases of a multi-flight campaign each get their own row here. The
# /sync-all endpoint iterates them in display order and runs sync_media_plan
# per sheet — see backend/services/media_plan_sync.py::sync_all_for_project.
# Soft-delete (is_active=FALSE) is the default so retrospective replay can
# still read closed phases' data; ?hard=true is reserved for future use.


class ProjectPlanCreate(BaseModel):
    """Request body for adding a media plan sheet to a project."""
    sheet_url_or_id: str
    phase_label: str | None = None
    display_order: int | None = None
    auto_sync: bool = True

    @field_validator("phase_label")
    @classmethod
    def _strip_phase_label(cls, v: str | None) -> str | None:
        if v is None:
            return None
        v = v.strip()
        return v or None


class ProjectPlanUpdate(BaseModel):
    """Patch body for an existing project_media_plans row."""
    phase_label: str | None = None
    display_order: int | None = None
    is_active: bool | None = None


def _list_plans_for_project(project_code: str) -> list[dict]:
    """Return every project_media_plans row for a project, ordered for display.

    Includes inactive rows so the UI can show retired phases struck-through;
    the dedup guard already filters them out of pacing/diagnostics queries.
    """
    rows = bq.run_query(
        f"""
        SELECT pmp.sheet_id,
               pmp.phase_label,
               pmp.display_order,
               pmp.is_active,
               pmp.created_at,
               mp.last_synced_at,
               mp.line_count
        FROM {bq.table('project_media_plans')} pmp
        LEFT JOIN (
            SELECT mp.sheet_id,
                   mp.project_code,
                   mp.synced_at AS last_synced_at,
                   (SELECT COUNT(*)
                      FROM {bq.table('media_plan_lines')} l
                     WHERE l.plan_id = mp.plan_id) AS line_count
            FROM {bq.table('media_plans')} mp
            WHERE mp.is_current = TRUE
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY mp.project_code, mp.sheet_id
                ORDER BY mp.synced_at DESC
            ) = 1
        ) mp
          ON mp.project_code = pmp.project_code
         AND mp.sheet_id     = pmp.sheet_id
        WHERE pmp.project_code = @pc
        ORDER BY pmp.is_active DESC,
                 pmp.display_order NULLS LAST,
                 pmp.created_at ASC
        """,
        [bq.string_param("pc", project_code)],
    )
    return [
        {
            "sheet_id": r["sheet_id"],
            "phase_label": r.get("phase_label"),
            "display_order": r.get("display_order"),
            "is_active": bool(r.get("is_active", True)),
            "created_at": str(r["created_at"]) if r.get("created_at") else None,
            "last_synced_at": str(r["last_synced_at"]) if r.get("last_synced_at") else None,
            "line_count": int(r["line_count"]) if r.get("line_count") is not None else 0,
        }
        for r in rows
    ]


@router.get("/projects/{project_code}/plans")
async def admin_list_project_plans(project_code: str):
    """Return every media plan registered against ``project_code``."""
    return {"project_code": project_code, "plans": _list_plans_for_project(project_code)}


@router.post("/projects/{project_code}/plans")
async def admin_add_project_plan(project_code: str, body: ProjectPlanCreate):
    """Add a media plan sheet to a project. Optionally syncs it immediately."""
    sheet_id = _extract_sheet_id(body.sheet_url_or_id)
    if not sheet_id:
        raise HTTPException(status_code=400, detail="Could not parse sheet ID from input")

    # If display_order is not provided, append after the current max.
    display_order = body.display_order
    if display_order is None:
        rows = bq.run_query(
            f"""
            SELECT COALESCE(MAX(display_order), 0) + 1 AS next_order
            FROM {bq.table('project_media_plans')}
            WHERE project_code = @pc
            """,
            [bq.string_param("pc", project_code)],
        )
        display_order = int(rows[0]["next_order"]) if rows else 1

    bq.run_query(
        f"""
        MERGE {bq.table('project_media_plans')} t
        USING (
            SELECT @pc AS project_code, @sheet_id AS sheet_id
        ) s
          ON t.project_code = s.project_code
         AND t.sheet_id   = s.sheet_id
        WHEN MATCHED THEN UPDATE SET
            phase_label    = @phase_label,
            display_order  = @display_order,
            is_active      = TRUE
        WHEN NOT MATCHED THEN
            INSERT (project_code, sheet_id, phase_label, display_order, is_active, created_at)
            VALUES (s.project_code, s.sheet_id, @phase_label, @display_order, TRUE, CURRENT_TIMESTAMP())
        """,
        [
            bq.string_param("pc", project_code),
            bq.string_param("sheet_id", sheet_id),
            bq.string_param("phase_label", body.phase_label or ""),
            bq.scalar_param("display_order", "INT64", display_order),
        ],
    )
    # phase_label="" is what we pass when the user sent NULL — normalise back
    # to NULL in storage so the SELECT path returns a clean Python None.
    if not body.phase_label:
        bq.run_query(
            f"""
            UPDATE {bq.table('project_media_plans')}
            SET phase_label = NULL
            WHERE project_code = @pc AND sheet_id = @sheet_id
            """,
            [bq.string_param("pc", project_code), bq.string_param("sheet_id", sheet_id)],
        )

    sync_result: dict | None = None
    if body.auto_sync:
        try:
            sync_result = sync_media_plan(sheet_id=sheet_id, project_code=project_code)
            sync_result.setdefault("status", "success")
        except Exception as e:
            logger.warning("Auto-sync after add-plan failed for %s/%s: %s", project_code, sheet_id, e)
            sync_result = {"status": "error", "message": str(e)}

    return {
        "status": "added",
        "project_code": project_code,
        "sheet_id": sheet_id,
        "phase_label": body.phase_label,
        "display_order": display_order,
        "sync_result": sync_result,
        "plans": _list_plans_for_project(project_code),
    }


@router.put("/projects/{project_code}/plans/{sheet_id}")
async def admin_update_project_plan(
    project_code: str,
    sheet_id: str,
    body: ProjectPlanUpdate,
):
    """Patch a project_media_plans row (phase_label, display_order, is_active)."""
    sets: list[str] = []
    params: list = [
        bq.string_param("pc", project_code),
        bq.string_param("sheet_id", sheet_id),
    ]
    if body.phase_label is not None:
        sets.append("phase_label = @phase_label")
        params.append(bq.string_param("phase_label", body.phase_label.strip()))
    if body.display_order is not None:
        sets.append("display_order = @display_order")
        params.append(bq.scalar_param("display_order", "INT64", body.display_order))
    if body.is_active is not None:
        sets.append("is_active = @is_active")
        params.append(bq.scalar_param("is_active", "BOOL", body.is_active))

    if not sets:
        raise HTTPException(status_code=400, detail="No fields to update")

    bq.run_query(
        f"""
        UPDATE {bq.table('project_media_plans')}
        SET {', '.join(sets)}
        WHERE project_code = @pc AND sheet_id = @sheet_id
        """,
        params,
    )
    return {
        "status": "updated",
        "project_code": project_code,
        "sheet_id": sheet_id,
        "plans": _list_plans_for_project(project_code),
    }


@router.delete("/projects/{project_code}/plans/{sheet_id}")
async def admin_remove_project_plan(
    project_code: str,
    sheet_id: str,
    hard: bool = Query(False, description="If true, hard-delete the row (loses retrospective access). Default soft-deletes via is_active=FALSE."),
):
    """Soft-delete a plan (is_active=FALSE) by default; ``?hard=true`` removes the row.

    Soft delete is the default because retrospective replay re-runs pacing
    against historical media_plan_lines, which would no longer be reachable
    if the join table row disappears. Hard delete is reserved for cleaning
    up rows added by mistake.
    """
    if hard:
        bq.run_query(
            f"""
            DELETE FROM {bq.table('project_media_plans')}
            WHERE project_code = @pc AND sheet_id = @sheet_id
            """,
            [bq.string_param("pc", project_code), bq.string_param("sheet_id", sheet_id)],
        )
        return {
            "status": "deleted",
            "project_code": project_code,
            "sheet_id": sheet_id,
            "plans": _list_plans_for_project(project_code),
        }

    bq.run_query(
        f"""
        UPDATE {bq.table('project_media_plans')}
        SET is_active = FALSE
        WHERE project_code = @pc AND sheet_id = @sheet_id
        """,
        [bq.string_param("pc", project_code), bq.string_param("sheet_id", sheet_id)],
    )
    return {
        "status": "retired",
        "project_code": project_code,
        "sheet_id": sheet_id,
        "plans": _list_plans_for_project(project_code),
    }


@router.post("/projects/{project_code}/sync-all")
async def admin_sync_all_plans(project_code: str):
    """Sync every active media plan for ``project_code`` in display order."""
    return sync_all_for_project(project_code)


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
async def update_media_plan_line(line_id: str, body: MediaPlanLineUpdate):
    """Update a media plan line's display name (audience_name).

    Also persists the override to media_plan_line_overrides so it survives
    re-syncs (the override is keyed on project_code + platform_id + budget).
    """
    # Update the current line in media_plan_lines
    sql = f"""
        UPDATE {bq.table('media_plan_lines')}
        SET audience_name = @audience_name
        WHERE line_id = @line_id
    """
    bq.run_query(sql, [
        bq.string_param("audience_name", body.audience_name),
        bq.string_param("line_id", line_id),
    ])

    # Fetch the line's stable key fields to persist the override
    line_row = bq.run_query(f"""
        SELECT project_code, platform_id, budget
        FROM {bq.table('media_plan_lines')}
        WHERE line_id = @line_id
    """, [bq.string_param("line_id", line_id)])

    if line_row:
        row = line_row[0]
        # Upsert into overrides table (keyed on project_code + platform_id + budget ±1%)
        bq.run_query(f"""
            MERGE {bq.table('media_plan_line_overrides')} t
            USING (SELECT @pc AS project_code, @pid AS platform_id, @budget AS budget) s
            ON t.project_code = s.project_code
               AND t.platform_id = s.platform_id
               AND ABS(t.budget - s.budget) / GREATEST(s.budget, 1) < 0.01
            WHEN MATCHED THEN UPDATE SET
                audience_name = @audience_name,
                updated_at = CURRENT_TIMESTAMP()
            WHEN NOT MATCHED THEN INSERT
                (project_code, platform_id, budget, audience_name, updated_at)
                VALUES (@pc, @pid, @budget, @audience_name, CURRENT_TIMESTAMP())
        """, [
            bq.string_param("pc", row["project_code"]),
            bq.string_param("pid", row.get("platform_id") or ""),
            bq.scalar_param("budget", "FLOAT64", float(row.get("budget") or 0)),
            bq.string_param("audience_name", body.audience_name),
        ])

    return {"status": "updated", "line_id": line_id, "audience_name": body.audience_name}


# ── Bundle override endpoints (ADAC-54 follow-up) ────────────────────
#
# Two endpoints, one for each user action surfaced in the Pacing tab's
# bundle badge:
#
#   POST   /api/admin/bundles/{bundle_id}/confirm   — lock the parser's
#       bundle suggestion in. Writes 'confirmed_parent' to the override
#       table and updates the live media_plan_lines rows so the change is
#       visible without waiting for the next sync.
#
#   DELETE /api/admin/bundles/{bundle_id}/override — clear any override.
#       The next sync re-decides based on the current spreadsheet. Today's
#       live rows are reverted to 'suggested_*' so the UI reflects the
#       reset immediately.
#
# Reject was deferred: the parser zeros out children's budgets when it
# detects a bundle, so "treat as standalone" can't be reconstructed
# without re-syncing. The natural workflow for that case is to un-merge
# Budget cells in the source sheet and re-sync. See Asana follow-up.
#
# Permissions: matches the audience_name endpoint above — no application-
# level role check, IAP at Cloud Run is the gate. A future "proper RBAC"
# pass should hit all admin endpoints together rather than picking off
# one at a time.


def _resolve_user_email(request: Request) -> str | None:
    """Pull the IAP-attached user email if available; None for the dev stub."""
    user = getattr(request.state, "user", None) or {}
    return user.get("email") if isinstance(user, dict) else None


def _verify_bundle_exists(project_code: str, bundle_id: str) -> int:
    """Return the count of media_plan_lines rows in this bundle, raising
    404 if zero. Catches typos in the URL and prevents an MERGE that
    would write an override referencing nothing."""
    rows = bq.run_query(
        f"""
        SELECT COUNT(*) AS n
        FROM {bq.table('media_plan_lines')}
        WHERE project_code = @pc
          AND bundle_id = @bid
        """,
        [
            bq.string_param("pc", project_code),
            bq.string_param("bid", bundle_id),
        ],
    )
    n = int(rows[0]["n"]) if rows else 0
    if n == 0:
        raise HTTPException(
            404,
            f"No media plan lines found for bundle {bundle_id} in project {project_code}",
        )
    return n


@router.post("/bundles/{bundle_id}/confirm")
async def confirm_bundle(
    bundle_id: str,
    request: Request,
    project_code: str = Query(..., description="The project owning this bundle"),
):
    """Lock in the parser's bundle suggestion as user-confirmed.

    Writes (project_code, bundle_id, 'confirmed_parent') to the override
    table and immediately updates the live ``media_plan_lines`` rows so
    the user sees the result without waiting for a sync. The override
    survives subsequent syncs — see ``_apply_bundle_overrides`` in
    ``services/media_plan_sync.py``.
    """
    member_count = _verify_bundle_exists(project_code, bundle_id)
    updated_by = _resolve_user_email(request)

    # MERGE into the override table — single source of truth across syncs.
    bq.run_query(
        f"""
        MERGE {bq.table('media_plan_bundle_overrides')} t
        USING (
            SELECT
                @pc AS project_code,
                @bid AS bundle_id,
                'confirmed_parent' AS bundle_role,
                CURRENT_TIMESTAMP() AS updated_at,
                @updated_by AS updated_by
        ) s
        ON t.project_code = s.project_code AND t.bundle_id = s.bundle_id
        WHEN MATCHED THEN UPDATE SET
            bundle_role = s.bundle_role,
            updated_at = s.updated_at,
            updated_by = s.updated_by
        WHEN NOT MATCHED THEN INSERT
            (project_code, bundle_id, bundle_role, updated_at, updated_by)
            VALUES (s.project_code, s.bundle_id, s.bundle_role, s.updated_at, s.updated_by)
        """,
        [
            bq.string_param("pc", project_code),
            bq.string_param("bid", bundle_id),
            bq.string_param("updated_by", updated_by or ""),
        ],
    )

    # Mirror the apply logic from media_plan_sync._apply_bundle_overrides:
    # parents have non-NULL budget (pool total), children have NULL.
    bq.run_query(
        f"""
        UPDATE {bq.table('media_plan_lines')}
        SET bundle_role = CASE
              WHEN budget IS NULL THEN 'confirmed_child'
              ELSE 'confirmed_parent'
            END
        WHERE project_code = @pc AND bundle_id = @bid
        """,
        [
            bq.string_param("pc", project_code),
            bq.string_param("bid", bundle_id),
        ],
    )

    return {
        "status": "confirmed",
        "project_code": project_code,
        "bundle_id": bundle_id,
        "members_updated": member_count,
    }


@router.delete("/bundles/{bundle_id}/override")
async def clear_bundle_override(
    bundle_id: str,
    project_code: str = Query(..., description="The project owning this bundle"),
):
    """Clear any saved override for this bundle.

    The next sync will re-decide whether this is a bundle based on the
    current spreadsheet (re-reads merged Budget cells). Live
    ``media_plan_lines`` rows are reverted to ``suggested_*`` immediately
    so the UI reflects the reset without a re-sync. If the user wants the
    parser's suggestion to permanently change, the underlying source
    sheet is the right place to edit.
    """
    _verify_bundle_exists(project_code, bundle_id)

    # Drop the override row (no-op if it never existed).
    bq.run_query(
        f"""
        DELETE FROM {bq.table('media_plan_bundle_overrides')}
        WHERE project_code = @pc AND bundle_id = @bid
        """,
        [
            bq.string_param("pc", project_code),
            bq.string_param("bid", bundle_id),
        ],
    )

    # Revert live lines to the parser's "suggested" state. Same parent/child
    # split rule as the apply path.
    bq.run_query(
        f"""
        UPDATE {bq.table('media_plan_lines')}
        SET bundle_role = CASE
              WHEN budget IS NULL THEN 'suggested_child'
              ELSE 'suggested_parent'
            END
        WHERE project_code = @pc AND bundle_id = @bid
        """,
        [
            bq.string_param("pc", project_code),
            bq.string_param("bid", bundle_id),
        ],
    )

    return {
        "status": "cleared",
        "project_code": project_code,
        "bundle_id": bundle_id,
    }


@router.post("/bundles/{bundle_id}/reject")
async def reject_bundle(
    bundle_id: str,
    request: Request,
    project_code: str = Query(..., description="The project owning this bundle"),
):
    """Mark a parser-suggested bundle as user-rejected (ADAC follow-up).

    Writes (project_code, bundle_id, 'rejected') to the override table and
    immediately sets every member's ``bundle_role`` to ``'rejected'`` so the
    UI updates without waiting for a sync. Persists across re-syncs via
    ``_apply_bundle_overrides`` in ``services/media_plan_sync.py``.

    Behaviour the user gets (option 3 from the design doc): the former parent
    line becomes a standalone with the full pool budget. The former children,
    whose budgets were zeroed by the parser when the bundle was first
    detected, fall through pacing's ``budget <= 0`` skip and disappear from
    the dashboard. The Reject button tooltip on the frontend explains this
    behaviour. If the user wants the children back as standalones with their
    own budgets, they un-merge the source sheet's Budget cells and re-sync.
    """
    member_count = _verify_bundle_exists(project_code, bundle_id)
    updated_by = _resolve_user_email(request)

    # MERGE into the override table — single source of truth across syncs.
    # The override row's bundle_role is the override TYPE ('confirmed_parent'
    # or 'rejected'), not the per-line role written to media_plan_lines.
    bq.run_query(
        f"""
        MERGE {bq.table('media_plan_bundle_overrides')} t
        USING (
            SELECT
                @pc AS project_code,
                @bid AS bundle_id,
                'rejected' AS bundle_role,
                CURRENT_TIMESTAMP() AS updated_at,
                @updated_by AS updated_by
        ) s
        ON t.project_code = s.project_code AND t.bundle_id = s.bundle_id
        WHEN MATCHED THEN UPDATE SET
            bundle_role = s.bundle_role,
            updated_at = s.updated_at,
            updated_by = s.updated_by
        WHEN NOT MATCHED THEN INSERT
            (project_code, bundle_id, bundle_role, updated_at, updated_by)
            VALUES (s.project_code, s.bundle_id, s.bundle_role, s.updated_at, s.updated_by)
        """,
        [
            bq.string_param("pc", project_code),
            bq.string_param("bid", bundle_id),
            bq.string_param("updated_by", updated_by or ""),
        ],
    )

    # Update live lines: every member gets bundle_role='rejected', regardless
    # of whether it was previously a parent or child. The pacing service
    # treats 'rejected' as not-parent / not-child — parents fall through to
    # standalone pacing with the pool budget; children with NULL budgets get
    # filtered out by the budget<=0 skip.
    bq.run_query(
        f"""
        UPDATE {bq.table('media_plan_lines')}
        SET bundle_role = 'rejected'
        WHERE project_code = @pc AND bundle_id = @bid
        """,
        [
            bq.string_param("pc", project_code),
            bq.string_param("bid", bundle_id),
        ],
    )

    return {
        "status": "rejected",
        "project_code": project_code,
        "bundle_id": bundle_id,
        "members_updated": member_count,
    }


@router.post("/creative-aliases")
async def create_creative_alias(body: dict):
    """Create a manual creative variant alias for ad name grouping."""
    project_code = body.get("project_code")
    ad_name_pattern = body.get("ad_name_pattern")
    creative_variant = body.get("creative_variant")
    if not project_code or not ad_name_pattern or not creative_variant:
        raise HTTPException(400, "project_code, ad_name_pattern, and creative_variant are required")

    alias_id = f"cva-{uuid.uuid4().hex[:12]}"
    sql = f"""
        INSERT INTO {bq.table('creative_variant_aliases')}
            (alias_id, project_code, ad_name_pattern, platform_id, creative_variant, created_by)
        VALUES (@alias_id, @project_code, @ad_name_pattern, @platform_id, @creative_variant, 'admin')
    """
    bq.run_query(sql, [
        bq.string_param("alias_id", alias_id),
        bq.string_param("project_code", project_code),
        bq.string_param("ad_name_pattern", ad_name_pattern),
        bq.scalar_param("platform_id", "STRING", body.get("platform_id") or None),
        bq.string_param("creative_variant", creative_variant),
    ])
    return {"status": "created", "alias_id": alias_id}


@router.delete("/creative-aliases/{alias_id}")
async def delete_creative_alias(alias_id: str):
    """Delete a creative variant alias."""
    sql = f"DELETE FROM {bq.table('creative_variant_aliases')} WHERE alias_id = @alias_id"
    bq.run_query(sql, [bq.string_param("alias_id", alias_id)])
    return {"status": "deleted", "alias_id": alias_id}
