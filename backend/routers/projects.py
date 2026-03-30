from fastapi import APIRouter, HTTPException, Query

from backend.models.projects import ProjectSummary, ProjectDetail
from backend.services import bigquery_client as bq

router = APIRouter(prefix="/api/projects", tags=["projects"])


@router.get("/", response_model=list[ProjectSummary])
async def list_projects(
    status: str | None = Query(None, description="Filter by project status"),
):
    status_clause = "AND p.status = @status" if status else ""
    sql = f"""
        SELECT
            p.project_code,
            p.project_name,
            c.client_name,
            p.status,
            p.start_date,
            p.end_date,
            p.net_budget,
            COALESCE(s.total_spend, 0) AS total_spend,
            DATE_DIFF(p.end_date, CURRENT_DATE(), DAY) AS days_remaining,
            p.updated_at
        FROM {bq.table('dim_projects')} p
        LEFT JOIN {bq.table('dim_clients')} c USING (client_id)
        LEFT JOIN (
            SELECT project_code, SUM(spend) AS total_spend
            FROM {bq.table('fact_digital_daily')}
            GROUP BY project_code
        ) s USING (project_code)
        WHERE 1=1
        {status_clause}
        ORDER BY p.start_date DESC
    """
    params = [bq.string_param("status", status)] if status else None
    rows = bq.run_query(sql, params)

    return [
        ProjectSummary(
            project_code=r["project_code"],
            project_name=r["project_name"],
            client_name=r.get("client_name"),
            status=r.get("status", "active"),
            start_date=r.get("start_date"),
            end_date=r.get("end_date"),
            net_budget=float(r["net_budget"]) if r.get("net_budget") else None,
            total_spend=float(r.get("total_spend", 0)),
            days_remaining=r.get("days_remaining"),
            updated_at=r.get("updated_at"),
        )
        for r in rows
    ]


@router.get("/{project_code}", response_model=ProjectDetail)
async def get_project(project_code: str):
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
            p.created_at,
            p.updated_at
        FROM {bq.table('dim_projects')} p
        LEFT JOIN {bq.table('dim_clients')} c USING (client_id)
        LEFT JOIN (
            SELECT
                project_code,
                SUM(spend) AS total_spend,
                COUNT(DISTINCT platform_id) AS platforms_active,
                MIN(date) AS first_data_date,
                MAX(date) AS last_data_date
            FROM {bq.table('fact_digital_daily')}
            GROUP BY project_code
        ) s USING (project_code)
        WHERE p.project_code = @project_code
    """
    rows = bq.run_query(sql, [bq.string_param("project_code", project_code)])
    if not rows:
        raise HTTPException(404, f"Project {project_code} not found")

    r = rows[0]
    return ProjectDetail(
        project_code=r["project_code"],
        project_name=r["project_name"],
        client_id=r.get("client_id"),
        client_name=r.get("client_name"),
        campaign_type=r.get("campaign_type"),
        status=r.get("status", "active"),
        start_date=r.get("start_date"),
        end_date=r.get("end_date"),
        net_budget=float(r["net_budget"]) if r.get("net_budget") else None,
        currency=r.get("currency", "CAD"),
        total_spend=float(r.get("total_spend", 0)),
        days_remaining=r.get("days_remaining"),
        platforms_active=r.get("platforms_active", 0),
        first_data_date=r.get("first_data_date"),
        last_data_date=r.get("last_data_date"),
        media_plan_sheet_id=r.get("media_plan_sheet_id"),
        slack_channel_id=r.get("slack_channel_id"),
        created_at=r.get("created_at"),
        updated_at=r.get("updated_at"),
    )
