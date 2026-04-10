from datetime import date

from fastapi import APIRouter, HTTPException, Query

from backend.models.pacing import LinePacing, PacingHistoryPoint, PacingHistoryResponse, PacingResponse
from backend.services import bigquery_client as bq
from backend.services.pacing import run_all_active, run_pacing_for_project

router = APIRouter(prefix="/api/pacing", tags=["pacing"])


def _float(v, default=0.0) -> float:
    return float(v) if v is not None else default


@router.get("/{project_code}", response_model=PacingResponse)
async def get_pacing(project_code: str):
    """Return the latest pacing snapshot from budget_tracking for a project."""

    project_sql = f"""
        SELECT project_code, net_budget
        FROM {bq.table('dim_projects')}
        WHERE project_code = @project_code
    """
    projects = bq.run_query(project_sql, [bq.string_param("project_code", project_code)])
    if not projects:
        raise HTTPException(404, f"Project {project_code} not found")

    net_budget = _float(projects[0].get("net_budget"))

    tracking_sql = f"""
        SELECT
            bt.date,
            bt.line_id,
            bt.line_code,
            bt.platform_id,
            bt.channel_category,
            bt.line_status,
            bt.planned_budget,
            bt.planned_spend_to_date,
            bt.actual_spend_to_date,
            bt.remaining_budget,
            bt.remaining_days,
            bt.pacing_percentage,
            bt.daily_budget_required,
            bt.is_over_pacing,
            bt.is_under_pacing,
            mpl.audience_name,
            mpl.flight_start,
            mpl.flight_end
        FROM {bq.table('budget_tracking')} bt
        LEFT JOIN {bq.table('media_plan_lines')} mpl ON bt.line_id = mpl.line_id
        WHERE bt.project_code = @project_code
            AND bt.date = (
                SELECT MAX(date) FROM {bq.table('budget_tracking')}
                WHERE project_code = @project_code
            )
        ORDER BY bt.platform_id, bt.line_code
    """
    rows = bq.run_query(tracking_sql, [bq.string_param("project_code", project_code)])

    if not rows:
        return PacingResponse(
            project_code=project_code,
            as_of_date=date.today(),
            net_budget=net_budget,
        )

    as_of = rows[0]["date"]
    total_planned = sum(_float(r.get("planned_spend_to_date")) for r in rows)
    total_actual = sum(_float(r.get("actual_spend_to_date")) for r in rows)

    return PacingResponse(
        project_code=project_code,
        as_of_date=as_of,
        net_budget=net_budget,
        total_planned_to_date=total_planned,
        total_actual_to_date=total_actual,
        overall_pacing_percentage=round(total_actual / total_planned * 100, 1) if total_planned else 0,
        lines=[
            LinePacing(
                line_id=r["line_id"],
                line_code=r.get("line_code"),
                platform_id=r.get("platform_id"),
                channel_category=r.get("channel_category"),
                audience_name=r.get("audience_name"),
                flight_start=str(r["flight_start"]) if r.get("flight_start") else None,
                flight_end=str(r["flight_end"]) if r.get("flight_end") else None,
                line_status=r.get("line_status", "active"),
                planned_budget=_float(r.get("planned_budget")),
                planned_spend_to_date=_float(r.get("planned_spend_to_date")),
                actual_spend_to_date=_float(r.get("actual_spend_to_date")),
                remaining_budget=_float(r.get("remaining_budget")),
                remaining_days=int(r.get("remaining_days") or 0),
                pacing_percentage=_float(r.get("pacing_percentage")),
                daily_budget_required=_float(r.get("daily_budget_required"), None),
                is_over_pacing=bool(r.get("is_over_pacing")),
                is_under_pacing=bool(r.get("is_under_pacing")),
            )
            for r in rows
        ],
    )


@router.get("/{project_code}/history", response_model=PacingHistoryResponse)
async def get_pacing_history(
    project_code: str,
    days: int = Query(60, ge=7, le=365),
):
    """Return daily pacing snapshots from budget_tracking for historical trend."""
    project_sql = f"""
        SELECT project_code
        FROM {bq.table('dim_projects')}
        WHERE project_code = @project_code
    """
    projects = bq.run_query(project_sql, [bq.string_param("project_code", project_code)])
    if not projects:
        raise HTTPException(404, f"Project {project_code} not found")

    sql = f"""
        SELECT date, line_id, pacing_percentage
        FROM {bq.table('budget_tracking')}
        WHERE project_code = @project_code
            AND date >= DATE_SUB(CURRENT_DATE(), INTERVAL @days DAY)
        ORDER BY date ASC, line_id
    """
    rows = bq.run_query(sql, [
        bq.string_param("project_code", project_code),
        bq.scalar_param("days", "INT64", days),
    ])
    return PacingHistoryResponse(
        project_code=project_code,
        history=[
            PacingHistoryPoint(
                date=str(r["date"]),
                line_id=r["line_id"],
                pacing_percentage=float(r.get("pacing_percentage") or 0),
            )
            for r in rows
        ],
    )


@router.post("/run")
async def run_pacing():
    """Trigger pacing calculation for all active projects with media plans."""
    result = run_all_active()
    return result


@router.post("/{project_code}/run")
async def run_pacing_single(project_code: str):
    """Trigger pacing calculation for a single project."""
    result = run_pacing_for_project(project_code)
    return result
