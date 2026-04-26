from datetime import date

from fastapi import APIRouter, HTTPException, Query

from backend.models.pacing import (
    BundleMember,
    LinePacing,
    PacingHistoryPoint,
    PacingHistoryResponse,
    PacingResponse,
    PhaseSummary,
)
from backend.services import bigquery_client as bq
from backend.services.pacing import run_all_active, run_pacing_for_project

router = APIRouter(prefix="/api/pacing", tags=["pacing"])


def _float(v, default=0.0) -> float:
    return float(v) if v is not None else default


@router.get("/{project_code}", response_model=PacingResponse)
async def get_pacing(
    project_code: str,
    as_of_date: date | None = Query(
        None,
        description=(
            "Pin the response to a specific historical date instead of the "
            "latest budget_tracking row. Used by the Retrospective Mode "
            "frontend (ADAC-51 commit 7) to render the pacing snapshot for "
            "a past date. Defaults to the most recent row when omitted, "
            "matching the live page's contract."
        ),
    ),
):
    """Return the pacing snapshot from budget_tracking for a project.

    By default returns the most recent row. When ``as_of_date`` is supplied,
    returns the row for that specific date (or empty if no daily-pipeline run
    landed for that date). Used by Retrospective Mode for historical replay.
    """

    project_sql = f"""
        SELECT project_code, net_budget
        FROM {bq.table('dim_projects')}
        WHERE project_code = @project_code
    """
    projects = bq.run_query(project_sql, [bq.string_param("project_code", project_code)])
    if not projects:
        raise HTTPException(404, f"Project {project_code} not found")

    net_budget = _float(projects[0].get("net_budget"))

    # Retrospective Mode (ADAC-51): when as_of_date is supplied, pin the date
    # filter to that specific row. Otherwise fall back to the latest row, which
    # is the live page's contract.
    if as_of_date is not None:
        date_filter = "AND bt.date = @as_of_date"
        date_params = [bq.date_param("as_of_date", as_of_date)]
    else:
        date_filter = (
            "AND bt.date = ("
            f"SELECT MAX(date) FROM {bq.table('budget_tracking')} "
            "WHERE project_code = @project_code)"
        )
        date_params = []

    tracking_sql = f"""
        WITH mpl_dedup AS (
            SELECT * EXCEPT(_rn) FROM (
                SELECT
                    l.line_id,
                    l.audience_name,
                    l.flight_start,
                    l.flight_end,
                    -- Multi-plan: phase metadata flows from media_plans.sheet_id
                    -- through to the join table for the human label and order.
                    mp.sheet_id        AS sheet_id,
                    pmp.phase_label    AS phase_label,
                    pmp.display_order  AS phase_display_order,
                    ROW_NUMBER() OVER (
                        PARTITION BY l.line_id
                        ORDER BY l.sync_version DESC
                    ) AS _rn
                FROM {bq.table('media_plan_lines')} l
                JOIN {bq.table('media_plans')} mp
                  ON l.plan_id = mp.plan_id
                 AND mp.is_current = TRUE
                JOIN {bq.table('project_media_plans')} pmp
                  ON mp.project_code = pmp.project_code
                 AND mp.sheet_id   = pmp.sheet_id
                 AND pmp.is_active = TRUE
                WHERE l.project_code = @project_code
            ) WHERE _rn = 1
        )
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
            bt.bundle_id,
            bt.bundle_role,
            mpl.audience_name,
            mpl.flight_start,
            mpl.flight_end,
            mpl.sheet_id,
            mpl.phase_label,
            mpl.phase_display_order
        FROM {bq.table('budget_tracking')} bt
        LEFT JOIN mpl_dedup mpl ON bt.line_id = mpl.line_id
        WHERE bt.project_code = @project_code
            {date_filter}
        ORDER BY mpl.phase_display_order NULLS LAST, mpl.sheet_id,
                 bt.platform_id, bt.bundle_id, bt.line_code
    """
    rows = bq.run_query(
        tracking_sql,
        [bq.string_param("project_code", project_code), *date_params],
    )

    if not rows:
        return PacingResponse(
            project_code=project_code,
            as_of_date=date.today(),
            net_budget=net_budget,
        )

    as_of = rows[0]["date"]

    # Fetch bundle members for any bundle_id the pacing rows reference.
    # Children aren't written to budget_tracking (pacing skips them), so their
    # audience_name / line_code only lives on media_plan_lines. We surface the
    # list on each parent's response so the UI can render the expandable card
    # without a second round-trip.
    bundle_ids = list({r.get("bundle_id") for r in rows if r.get("bundle_id")})
    bundle_members_by_id: dict[str, list[BundleMember]] = {}
    if bundle_ids:
        # Filter to "child-like" rows. For suggested/confirmed bundles the
        # role itself is the discriminator. For rejected bundles every member
        # carries bundle_role='rejected' (no parent/child split), so we use
        # the budget convention — children have NULL budget by design — to
        # exclude the former parent. This is what lets the frontend render
        # the rejected-state badge + Clear button on the parent's row.
        members_sql = f"""
            WITH mpl_dedup AS (
                SELECT * EXCEPT(_rn) FROM (
                    SELECT
                        line_id, line_code, audience_name, bundle_id, bundle_role, budget,
                        ROW_NUMBER() OVER (
                            PARTITION BY line_id
                            ORDER BY sync_version DESC
                        ) AS _rn
                    FROM {bq.table('media_plan_lines')}
                    WHERE project_code = @project_code
                      -- Plan-id-aware + multi-plan dedup guard (see top
                      -- of this router for the canonical comment).
                      AND plan_id IN (
                          SELECT mp.plan_id
                          FROM {bq.table('media_plans')} mp
                          JOIN {bq.table('project_media_plans')} pmp
                            ON mp.project_code = pmp.project_code
                           AND mp.sheet_id   = pmp.sheet_id
                          WHERE mp.project_code = @project_code
                            AND mp.is_current   = TRUE
                            AND pmp.is_active   = TRUE
                      )
                ) WHERE _rn = 1
            )
            SELECT bundle_id, line_id, line_code, audience_name
            FROM mpl_dedup
            WHERE bundle_id IN UNNEST(@bundle_ids)
              AND (
                bundle_role IN ('suggested_child', 'confirmed_child')
                OR (bundle_role = 'rejected' AND budget IS NULL)
              )
            ORDER BY bundle_id, line_id
        """
        member_rows = bq.run_query(
            members_sql,
            [
                bq.string_param("project_code", project_code),
                bq.array_param("bundle_ids", "STRING", bundle_ids),
            ],
        )
        for mr in member_rows:
            bundle_members_by_id.setdefault(mr["bundle_id"], []).append(
                BundleMember(
                    line_id=mr["line_id"],
                    line_code=mr.get("line_code"),
                    audience_name=mr.get("audience_name"),
                )
            )

    # C2: Conservative approach — exclude pending lines from BOTH numerator and denominator
    # to avoid inflating overall_pacing_percentage (aligns with conservative-estimate ethos)
    active_rows = [r for r in rows if r.get("line_status") not in ("pending", "not_started")]
    pending_count = len(rows) - len(active_rows)

    total_planned = sum(_float(r.get("planned_spend_to_date")) for r in active_rows)
    total_actual = sum(_float(r.get("actual_spend_to_date")) for r in active_rows)

    # Multi-plan: aggregate per (sheet_id, phase_label, display_order). Lines
    # with no sheet (legacy projects whose plan never landed in
    # project_media_plans) are dropped from the phases list — the response
    # still includes them in `lines` so nothing is hidden.
    phases_by_sheet: dict[str, dict] = {}
    for r in rows:
        sid = r.get("sheet_id")
        if not sid:
            continue
        bucket = phases_by_sheet.setdefault(sid, {
            "sheet_id": sid,
            "phase_label": r.get("phase_label"),
            "display_order": r.get("phase_display_order"),
            "line_count": 0,
            "planned_budget": 0.0,
            "planned_spend_to_date": 0.0,
            "actual_spend_to_date": 0.0,
        })
        bucket["line_count"] += 1
        bucket["planned_budget"] += _float(r.get("planned_budget"))
        bucket["planned_spend_to_date"] += _float(r.get("planned_spend_to_date"))
        bucket["actual_spend_to_date"] += _float(r.get("actual_spend_to_date"))

    phase_summaries = sorted(
        [
            PhaseSummary(
                sheet_id=p["sheet_id"],
                phase_label=p["phase_label"],
                display_order=p["display_order"],
                line_count=p["line_count"],
                planned_budget=p["planned_budget"],
                planned_spend_to_date=p["planned_spend_to_date"],
                actual_spend_to_date=p["actual_spend_to_date"],
                pacing_percentage=(
                    round(p["actual_spend_to_date"] / p["planned_spend_to_date"] * 100, 1)
                    if p["planned_spend_to_date"]
                    else 0
                ),
            )
            for p in phases_by_sheet.values()
        ],
        # NULL display_order sorts last; tie-break on phase_label for stability.
        key=lambda s: (s.display_order is None, s.display_order or 0, s.phase_label or ""),
    )

    return PacingResponse(
        project_code=project_code,
        as_of_date=as_of,
        net_budget=net_budget,
        total_planned_to_date=total_planned,
        total_actual_to_date=total_actual,
        overall_pacing_percentage=round(total_actual / total_planned * 100, 1) if total_planned else 0,
        pending_line_count=pending_count,
        lines=[
            LinePacing(
                line_id=r["line_id"],
                line_code=r.get("line_code"),
                platform_id=r.get("platform_id"),
                channel_category=r.get("channel_category"),
                audience_name=r.get("audience_name"),
                flight_start=str(r["flight_start"]) if r.get("flight_start") else None,
                flight_end=str(r["flight_end"]) if r.get("flight_end") else None,
                line_status=r.get("line_status") or "unknown",  # C3: changed to "unknown"
                planned_budget=_float(r.get("planned_budget")),
                planned_spend_to_date=_float(r.get("planned_spend_to_date")),
                actual_spend_to_date=_float(r.get("actual_spend_to_date")),
                remaining_budget=_float(r.get("remaining_budget")),
                remaining_days=int(r.get("remaining_days") or 0),
                pacing_percentage=_float(r.get("pacing_percentage")),
                daily_budget_required=_float(r.get("daily_budget_required"), None),
                is_over_pacing=bool(r.get("is_over_pacing")),
                is_under_pacing=bool(r.get("is_under_pacing")),
                bundle_id=r.get("bundle_id"),
                bundle_role=r.get("bundle_role"),
                bundle_members=bundle_members_by_id.get(r.get("bundle_id") or "", []),
                sheet_id=r.get("sheet_id"),
                phase_label=r.get("phase_label"),
                phase_display_order=r.get("phase_display_order"),
            )
            for r in rows
        ],
        phases=phase_summaries,
    )


@router.get("/{project_code}/history", response_model=PacingHistoryResponse)
async def get_pacing_history(
    project_code: str,
    days: int = Query(60, ge=7, le=365),
    as_of_date: date | None = Query(
        None,
        description=(
            "Anchor the history window at this date instead of today. "
            "Required by Retrospective Mode (ADAC-51) so a past-snapshot view "
            "can show the trailing N days ending at the replay date rather "
            "than today. Defaults to today when omitted."
        ),
    ),
):
    """Return daily pacing snapshots from budget_tracking for historical trend.

    Window is ``[as_of_date - days, as_of_date]``. In live mode ``as_of_date``
    defaults to today; in retrospective mode it's the replay date.
    """
    anchor = as_of_date or date.today()
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
            AND date >= DATE_SUB(@anchor, INTERVAL @days DAY)
            AND date <= @anchor
        ORDER BY date ASC, line_id
    """
    rows = bq.run_query(sql, [
        bq.string_param("project_code", project_code),
        bq.scalar_param("days", "INT64", days),
        bq.date_param("anchor", anchor),
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
    """Trigger pacing calculation for all active projects with media plans.

    Always runs 'as of today'. For a historical replay, use the snapshot
    endpoint shipped in ADAC-51 commit 5.
    """
    result = run_all_active(date.today())
    return result


@router.post("/{project_code}/run")
async def run_pacing_single(project_code: str):
    """Trigger pacing calculation for a single project as of today."""
    result = run_pacing_for_project(project_code, date.today())
    return result
