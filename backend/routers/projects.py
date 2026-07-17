import time

from fastapi import APIRouter, HTTPException, Query

from backend.config import settings
from backend.models.projects import ProjectSummary, ProjectDetail
from backend.services import bigquery_client as bq

router = APIRouter(prefix="/api/projects", tags=["projects"])


def _now() -> float:
    # monotonic (not wall-clock) so a system-clock adjustment can't wedge the
    # TTL. Tests monkeypatch this seam for a deterministic clock.
    return time.monotonic()


# In-process, per-endpoint TTL cache for the project pacing rollup. The list
# cache is keyed by the two query-affecting params; the detail cache by
# project_code. Each value is (stored_at_monotonic, payload). TTL comes from
# settings.projects_cache_ttl_seconds; TTL <= 0 disables caching entirely.
_LIST_CACHE: dict[tuple, tuple[float, list[ProjectSummary]]] = {}
_DETAIL_CACHE: dict[str, tuple[float, ProjectDetail]] = {}


# CONVENTION: the list/detail rollup reads fact_digital_daily, budget_tracking,
# media_plan_lines/media_plans/project_media_plans, dim_projects, and dim_clients.
# ANY router endpoint that writes one of those — directly or via run_transformation /
# run_pacing_* / run_daily_pipeline / sync_media_plan — MUST call one of these after
# the write returns, or the project list/detail cache will serve stale numbers.
def invalidate_project(code: str) -> None:
    """Evict a project's detail entry and the whole list rollup (the list
    aggregates this project's numbers, so any list key may be affected)."""
    _DETAIL_CACHE.pop(code, None)
    _LIST_CACHE.clear()


def invalidate_all() -> None:
    _DETAIL_CACHE.clear()
    _LIST_CACHE.clear()


@router.get("/", response_model=list[ProjectSummary])
async def list_projects(
    status: str | None = Query(None, description="Filter by project status"),
    include_recently_ended: bool = Query(True, description="Include projects completed within the last 14 days"),
    refresh: bool = Query(False, description="Bypass the cache and force a fresh query"),
):
    ttl = settings.projects_cache_ttl_seconds
    cache_key = (status, include_recently_ended)
    if ttl > 0 and not refresh:
        entry = _LIST_CACHE.get(cache_key)
        if entry is not None and _now() - entry[0] < ttl:
            return entry[1]

    status_clause = "AND p.status = @status" if status else ""
    if not status and include_recently_ended:
        status_clause = (
            "AND (p.status = 'active' "
            "OR (p.status = 'completed' AND p.end_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY)))"
        )
    # AI-001: roll up project-level pacing_percentage from budget_tracking on
    # the latest snapshot date per project. Mirrors the conservative-estimate
    # pattern in routers/pacing.py (~lines 218–224): pending/not_started lines
    # are excluded from BOTH numerator and denominator so a half-started
    # project doesn't artificially under-pace. The frontend PacingBadge falls
    # back to "No Data" when this is null — which is the honest signal for
    # projects whose daily pipeline hasn't produced a row yet.
    #
    # No MPL dedup CTE needed here: budget_tracking is the pacing engine's
    # own materialized output (one row per (date, line_id)) and does not read
    # media_plan_lines in this query. See `feedback_mpl_dedup.md` — the
    # dedup-guard pattern only applies to direct media_plan_lines reads.
    sql = f"""
        SELECT
            p.project_code,
            p.project_name,
            c.client_name,
            p.status,
            p.start_date,
            p.end_date,
            p.net_budget,
            COALESCE(dir.direct_budget, 0) AS direct_budget,
            COALESCE(dir.self_serve_budget, 0) AS self_serve_budget,
            COALESCE(s.total_spend, 0) AS total_spend,
            CASE
                WHEN bt.bt_planned > 0
                THEN ROUND(SAFE_DIVIDE(bt.bt_actual, bt.bt_planned) * 100, 1)
                ELSE NULL
            END AS pacing_percentage,
            DATE_DIFF(p.end_date, CURRENT_DATE(), DAY) AS days_remaining,
            CASE
                WHEN p.status = 'completed'
                 AND p.end_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY)
                THEN TRUE ELSE FALSE
            END AS recently_ended,
            p.updated_at
        FROM {bq.table('dim_projects')} p
        LEFT JOIN {bq.table('dim_clients')} c USING (client_id)
        LEFT JOIN (
            SELECT project_code, SUM(spend) AS total_spend
            FROM {bq.table('fact_digital_daily')}
            GROUP BY project_code
        ) s USING (project_code)
        LEFT JOIN (
            SELECT
                bt_inner.project_code,
                SUM(IF(bt_inner.line_status NOT IN ('pending','not_started')
                       AND bt_inner.planned_spend_to_date > 0,
                       bt_inner.actual_spend_to_date, 0)) AS bt_actual,
                SUM(IF(bt_inner.line_status NOT IN ('pending','not_started'),
                       bt_inner.planned_spend_to_date, 0)) AS bt_planned
            FROM {bq.table('budget_tracking')} bt_inner
            JOIN (
                SELECT project_code, MAX(date) AS max_date
                FROM {bq.table('budget_tracking')}
                GROUP BY project_code
            ) latest
              ON bt_inner.project_code = latest.project_code
             AND bt_inner.date         = latest.max_date
            GROUP BY bt_inner.project_code
        ) bt USING (project_code)
        LEFT JOIN (
            -- Direct-buy budget (is_direct) AND self-serve budget (the pacing
            -- inclusion set: COALESCE(is_direct_override, is_direct) = FALSE,
            -- NULL excluded — mirrors pacing.py:373-397) in a SINGLE deduped
            -- read of media_plan_lines, so this does not add a second read.
            SELECT
                project_code,
                SUM(IF(COALESCE(is_direct_override, is_direct, FALSE) = TRUE, budget, 0)) AS direct_budget,
                SUM(IF(COALESCE(is_direct_override, is_direct) = FALSE, budget, 0)) AS self_serve_budget
            FROM (
                SELECT
                    mpl.project_code, mpl.budget, mpl.is_direct, mpl.is_direct_override,
                    ROW_NUMBER() OVER (
                        PARTITION BY mpl.line_id ORDER BY mpl.sync_version DESC
                    ) AS _rn
                FROM {bq.table('media_plan_lines')} mpl
                JOIN {bq.table('media_plans')} mp
                  ON mpl.plan_id = mp.plan_id AND mp.is_current = TRUE
                JOIN {bq.table('project_media_plans')} pmp
                  ON mp.project_code = pmp.project_code
                 AND mp.sheet_id     = pmp.sheet_id
                 AND pmp.is_active    = TRUE
            )
            WHERE _rn = 1
            GROUP BY project_code
        ) dir USING (project_code)
        WHERE 1=1
        {status_clause}
        ORDER BY p.start_date DESC
    """
    params = [bq.string_param("status", status)] if status else None
    rows = bq.run_query(sql, params)

    result = [
        ProjectSummary(
            project_code=r["project_code"],
            project_name=r["project_name"],
            client_name=r.get("client_name"),
            status=r.get("status", "active"),
            start_date=r.get("start_date"),
            end_date=r.get("end_date"),
            net_budget=float(r["net_budget"]) if r.get("net_budget") else None,
            direct_budget=float(r.get("direct_budget") or 0),
            self_serve_budget=float(r.get("self_serve_budget") or 0),
            total_spend=float(r.get("total_spend", 0)),
            pacing_percentage=(
                float(r["pacing_percentage"])
                if r.get("pacing_percentage") is not None
                else None
            ),
            days_remaining=r.get("days_remaining"),
            recently_ended=bool(r.get("recently_ended", False)),
            updated_at=r.get("updated_at"),
        )
        for r in rows
    ]
    if ttl > 0:
        _LIST_CACHE[cache_key] = (_now(), result)
    return result


@router.get("/{project_code}", response_model=ProjectDetail)
async def get_project(
    project_code: str,
    refresh: bool = Query(False, description="Bypass the cache and force a fresh query"),
):
    ttl = settings.projects_cache_ttl_seconds
    if ttl > 0 and not refresh:
        entry = _DETAIL_CACHE.get(project_code)
        if entry is not None and _now() - entry[0] < ttl:
            return entry[1]

    # AI-001: same project-level pacing_percentage rollup as list_projects,
    # scoped to a single project_code. See the long-form comment on
    # list_projects above for the conservative-estimate ethos and the
    # rationale for not needing the media_plan_lines dedup-guard CTE here.
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
            COALESCE(dir.direct_budget, 0) AS direct_budget,
            COALESCE(dir.self_serve_budget, 0) AS self_serve_budget,
            p.currency,
            p.media_plan_sheet_id,
            p.slack_channel_id,
            COALESCE(s.total_spend, 0) AS total_spend,
            CASE
                WHEN bt.bt_planned > 0
                THEN ROUND(SAFE_DIVIDE(bt.bt_actual, bt.bt_planned) * 100, 1)
                ELSE NULL
            END AS pacing_percentage,
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
        LEFT JOIN (
            SELECT
                project_code,
                SUM(IF(line_status NOT IN ('pending','not_started')
                       AND planned_spend_to_date > 0,
                       actual_spend_to_date, 0)) AS bt_actual,
                SUM(IF(line_status NOT IN ('pending','not_started'),
                       planned_spend_to_date, 0)) AS bt_planned
            FROM {bq.table('budget_tracking')}
            WHERE project_code = @project_code
              AND date = (
                SELECT MAX(date)
                FROM {bq.table('budget_tracking')}
                WHERE project_code = @project_code
              )
            GROUP BY project_code
        ) bt USING (project_code)
        LEFT JOIN (
            -- Direct-buy budget (is_direct) AND self-serve budget (the pacing
            -- inclusion set: COALESCE(is_direct_override, is_direct) = FALSE,
            -- NULL excluded — mirrors pacing.py:373-397) in a SINGLE deduped
            -- read of media_plan_lines, so this does not add a second read.
            SELECT
                project_code,
                SUM(IF(COALESCE(is_direct_override, is_direct, FALSE) = TRUE, budget, 0)) AS direct_budget,
                SUM(IF(COALESCE(is_direct_override, is_direct) = FALSE, budget, 0)) AS self_serve_budget
            FROM (
                SELECT
                    mpl.project_code, mpl.budget, mpl.is_direct, mpl.is_direct_override,
                    ROW_NUMBER() OVER (
                        PARTITION BY mpl.line_id ORDER BY mpl.sync_version DESC
                    ) AS _rn
                FROM {bq.table('media_plan_lines')} mpl
                JOIN {bq.table('media_plans')} mp
                  ON mpl.plan_id = mp.plan_id AND mp.is_current = TRUE
                JOIN {bq.table('project_media_plans')} pmp
                  ON mp.project_code = pmp.project_code
                 AND mp.sheet_id     = pmp.sheet_id
                 AND pmp.is_active    = TRUE
                WHERE mpl.project_code = @project_code
            )
            WHERE _rn = 1
            GROUP BY project_code
        ) dir USING (project_code)
        WHERE p.project_code = @project_code
    """
    rows = bq.run_query(sql, [bq.string_param("project_code", project_code)])
    if not rows:
        raise HTTPException(404, f"Project {project_code} not found")

    r = rows[0]
    result = ProjectDetail(
        project_code=r["project_code"],
        project_name=r["project_name"],
        client_id=r.get("client_id"),
        client_name=r.get("client_name"),
        campaign_type=r.get("campaign_type"),
        status=r.get("status", "active"),
        start_date=r.get("start_date"),
        end_date=r.get("end_date"),
        net_budget=float(r["net_budget"]) if r.get("net_budget") else None,
        direct_budget=float(r.get("direct_budget") or 0),
        self_serve_budget=float(r.get("self_serve_budget") or 0),
        currency=r.get("currency", "CAD"),
        total_spend=float(r.get("total_spend", 0)),
        pacing_percentage=(
            float(r["pacing_percentage"])
            if r.get("pacing_percentage") is not None
            else None
        ),
        days_remaining=r.get("days_remaining"),
        platforms_active=r.get("platforms_active", 0),
        first_data_date=r.get("first_data_date"),
        last_data_date=r.get("last_data_date"),
        media_plan_sheet_id=r.get("media_plan_sheet_id"),
        slack_channel_id=r.get("slack_channel_id"),
        created_at=r.get("created_at"),
        updated_at=r.get("updated_at"),
    )
    if ttl > 0:
        _DETAIL_CACHE[project_code] = (_now(), result)
    return result
