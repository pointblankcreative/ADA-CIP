import logging
from datetime import date

from fastapi import APIRouter, HTTPException, Query

from backend.models.pacing import (
    BundleMember,
    DirectLine,
    LinePacing,
    PacingHistoryPoint,
    PacingHistoryResponse,
    PacingResponse,
    PhaseSummary,
    UntrackedPlatformSpend,
)
from backend.routers import projects as projects_router
from backend.services import bigquery_client as bq
from backend.services.pacing import run_all_active, run_pacing_for_project

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/pacing", tags=["pacing"])


def _float(v, default=0.0) -> float:
    return float(v) if v is not None else default


def _query_untracked_platform_spend(
    project_code: str,
    as_of_date: date | None,
) -> list[UntrackedPlatformSpend]:
    """AI-002: spend in fact_digital_daily on platforms that have NO line in
    the current, active media plan. The pacing engine only ever queries spend
    for platforms it has lines for, so without this bucket an unplanned
    platform (e.g. a StackAdapt buy missing from the synced plan — AI-022)
    silently disappears from the Pacing tab while still counting toward the
    project header's fact-table total.

    "Tracked" is defined per-platform from the deduped, current, active
    media plan lines — the standard ROW_NUMBER dedup + plan_id IN (current
    media_plans × active project_media_plans) guard (see members_sql below
    and feedback_mpl_dedup.md).

    ``as_of_date`` clamps the spend window for Retrospective Mode so a
    replay never peeks past the snapshot date (matches the engine's
    ``date <= @as_of_date`` convention). None means live mode — no clamp.
    """
    params = [bq.string_param("project_code", project_code)]
    date_clause = ""
    if as_of_date is not None:
        date_clause = "AND f.date <= @as_of_date"
        params.append(bq.date_param("as_of_date", as_of_date))
    sql = f"""
        WITH tracked_platforms AS (
            SELECT DISTINCT platform_id FROM (
                SELECT
                    l.platform_id,
                    ROW_NUMBER() OVER (
                        PARTITION BY l.line_id
                        ORDER BY l.sync_version DESC
                    ) AS _rn
                FROM {bq.table('media_plan_lines')} l
                WHERE l.project_code = @project_code
                  -- Plan-id-aware + multi-plan dedup guard (see top of this
                  -- router for the canonical comment).
                  AND l.plan_id IN (
                      SELECT mp.plan_id
                      FROM {bq.table('media_plans')} mp
                      JOIN {bq.table('project_media_plans')} pmp
                        ON mp.project_code = pmp.project_code
                       AND mp.sheet_id   = pmp.sheet_id
                      WHERE mp.project_code = @project_code
                        AND mp.is_current   = TRUE
                        AND pmp.is_active   = TRUE
                  )
            ) WHERE _rn = 1 AND platform_id IS NOT NULL
        )
        SELECT f.platform_id,
               SUM(f.spend)  AS spend,
               MIN(f.date)   AS first_date,
               MAX(f.date)   AS last_date
        FROM {bq.table('fact_digital_daily')} f
        WHERE f.project_code = @project_code
          AND f.platform_id IS NOT NULL
          AND f.platform_id NOT IN (SELECT platform_id FROM tracked_platforms)
          {date_clause}
        GROUP BY f.platform_id
        HAVING SUM(f.spend) > 0
        ORDER BY spend DESC
    """
    rows = bq.run_query(sql, params)
    return [
        UntrackedPlatformSpend(
            platform_id=r["platform_id"],
            spend=_float(r.get("spend")),
            first_date=str(r["first_date"]) if r.get("first_date") else None,
            last_date=str(r["last_date"]) if r.get("last_date") else None,
        )
        for r in rows
    ]


def _query_direct_lines(project_code: str) -> list[DirectLine]:
    """bcdirect: direct-buy lines (``media_plan_lines.is_direct = TRUE``) for a
    project — budgeted lines with NO self-serve spend feed (CTV, DOOH direct,
    LED truck, transit, …). These are EXCLUDED from pacing, so they never
    appear in budget_tracking; we read them straight off media_plan_lines and
    surface them as budget CONTEXT (no pacing %, no alarms).

    Uses the standard ROW_NUMBER dedup + plan_id IN (current media_plans ×
    active project_media_plans) guard — identical to
    ``_query_untracked_platform_spend`` above and feedback_mpl_dedup.md — so a
    stale sync version or a retired phase can't inflate the list. COALESCE
    guards the migration window where pre-resync rows carry is_direct = NULL
    (treated as not-direct, i.e. excluded here — they pace instead).
    """
    sql = f"""
        SELECT line_id, site_network, platform_id, budget, audience_name, is_direct_override
        FROM (
            SELECT
                l.line_id,
                l.site_network,
                l.platform_id,
                l.budget,
                l.audience_name,
                l.is_direct_override,
                ROW_NUMBER() OVER (
                    PARTITION BY l.line_id
                    ORDER BY l.sync_version DESC
                ) AS _rn
            FROM {bq.table('media_plan_lines')} l
            WHERE l.project_code = @project_code
              AND COALESCE(l.is_direct_override, l.is_direct, FALSE) = TRUE
              -- Plan-id-aware + multi-plan dedup guard (see top of this router
              -- for the canonical comment).
              AND l.plan_id IN (
                  SELECT mp.plan_id
                  FROM {bq.table('media_plans')} mp
                  JOIN {bq.table('project_media_plans')} pmp
                    ON mp.project_code = pmp.project_code
                   AND mp.sheet_id   = pmp.sheet_id
                  WHERE mp.project_code = @project_code
                    AND mp.is_current   = TRUE
                    AND pmp.is_active   = TRUE
              )
        )
        WHERE _rn = 1
        ORDER BY budget DESC
    """
    rows = bq.run_query(sql, [bq.string_param("project_code", project_code)])
    return [
        DirectLine(
            line_id=r.get("line_id"),
            label=(
                r.get("audience_name")
                or r.get("site_network")
                or r.get("platform_id")
                or "Direct buy"
            ),
            platform=r.get("site_network") or r.get("platform_id"),
            budget=_float(r.get("budget")),
            audience=r.get("audience_name"),
            is_direct_override=r.get("is_direct_override"),
        )
        for r in rows
    ]


def _attach_plan_metadata(project_code: str, replay_lines: list[dict]) -> list[dict]:
    """Join media-plan metadata onto replayed pacing rows (AI-070/072).

    ``run_pacing_for_project``'s tracking rows match the budget_tracking
    column schema, but the stored read path also picks up audience_name /
    flight dates / sheet+phase metadata from its mpl_dedup LEFT JOIN. This
    helper runs the same dedup CTE standalone and merges the metadata onto
    the in-memory replay rows so both paths produce an identical response
    shape.

    Replay only happens on retrospective requests, so this uses the
    retro-loosened phase filter (no ``pmp.is_active`` restriction) — same
    reasoning as the stored retro path: a phase retired today was likely
    active on the replay date.
    """
    metadata_sql = f"""
        SELECT * EXCEPT(_rn) FROM (
            SELECT
                l.line_id,
                l.audience_name,
                l.flight_start,
                l.flight_end,
                mp.sheet_id        AS sheet_id,
                pmp.phase_label    AS phase_label,
                pmp.display_order  AS phase_display_order,
                pmp.is_active      AS phase_is_active,
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
            WHERE l.project_code = @project_code
        ) WHERE _rn = 1
    """
    meta_rows = bq.run_query(
        metadata_sql, [bq.string_param("project_code", project_code)]
    )
    meta_by_line = {m["line_id"]: m for m in meta_rows}

    merged = []
    for r in replay_lines:
        meta = meta_by_line.get(r.get("line_id"), {})
        merged.append({
            **r,
            "audience_name": meta.get("audience_name"),
            "flight_start": meta.get("flight_start"),
            "flight_end": meta.get("flight_end"),
            "sheet_id": meta.get("sheet_id"),
            "phase_label": meta.get("phase_label"),
            "phase_display_order": meta.get("phase_display_order"),
            "phase_is_active": meta.get("phase_is_active"),
        })

    # Match the stored path's ORDER BY: phase_display_order NULLS LAST,
    # sheet_id, platform_id, bundle_id, line_code.
    def _key(r: dict):
        return (
            r.get("phase_display_order") is None,
            r.get("phase_display_order") or 0,
            r.get("sheet_id") or "",
            r.get("platform_id") or "",
            r.get("bundle_id") or "",
            r.get("line_code") or "",
        )

    merged.sort(key=_key)
    return merged


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

    # Multi-plan retrospective: a phase that's been retired today
    # (project_media_plans.is_active = FALSE) was likely active on a past
    # date. Live mode keeps the strict filter so retired phases drop out;
    # retrospective mode loosens it so historical replay can still attribute
    # past lines to their phase. The strict mode is correct in live because
    # the dedup guard's primary job is dropping stale plan_ids — the active
    # check is incidental on the live path but load-bearing in retro.
    pmp_active_filter = (
        "AND pmp.is_active = TRUE" if as_of_date is None else ""
    )

    tracking_sql = f"""
        WITH mpl_dedup AS (
            SELECT * EXCEPT(_rn) FROM (
                SELECT
                    l.line_id,
                    l.audience_name,
                    l.is_direct_override,
                    l.flight_start,
                    l.flight_end,
                    -- Multi-plan: phase metadata flows from media_plans.sheet_id
                    -- through to the join table for the human label and order.
                    mp.sheet_id        AS sheet_id,
                    pmp.phase_label    AS phase_label,
                    pmp.display_order  AS phase_display_order,
                    pmp.is_active      AS phase_is_active,
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
                 {pmp_active_filter}
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
            bt.is_not_reporting,
            bt.is_estimate,
            mpl.audience_name,
            mpl.flight_start,
            mpl.flight_end,
            mpl.sheet_id,
            mpl.phase_label,
            mpl.phase_display_order,
            mpl.is_direct_override
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

    # AI-002: surface spend on platforms with no media plan line. Runs on
    # every path (including the empty fallback) so a project whose pacing
    # engine never ran but which has fact spend never shows $0.
    untracked = _query_untracked_platform_spend(project_code, as_of_date)
    untracked_total = sum(u.spend for u in untracked)

    # bcdirect: direct buys (is_direct lines) — excluded from pacing, surfaced
    # as budget context. Computed on every path (incl. the empty fallback) so a
    # project with direct buys but no pacing snapshot still shows them. Read off
    # media_plan_lines (point-in-time independent), so it's not as_of-date-aware.
    direct_lines = _query_direct_lines(project_code)
    direct_budget = sum(d.budget for d in direct_lines)

    replayed = False
    if not rows and as_of_date is not None:
        # AI-070/072: retrospective request for a date with no stored
        # budget_tracking snapshot (e.g. the project was registered after
        # that date, or the daily pipeline missed a day). Mirror the
        # diagnostics snapshot semantics: compute a point-in-time replay on
        # demand. skip_writes keeps budget_tracking and alerts untouched.
        try:
            replay = run_pacing_for_project(
                project_code, as_of_date, skip_writes=True,
            )
            replay_lines = replay.get("lines") or []
        except Exception:
            logger.exception(
                "Pacing compute-on-miss replay failed for %s @ %s; "
                "falling back to the empty state",
                project_code, as_of_date,
            )
            replay_lines = []
        if replay_lines:
            # tracking_rows match budget_tracking columns; join the plan
            # metadata the stored path gets from its mpl_dedup LEFT JOIN.
            rows = _attach_plan_metadata(project_code, replay_lines)
            replayed = True

    if not rows:
        # AI-070/071: honest empty state. Echo the REQUESTED date (never
        # today) and tell the frontend when snapshots begin so it can render
        # "No pacing snapshot for this date — snapshots begin YYYY-MM-DD".
        earliest = bq.run_query(
            f"SELECT MIN(date) AS d FROM {bq.table('budget_tracking')} "
            "WHERE project_code = @project_code",
            [bq.string_param("project_code", project_code)],
        )
        earliest_date = (
            earliest[0]["d"] if earliest and earliest[0].get("d") else None
        )
        return PacingResponse(
            project_code=project_code,
            as_of_date=as_of_date or date.today(),
            net_budget=net_budget,
            snapshot_missing=True,
            earliest_snapshot_date=earliest_date,
            untracked_spend=untracked_total,
            untracked_platforms=untracked,
            total_actual_all_platforms=untracked_total,
            direct_budget=direct_budget,
            direct_lines=direct_lines,
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

    # P-FRESH-PACE: not_reporting + residual-estimate lines are held out of the
    # pacing RATIO (both numerator and denominator). A not_reporting line stopped
    # reporting mid-flight (its frozen actual would inflate the ratio while its
    # planned=0 offers no baseline); a residual-estimate line's spend is a
    # budget-weight guess, not a measurement. Both still count toward the
    # displayed total_actual_to_date — only the pacing % excludes them.
    def _in_ratio(r: dict) -> bool:
        return not r.get("is_not_reporting") and not r.get("is_estimate")

    total_planned = sum(
        _float(r.get("planned_spend_to_date")) for r in active_rows if _in_ratio(r)
    )
    total_actual = sum(_float(r.get("actual_spend_to_date")) for r in active_rows)

    # Pacing % is computed only over lines that have a planned baseline to pace
    # against. A line with spend but planned_spend_to_date == 0 (degenerate
    # flight dates, or a grid/flight disagreement the baseline floor could not
    # resolve) would otherwise add to the numerator while contributing nothing
    # to the denominator, inflating overall_pacing_percentage. Its spend still
    # counts toward total_actual_to_date (the displayed "spent" figure); only
    # the ratio excludes it. total_planned already excludes these lines, since a
    # zero baseline contributes 0 to that sum.
    paced_actual = sum(
        _float(r.get("actual_spend_to_date"))
        for r in active_rows
        if _in_ratio(r) and _float(r.get("planned_spend_to_date")) > 0
    )

    # P-FRESH-PACE: the ratio's denominator can collapse to 0 because EVERY
    # in-flight line is held out (all not_reporting / estimate) — not because
    # the campaign is genuinely at 0% spend. Without distinguishing the two, the
    # frontend's pacingStatus(0) paints an alarming red "critical-under 0.0%",
    # the exact false verdict this PR removes. Flag the hold-out case so the UI
    # renders the Overall Pacing tile neutrally instead. (The existing
    # `unattributedSpend` guard can't catch it — not_reporting lines keep a
    # nonzero frozen actual, so noLineSpend is false.)
    ratio_excluded_all = bool(active_rows) and all(
        not _in_ratio(r) for r in active_rows
    )

    # (c) Surface, rather than silently drop, lines that are SPENDING with no
    # planned baseline (active, actual>0, planned<=0). The guard above correctly
    # keeps them out of the % (a zero baseline can't pace), but their spend would
    # then vanish from the headline. Expose the count + amount so the UI can say
    # "$X spending with no baseline (data settling)" instead of reading near
    # zero. In a healthy state the baseline floor prevents this; a nonzero count
    # flags a transient/bad snapshot (e.g. a pace that ran mid-sync).
    # A not_reporting line also has planned<=0 with actual>0, but it is NOT a
    # settling-data gap — it stopped reporting. Exclude it here so the "spending
    # with no baseline" warning doesn't fire for it; the StalenessNote covers it.
    spend_without_baseline = sum(
        _float(r.get("actual_spend_to_date"))
        for r in active_rows
        if _float(r.get("planned_spend_to_date")) <= 0
        and _float(r.get("actual_spend_to_date")) > 0
        and not r.get("is_not_reporting")
    )
    lines_without_baseline = sum(
        1 for r in active_rows
        if _float(r.get("planned_spend_to_date")) <= 0
        and _float(r.get("actual_spend_to_date")) > 0
        and not r.get("is_not_reporting")
    )

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
            # phase_is_active is None on the live path's column read because
            # the JOIN filters those rows out — default to True for safety.
            "is_active": bool(r.get("phase_is_active") if r.get("phase_is_active") is not None else True),
            "line_count": 0,
            "planned_budget": 0.0,
            "planned_spend_to_date": 0.0,
            "actual_spend_to_date": 0.0,
            # Spend from lines that have a planned baseline (planned>0); the
            # numerator for phase pacing %, mirroring the project-level guard.
            "paced_actual_spend": 0.0,
        })
        bucket["line_count"] += 1
        bucket["planned_budget"] += _float(r.get("planned_budget"))
        # Displayed actual includes held-out lines (mirrors total_actual_to_date).
        bucket["actual_spend_to_date"] += _float(r.get("actual_spend_to_date"))
        # P-FRESH-PACE: phase pacing % must use the SAME hold-out as the
        # project-level Overall Pacing, or a phase pill diverges from the KPI
        # for identical data (#121 differs-between-tabs). not_reporting lines
        # already have planned=0, but an is_estimate line keeps a nonzero
        # baseline — exclude both from the phase numerator AND denominator.
        if _in_ratio(r):
            bucket["planned_spend_to_date"] += _float(r.get("planned_spend_to_date"))
            if _float(r.get("planned_spend_to_date")) > 0:
                bucket["paced_actual_spend"] += _float(r.get("actual_spend_to_date"))

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
                    round(p["paced_actual_spend"] / p["planned_spend_to_date"] * 100, 1)
                    if p["planned_spend_to_date"]
                    else 0
                ),
                is_active=p["is_active"],
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
        overall_pacing_percentage=round(paced_actual / total_planned * 100, 1) if total_planned else 0,
        # AI-002: untracked spend is included in the spent/remaining math
        # (conservative — never overstate remaining budget) but EXCLUDED from
        # overall_pacing_percentage (no planned baseline to pace against).
        untracked_spend=untracked_total,
        untracked_platforms=untracked,
        total_actual_all_platforms=total_actual + untracked_total,
        # bcdirect: direct buys (is_direct), excluded from pacing, surfaced as
        # budget context only (no pacing %, no alarms).
        direct_budget=direct_budget,
        direct_lines=direct_lines,
        # AI-070/072: True when these rows were computed on demand rather
        # than read from a stored budget_tracking snapshot.
        replayed=replayed,
        pending_line_count=pending_count,
        spend_without_baseline=spend_without_baseline,
        lines_without_baseline=lines_without_baseline,
        # P-FRESH-PACE: every in-flight line is held out of the ratio → render
        # the Overall Pacing tile neutrally, not a red 0.0%.
        ratio_excluded_all=ratio_excluded_all,
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
                # P-FRESH-PACE: surface the ratio hold-out flags so the UI can
                # render the StalenessNote and mark held-out lines.
                is_not_reporting=bool(r.get("is_not_reporting"))
                or r.get("line_status") == "not_reporting",
                is_estimate=bool(r.get("is_estimate")),
                bundle_id=r.get("bundle_id"),
                bundle_role=r.get("bundle_role"),
                bundle_members=bundle_members_by_id.get(r.get("bundle_id") or "", []),
                sheet_id=r.get("sheet_id"),
                phase_label=r.get("phase_label"),
                phase_display_order=r.get("phase_display_order"),
                is_direct_override=r.get("is_direct_override"),
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
    # Bulk re-pace rewrote budget_tracking across the board — drop all rollup.
    projects_router.invalidate_all()
    return result


@router.post("/{project_code}/run")
async def run_pacing_single(project_code: str):
    """Trigger pacing calculation for a single project as of today."""
    result = run_pacing_for_project(project_code, date.today())
    # Re-pace rewrote this project's budget_tracking — drop its stale rollup.
    projects_router.invalidate_project(project_code)
    return result
