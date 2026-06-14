from datetime import date

from pydantic import BaseModel


class BundleMember(BaseModel):
    """A sibling audience inside a CBO-style bundle. The bundle's parent line
    carries the shared budget + pacing signal; members are surfaced here so
    the UI can render them as expandable rows under the parent."""

    line_id: str
    line_code: str | None = None
    audience_name: str | None = None


class LinePacing(BaseModel):
    line_id: str
    line_code: str | None = None
    platform_id: str | None = None
    channel_category: str | None = None
    audience_name: str | None = None
    flight_start: str | None = None
    flight_end: str | None = None
    line_status: str = "active"  # not_started | pending | active | completed
    planned_budget: float = 0
    planned_spend_to_date: float = 0
    actual_spend_to_date: float = 0
    remaining_budget: float = 0
    remaining_days: int = 0
    pacing_percentage: float = 0
    daily_budget_required: float | None = None
    is_over_pacing: bool = False
    is_under_pacing: bool = False
    # PR 5: bundled-optimization context. NULL for standalone lines.
    bundle_id: str | None = None
    bundle_role: str | None = None  # suggested_parent | suggested_child | confirmed_* | rejected
    bundle_members: list[BundleMember] = []  # populated only on parent rows
    # Multi-plan support (2026-04-25): which sheet/phase this line came from.
    # Single-plan projects get a single phase row with phase_label=None and a
    # stable sheet_id. The frontend groups by sheet_id and renders phase_label
    # (or "Phase {display_order}" as a fallback) as the section header.
    sheet_id: str | None = None
    phase_label: str | None = None
    phase_display_order: int | None = None


class UntrackedPlatformSpend(BaseModel):
    """Spend present in fact_digital_daily for a platform that has NO line in
    the current, active media plan (AI-002 / AI-022). Surfaced so the Pacing
    tab never silently hides real spend — there is no planned baseline, so
    these carry spend only (no pacing %)."""

    platform_id: str
    spend: float = 0
    first_date: str | None = None
    last_date: str | None = None


class DirectLine(BaseModel):
    """A direct-buy media plan line (``media_plan_lines.is_direct = TRUE``):
    a budgeted line with NO self-serve spend feed (CTV, DOOH direct, building
    projection, LED truck, transit, …). These are excluded from pacing — they
    can never produce budget_tracking rows or alarms — so they're surfaced
    here purely as budget CONTEXT (managed directly, not tracked in ADA)."""

    label: str
    platform: str | None = None
    budget: float = 0
    audience: str | None = None


class PhaseSummary(BaseModel):
    """Aggregate roll-up for one phase (one project_media_plans row).

    Surfaced alongside ``PacingResponse.lines`` so the UI can render the
    phase header card without recomputing totals on the client.
    """
    sheet_id: str
    phase_label: str | None = None
    display_order: int | None = None
    line_count: int = 0
    planned_budget: float = 0
    planned_spend_to_date: float = 0
    actual_spend_to_date: float = 0
    pacing_percentage: float = 0
    # True when project_media_plans.is_active for this sheet. Live mode never
    # returns retired phases (filtered upstream); retrospective mode does, so
    # the UI can mark them as historical.
    is_active: bool = True


class PacingResponse(BaseModel):
    project_code: str
    as_of_date: date
    net_budget: float = 0
    total_planned_to_date: float = 0
    total_actual_to_date: float = 0
    overall_pacing_percentage: float = 0
    pending_line_count: int = 0  # C2: count of lines excluded from overall pacing
    # AI-002: spend on platforms with no media plan line. Included in the
    # spent/remaining math (conservative — never overstate remaining budget)
    # but EXCLUDED from overall_pacing_percentage (no planned baseline).
    untracked_spend: float = 0
    untracked_platforms: list[UntrackedPlatformSpend] = []
    # total_actual_to_date + untracked_spend. The number the header's
    # fact-table total should reconcile against.
    total_actual_all_platforms: float = 0
    # Direct buys (media_plan_lines.is_direct = TRUE): budgeted lines with no
    # self-serve feed, EXCLUDED from pacing. Surfaced as budget context only —
    # no pacing %, no over/under alarms. Additive + optional (same back-compat
    # pattern as untracked_spend) so a not-yet-redeployed frontend keeps working.
    direct_budget: float = 0
    direct_lines: list[DirectLine] = []
    # AI-070/071: explicit empty-state signalling for retrospective requests.
    # True when no stored budget_tracking row exists for the requested date
    # AND a compute-on-miss replay was impossible (no plan / no data). The
    # frontend renders "No pacing snapshot for this date — snapshots begin
    # {earliest_snapshot_date}" instead of dishonest zeros.
    snapshot_missing: bool = False
    earliest_snapshot_date: date | None = None
    # True when the rows were computed on demand (replay) rather than read
    # from a stored budget_tracking snapshot. Mirrors diagnostics' `cached`.
    replayed: bool = False
    lines: list[LinePacing] = []
    # Multi-plan support (2026-04-25): one entry per active sheet, ordered for
    # display. Single-plan projects get a one-element list. Empty when the
    # project has no registered phases yet (legacy fallback path).
    phases: list[PhaseSummary] = []


class PacingHistoryPoint(BaseModel):
    date: str
    line_id: str
    pacing_percentage: float


class PacingHistoryResponse(BaseModel):
    project_code: str
    history: list[PacingHistoryPoint] = []
