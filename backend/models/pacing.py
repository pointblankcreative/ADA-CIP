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


class PacingResponse(BaseModel):
    project_code: str
    as_of_date: date
    net_budget: float = 0
    total_planned_to_date: float = 0
    total_actual_to_date: float = 0
    overall_pacing_percentage: float = 0
    pending_line_count: int = 0  # C2: count of lines excluded from overall pacing
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
