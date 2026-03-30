from datetime import date

from pydantic import BaseModel


class LinePacing(BaseModel):
    line_id: str
    line_code: str | None = None
    platform_id: str | None = None
    channel_category: str | None = None
    planned_budget: float = 0
    planned_spend_to_date: float = 0
    actual_spend_to_date: float = 0
    remaining_budget: float = 0
    remaining_days: int = 0
    pacing_percentage: float = 0
    daily_budget_required: float | None = None
    is_over_pacing: bool = False
    is_under_pacing: bool = False


class PacingResponse(BaseModel):
    project_code: str
    as_of_date: date
    net_budget: float = 0
    total_planned_to_date: float = 0
    total_actual_to_date: float = 0
    overall_pacing_percentage: float = 0
    lines: list[LinePacing] = []
