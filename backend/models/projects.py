import re
from datetime import date, datetime

from pydantic import BaseModel, field_validator


class ProjectSummary(BaseModel):
    project_code: str
    project_name: str
    client_name: str | None = None
    status: str = "active"
    start_date: date | None = None
    end_date: date | None = None
    net_budget: float | None = None
    total_spend: float | None = None
    pacing_percentage: float | None = None
    days_remaining: int | None = None
    recently_ended: bool = False
    updated_at: datetime | None = None


class ProjectDetail(ProjectSummary):
    client_id: str | None = None
    campaign_type: str | None = None
    currency: str = "CAD"
    platforms_active: int = 0
    first_data_date: date | None = None
    last_data_date: date | None = None
    media_plan_sheet_id: str | None = None
    media_plan_tab_name: str | None = None
    slack_channel_id: str | None = None
    created_at: datetime | None = None


class ProjectCreateRequest(BaseModel):
    project_code: str
    client_name: str
    project_name: str
    start_date: date
    end_date: date
    net_budget: float
    media_plan_sheet_url: str | None = None
    media_plan_tab_name: str | None = None
    slack_channel_id: str | None = None

    @field_validator("project_code")
    @classmethod
    def validate_project_code(cls, v: str) -> str:
        if not re.match(r"^2[0-9]\d{3}$", v):
            raise ValueError("Project code must be YYNNN format (e.g. 25013, 26009)")
        return v

    @field_validator("net_budget")
    @classmethod
    def validate_budget_positive(cls, v: float) -> float:
        if v <= 0:
            raise ValueError("Net budget must be a positive number")
        return v

    @field_validator("end_date")
    @classmethod
    def validate_date_order(cls, v: date, info) -> date:
        start = info.data.get("start_date")
        if start and v <= start:
            raise ValueError("End date must be after start date")
        return v


class ProjectUpdateRequest(BaseModel):
    project_name: str | None = None
    start_date: date | None = None
    end_date: date | None = None
    net_budget: float | None = None
    status: str | None = None
    slack_channel_id: str | None = None
    media_plan_sheet_url: str | None = None
    media_plan_tab_name: str | None = None

    @field_validator("status")
    @classmethod
    def validate_status(cls, v: str | None) -> str | None:
        if v is not None and v not in ("planning", "active", "paused", "completed"):
            raise ValueError("Status must be one of: planning, active, paused, completed")
        return v


class AdminProjectResponse(ProjectDetail):
    """Extended project response with admin-only fields."""
    media_plan_synced: bool = False
    alert_count: int = 0
