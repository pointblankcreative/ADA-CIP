from datetime import datetime

from pydantic import BaseModel


class AlertResponse(BaseModel):
    alert_id: str
    project_code: str | None = None
    alert_type: str
    severity: str
    title: str
    message: str
    metadata: dict | None = None
    created_at: datetime | None = None
    acknowledged_at: datetime | None = None
    acknowledged_by: str | None = None
    resolved_at: datetime | None = None
    slack_sent: bool = False
