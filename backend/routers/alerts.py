import json

from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel, field_validator

from backend.models.alerts import AlertResponse
from backend.services import bigquery_client as bq

router = APIRouter(prefix="/api/alerts", tags=["alerts"])


@router.get("/", response_model=list[AlertResponse])
async def list_alerts(
    project_code: str | None = Query(None),
    severity: str | None = Query(None),
    acknowledged: bool | None = Query(None),
    limit: int = Query(100, ge=1, le=500),
):
    conditions = ["1=1"]
    params = []

    if project_code:
        conditions.append("a.project_code = @project_code")
        params.append(bq.string_param("project_code", project_code))
    if severity:
        conditions.append("a.severity = @severity")
        params.append(bq.string_param("severity", severity))
    if acknowledged is True:
        conditions.append("a.acknowledged_at IS NOT NULL")
    elif acknowledged is False:
        conditions.append("a.acknowledged_at IS NULL")

    where = " AND ".join(conditions)
    sql = f"""
        SELECT
            a.alert_id,
            a.project_code,
            a.alert_type,
            a.severity,
            a.title,
            a.message,
            a.metadata,
            a.created_at,
            a.acknowledged_at,
            a.acknowledged_by,
            a.ack_note,
            a.resolved_at,
            a.slack_sent
        FROM {bq.table('alerts')} a
        WHERE {where}
        ORDER BY a.created_at DESC
        LIMIT @limit
    """
    params.append(bq.scalar_param("limit", "INT64", limit))
    rows = bq.run_query(sql, params)

    return [
        AlertResponse(
            alert_id=r["alert_id"],
            project_code=r.get("project_code"),
            alert_type=r["alert_type"],
            severity=r["severity"],
            title=r["title"],
            message=r["message"],
            metadata=json.loads(r["metadata"]) if r.get("metadata") else None,
            created_at=r["created_at"],
            acknowledged_at=r.get("acknowledged_at"),
            acknowledged_by=r.get("acknowledged_by"),
            ack_note=r.get("ack_note"),
            resolved_at=r.get("resolved_at"),
            slack_sent=bool(r.get("slack_sent")),
        )
        for r in rows
    ]


@router.post("/dispatch")
async def dispatch_alerts():
    """Send unsent alerts to Slack."""
    from backend.services.slack_alerts import dispatch_unsent_alerts
    result = dispatch_unsent_alerts()
    return result


@router.post("/daily-digest")
async def daily_digest():
    """Post the daily digest summary to Slack."""
    from backend.services.slack_alerts import post_daily_digest
    result = post_daily_digest()
    return result


class AcknowledgePayload(BaseModel):
    """Optional body for acknowledge — a free-text note recording what the
    user did in response ("lowered Meta daily caps", "paused campaign")."""

    note: str | None = None

    @field_validator("note")
    @classmethod
    def _trim_note(cls, v: str | None) -> str | None:
        if v is None:
            return None
        v = v.strip()
        if len(v) > 1000:
            raise ValueError("note must be 1000 characters or fewer")
        return v or None


def _resolve_user_email(request: Request) -> str | None:
    """Pull the IAP-attached user email if available; None for the dev stub.
    Mirrors backend/routers/admin.py — a future RBAC pass should centralise
    this."""
    user = getattr(request.state, "user", None) or {}
    return user.get("email") if isinstance(user, dict) else None


@router.post("/{alert_id}/acknowledge")
async def acknowledge_alert(
    alert_id: str,
    request: Request,
    payload: AcknowledgePayload | None = None,
):
    """Mark an alert as acknowledged, recording who did it (IAP identity)
    and an optional note about the action taken. Idempotent: a second call
    on an already-acknowledged alert changes nothing."""
    acknowledged_by = _resolve_user_email(request) or "api"
    note = payload.note if payload else None

    sql = f"""
        UPDATE {bq.table('alerts')}
        SET acknowledged_at = CURRENT_TIMESTAMP(),
            acknowledged_by = @acknowledged_by,
            ack_note = @note
        WHERE alert_id = @alert_id
            AND acknowledged_at IS NULL
    """
    bq.run_query(
        sql,
        [
            bq.string_param("alert_id", alert_id),
            bq.string_param("acknowledged_by", acknowledged_by),
            bq.string_param("note", note),
        ],
    )
    return {
        "alert_id": alert_id,
        "acknowledged": True,
        "acknowledged_by": acknowledged_by,
        "ack_note": note,
    }
