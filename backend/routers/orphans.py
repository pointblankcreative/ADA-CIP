"""Orphan Project Auto-Discovery endpoints.

Surfaces project_codes with spend/activity in fact_* tables that are NOT in
dim_projects and have not been dismissed. The Overview page uses these to
render a panel with Configure / Dismiss CTAs.

- Configure is a pure frontend redirect to ``/admin/projects/new?code=X`` —
  no endpoint needed here; the existing admin create-project flow prefills
  the code and handles the rest.
- Dismiss / un-dismiss persist to the ``dismissed_orphans`` BigQuery table.
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

from backend.services import orphans as svc

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/orphan-projects", tags=["orphans"])


# ── Request/response schemas ────────────────────────────────────────────────


class OrphanDismissRequest(BaseModel):
    reason: str | None = None


# ── Endpoints ───────────────────────────────────────────────────────────────


@router.get("")
async def list_orphan_projects(include_dismissed: bool = False):
    """List orphan project_codes (spend in fact_* not in dim_projects).

    Query params:
    - ``include_dismissed`` (default false) — include dismissed orphans in
      the response. Dismissed entries have ``dismissed=true`` and a
      ``dismissed_at`` timestamp.
    """
    try:
        rows = svc.scan_orphans(include_dismissed=include_dismissed)
        return {"orphans": rows, "count": len(rows)}
    except Exception as e:
        logger.exception("Failed to scan orphan projects")
        raise HTTPException(500, f"Failed to scan orphan projects: {e}")


@router.post("/{project_code}/dismiss")
async def dismiss_orphan(project_code: str, body: OrphanDismissRequest, request: Request):
    """Mark an orphan project_code as dismissed.

    Dismissal is permanent until someone calls the un-dismiss endpoint.
    Idempotent: repeat calls update the reason + timestamp but don't error.
    """
    user = getattr(request.state, "user", None) or {}
    dismissed_by = user.get("email") if isinstance(user, dict) else None

    try:
        return svc.dismiss(
            project_code=project_code,
            dismissed_by=dismissed_by,
            reason=body.reason,
        )
    except Exception as e:
        logger.exception("Failed to dismiss orphan %s", project_code)
        raise HTTPException(500, f"Failed to dismiss orphan: {e}")


@router.post("/{project_code}/undismiss")
async def undismiss_orphan(project_code: str):
    """Remove a dismissal. The project_code will surface again on next scan."""
    try:
        removed = svc.undismiss(project_code)
    except Exception as e:
        logger.exception("Failed to undismiss orphan %s", project_code)
        raise HTTPException(500, f"Failed to undismiss orphan: {e}")

    if not removed:
        raise HTTPException(404, "No dismissal found for that project_code")
    return {"status": "undismissed", "project_code": project_code}
