"""Orphan Project Auto-Discovery endpoints.

Surfaces project_codes with spend/activity in fact_* tables that are NOT in
dim_projects and have not been suppressed. The Overview page renders a panel
with a single **Configure** CTA (a pure frontend redirect to
``/admin/projects/new?code=X``; no endpoint needed here).

Suppression is intentionally not exposed over HTTP. To stop a code surfacing,
add a row to the ``dismissed_orphans`` control table in BigQuery directly:

    INSERT INTO `point-blank-ada.cip.dismissed_orphans`
      (project_code, dismissed_by, reason, level)
    VALUES ('25034', 'you@pointblankcreative.ca', 'old test account', 'dismissed');

``level`` is ``dismissed`` (hidden from the active panel, visible under "show
dismissed") or ``archived`` (hidden everywhere). This read-only surface means
no one can suppress a code by accident, and every suppression is an explicit,
attributable row.
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException

from backend.services import orphans as svc

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/orphan-projects", tags=["orphans"])


@router.get("")
async def list_orphan_projects(include_dismissed: bool = False):
    """List orphan project_codes (spend in fact_* not in dim_projects).

    Query params:
    - ``include_dismissed`` (default false) — also include codes set to
      ``level = 'dismissed'`` in the control table. Codes set to ``archived``
      are never returned.
    """
    try:
        rows = svc.scan_orphans(include_dismissed=include_dismissed)
        return {"orphans": rows, "count": len(rows)}
    except Exception as e:
        logger.exception("Failed to scan orphan projects")
        raise HTTPException(500, f"Failed to scan orphan projects: {e}")
