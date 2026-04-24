"""Retrospective Mode router (ADAC-51).

Exposes a single endpoint that replays the diagnostic engine + pacing
calculations against a past date. Returns the same diagnostic shape the live
endpoint does so the frontend can reuse its existing DiagnosticOutput types
(frontend work in commits 6-8).

URL shape
---------

``GET /api/diagnostics/as-of/{as_of_date}/project/{project_code}``

Path params (not query) so the URL is bookmarkable and unambiguous. Per
Frazer's call on question 1 of the build-plan open questions: sharing the
link in Slack should make it obvious what you're looking at.

Response shape
--------------

::

    {
      "project_code": "25013",
      "as_of_date": "2026-03-01",
      "engine_version": "sha-abc123",
      "cached": true | false,
      "diagnostics": [ <DiagnosticOutput dict>, ... ],
      "pacing": { <run_pacing_for_project return value> }
    }

``diagnostics`` is 0..2 items (0 if the project has no media plan / no
derivable flight on ``as_of_date``; 1 pure; 2 mixed). Shape matches what
``/api/diagnostics/{code}`` returns today.

``pacing`` is the dict returned by ``pacing.run_pacing_for_project``. It's
computed fresh every call with ``skip_writes=True`` so retrospective views
don't corrupt budget_tracking. Not cached because budget_tracking already
stores point-in-time rows — one BQ lookup is cheap.

``cached`` is ``True`` if the diagnostics came from a fact_diagnostic_signals
snapshot under the current engine_version, ``False`` if the engine was
invoked to produce them. Useful for the frontend to show a "cached X ago"
indicator and for diagnosing cache behaviour in dev.
"""

from __future__ import annotations

import logging
from datetime import date
from typing import Any

from fastapi import APIRouter, HTTPException

from backend.config import settings
from backend.routers.diagnostics import _row_to_diagnostic
from backend.services import snapshots
from backend.services.pacing import run_pacing_for_project

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/diagnostics/as-of", tags=["retrospective"])


@router.get("/{as_of_date}/project/{project_code}")
async def get_retrospective(
    as_of_date: date,
    project_code: str,
) -> dict[str, Any]:
    """Replay diagnostics + pacing for ``project_code`` as of ``as_of_date``.

    Diagnostics path:
      - ``snapshots.find_or_compute`` looks up a cached row from
        fact_diagnostic_signals matching
        (project_code, as_of_date, settings.engine_version). Cache miss
        triggers a full engine run, which persists its outputs so the next
        call hits the cache.

    Pacing path:
      - ``run_pacing_for_project(project_code, as_of_date, skip_writes=True)``
        computes a fresh pacing view. ``skip_writes=True`` is load-bearing:
        without it we'd write today-shaped reconstructed rows into
        budget_tracking and fire Slack alerts about a past snapshot.

    Both paths tolerate projects with no media plan: diagnostics returns an
    empty list (``cached=False``), pacing returns its usual lines_processed=0
    shape.
    """
    # FastAPI's date converter validates the path segment is a real ISO date
    # and raises 422 automatically on bad input. So inside the handler we can
    # assume ``as_of_date`` is a valid date object.
    try:
        diag_rows, cached = snapshots.find_or_compute(project_code, as_of_date)
    except Exception as e:
        logger.error(
            "Retrospective diagnostics failed for %s @ %s: %s",
            project_code, as_of_date, e, exc_info=True,
        )
        raise HTTPException(500, f"Retrospective diagnostics failed: {e}")

    try:
        pacing_result = run_pacing_for_project(
            project_code, as_of_date, skip_writes=True,
        )
    except Exception as e:
        logger.error(
            "Retrospective pacing failed for %s @ %s: %s",
            project_code, as_of_date, e, exc_info=True,
        )
        raise HTTPException(500, f"Retrospective pacing failed: {e}")

    return {
        "project_code": project_code,
        "as_of_date": as_of_date.isoformat(),
        "engine_version": settings.engine_version,
        "cached": cached,
        "diagnostics": [_row_to_diagnostic(r) for r in diag_rows],
        "pacing": pacing_result,
    }
