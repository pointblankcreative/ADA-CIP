"""Snapshot cache for Retrospective Mode diagnostic replays (ADAC-51).

Retrospective Mode replays the diagnostic engine against a past date. Recomputing
from scratch on every request is wasteful and inconsistent — the engine's inputs
(fact_digital_daily, blocking chart, media plan) should be effectively immutable
for historical dates, so the output is too. We cache each computed replay in the
existing `fact_diagnostic_signals` table (keyed on engine_version, added in
commit 1 of this series) and look it up on subsequent requests.

Public API
----------

- ``find_snapshot(project_code, as_of_date, engine_version=None)`` — returns
  cached BigQuery rows for that key, or an empty list on miss. Pure projects
  return 1 row; mixed projects return 2 (one per campaign_type). When multiple
  computed_at rows exist for the same key, the latest wins.

- ``compute_and_store(project_code, as_of_date)`` — runs the engine and lets
  its existing ``_store_results`` pipeline write to fact_diagnostic_signals.
  Returns in-memory DiagnosticOutput objects.

- ``find_or_compute(project_code, as_of_date, engine_version=None, bypass_cache=False)``
  — the orchestrator used by the retrospective router (commit 5). Returns a
  list of dicts matching the BigQuery row shape, so the caller can pass them
  straight through ``_row_to_diagnostic``.

Cache-key semantics
-------------------

``engine_version`` is part of the key (not just as_of_date). When the engine
code changes, the cached row from the old SHA does NOT satisfy a request
computed under the new SHA — we recompute. In practice this means every
deploy invalidates the cache, which is fine: the live daily pipeline refills
as it runs, and retrospective queries for common dates will recompute on
first access and then be cached going forward.

Defaulting to ``settings.engine_version`` means a caller who doesn't specify
the version gets "today's code" semantics, which is almost always what they
want. The ADAC-37 historical backfill (commit 10) sets bypass_cache=True so
the batch always recomputes, avoiding accidental "Pre-ADA" cache hits.
"""

from __future__ import annotations

import logging
from datetime import date
from typing import Optional

from backend.config import settings
from backend.services import bigquery_client as bq
from backend.services.diagnostics.engine import run_diagnostics_for_project
from backend.services.diagnostics.models import DiagnosticOutput

logger = logging.getLogger(__name__)


def find_snapshot(
    project_code: str,
    as_of_date: date,
    engine_version: Optional[str] = None,
) -> list[dict]:
    """Look up cached diagnostic rows for (project_code, as_of_date, engine_version).

    Returns BQ row dicts, 0..2 per call. When ``engine_version`` is None, the
    current ``settings.engine_version`` is used (the most common caller
    intent: "give me the cached row for today's code if we've computed it").

    When multiple rows exist for the same (project, date, version, type) —
    e.g. because a retry happened after a partial failure — the latest
    ``computed_at`` wins.
    """
    version = engine_version or settings.engine_version

    sql = f"""
        WITH ranked AS (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY campaign_type
                       ORDER BY computed_at DESC
                   ) AS rn
            FROM {bq.table('fact_diagnostic_signals')}
            WHERE project_code = @project_code
              AND evaluation_date = @as_of_date
              AND engine_version = @engine_version
        )
        SELECT * EXCEPT (rn)
        FROM ranked
        WHERE rn = 1
        ORDER BY campaign_type
    """
    rows = bq.run_query(sql, [
        bq.string_param("project_code", project_code),
        bq.date_param("as_of_date", as_of_date),
        bq.string_param("engine_version", version),
    ])
    return rows


def compute_and_store(
    project_code: str,
    as_of_date: date,
) -> list[DiagnosticOutput]:
    """Compute fresh diagnostics for (project_code, as_of_date) and persist.

    Delegates to ``run_diagnostics_for_project`` which already writes to
    fact_diagnostic_signals via ``_store_results``. Returns in-memory
    DiagnosticOutput objects so the caller can serialize them immediately
    without a re-read.

    Intentionally does NOT call pacing — the retrospective endpoint handles
    pacing separately with ``skip_writes=True`` so it doesn't pollute
    budget_tracking.
    """
    outputs = run_diagnostics_for_project(project_code, as_of_date)
    return outputs


def find_or_compute(
    project_code: str,
    as_of_date: date,
    engine_version: Optional[str] = None,
    bypass_cache: bool = False,
) -> list[dict]:
    """Cache-or-compute for a single (project_code, as_of_date) replay.

    Returns a list of dicts matching the BigQuery row shape of
    fact_diagnostic_signals. The retrospective router (ADAC-51 commit 5)
    calls ``routers.diagnostics._row_to_diagnostic`` on each dict to get
    the API response shape.

    Empty list is returned if the project has no media plan / no derivable
    flight on ``as_of_date``.

    ``bypass_cache=True`` forces a recompute even if a cached row exists.
    Used by the ADAC-37 historical backfill (commit 10) so batch runs under
    a specific engine_version always get fresh results.
    """
    version = engine_version or settings.engine_version

    if not bypass_cache:
        cached = find_snapshot(project_code, as_of_date, engine_version=version)
        if cached:
            logger.info(
                "Snapshot cache hit: %s @ %s (engine_version=%s, rows=%d)",
                project_code, as_of_date, version, len(cached),
            )
            return cached

    logger.info(
        "Snapshot cache miss, computing: %s @ %s (engine_version=%s, bypass=%s)",
        project_code, as_of_date, version, bypass_cache,
    )
    outputs = compute_and_store(project_code, as_of_date)
    # Return the in-memory outputs serialized to the same dict shape BQ would
    # return. This keeps the caller's response-shaping path uniform whether
    # we hit the cache or computed fresh.
    return [output.to_bq_row() for output in outputs]
