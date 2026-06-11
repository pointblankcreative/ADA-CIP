"""Diagnostics API — surface the diagnostic signal engine output.

Three endpoints:
    GET  /api/diagnostics/{project_code}          — latest diagnostic (or ?date=)
    GET  /api/diagnostics/{project_code}/history  — trend data for sparklines
    POST /api/diagnostics/{project_code}/run      — manual trigger

Output is read back from fact_diagnostic_signals. The JSON columns
(pillars / signals / efficiency / alerts / platforms / line_ids) are
returned as-is — the frontend knows the shape.
"""

from __future__ import annotations

import json
import logging
from datetime import date
from typing import Any

from fastapi import APIRouter, HTTPException, Query

from backend.services import bigquery_client as bq
from backend.services.diagnostics.engine import run_diagnostics_for_project

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/diagnostics", tags=["diagnostics"])


# BigQuery returns JSON columns as Python objects already (via the REST API),
# but when serialized into a string this coerces them cleanly.
def _coerce_json(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return value
    return value


def _derive_health_coverage(pillars: dict) -> float | None:
    """Derive campaign-level coverage from per-pillar coverage (AI-040).

    Coverage lives inside the existing ``pillars`` JSON column (no new
    top-level BQ column), so the campaign-level number is computed here
    at read time:

        health_coverage = Σ(pillar_weight × pillar_coverage) / Σ(pillar_weight)

    Legacy rows (pre-coverage snapshots) lack the ``weight``/``coverage``
    keys → returns None and the frontend renders them exactly as before
    (no INSUFFICIENT DATA retro-blanking).
    """
    if not isinstance(pillars, dict):
        return None
    entries = [
        (p.get("weight"), p.get("coverage"))
        for p in pillars.values()
        if isinstance(p, dict) and p.get("weight") is not None
    ]
    if not entries or not any(c is not None for _, c in entries):
        return None
    total_w = sum(w for w, _ in entries)
    if total_w <= 0:
        return None
    return round(sum(w * (c or 0.0) for w, c in entries) / total_w, 3)


def _row_to_diagnostic(row: dict) -> dict:
    """Convert a fact_diagnostic_signals row into the API response shape."""
    pillars = _coerce_json(row.get("pillars")) or {}
    return {
        "id": row.get("id"),
        "project_code": row.get("project_code"),
        "campaign_type": row.get("campaign_type"),
        "evaluation_date": (
            row["evaluation_date"].isoformat()
            if row.get("evaluation_date") and hasattr(row["evaluation_date"], "isoformat")
            else row.get("evaluation_date")
        ),
        "flight_day": row.get("flight_day"),
        "flight_total_days": row.get("flight_total_days"),
        "health_score": row.get("health_score"),
        "health_status": row.get("health_status"),
        "health_coverage": _derive_health_coverage(pillars),
        "pillars": pillars,
        "signals": _coerce_json(row.get("signals")) or [],
        "efficiency": _coerce_json(row.get("efficiency")) or {},
        "alerts": _coerce_json(row.get("alerts")) or [],
        "platforms": _coerce_json(row.get("platforms")) or [],
        "line_ids": _coerce_json(row.get("line_ids")) or [],
        "computed_at": (
            row["computed_at"].isoformat()
            if row.get("computed_at") and hasattr(row["computed_at"], "isoformat")
            else row.get("computed_at")
        ),
        "spec_version": row.get("spec_version"),
    }


@router.get("/{project_code}")
async def get_diagnostics(
    project_code: str,
    evaluation_date: date | None = Query(None, alias="date"),
):
    """Return the latest diagnostic output for a project.

    If `date` is provided, return the diagnostic for that specific date.
    Otherwise, return the most recent per campaign_type.

    Response: list of DiagnosticOutput dicts (one per campaign_type).
    """
    base = f"""
        SELECT
            id, project_code, campaign_type, evaluation_date,
            flight_day, flight_total_days,
            health_score, health_status,
            pillars, signals, efficiency, alerts,
            platforms, line_ids,
            computed_at, spec_version
        FROM {bq.table('fact_diagnostic_signals')}
        WHERE project_code = @project_code
    """

    params = [bq.string_param("project_code", project_code)]

    if evaluation_date is not None:
        sql = base + "\n  AND evaluation_date = @eval_date\n ORDER BY campaign_type"
        params.append(bq.date_param("eval_date", evaluation_date))
    else:
        # Latest row per campaign_type
        sql = f"""
            WITH ranked AS (
                SELECT *,
                       ROW_NUMBER() OVER (
                           PARTITION BY campaign_type
                           ORDER BY evaluation_date DESC, computed_at DESC
                       ) AS rn
                FROM {bq.table('fact_diagnostic_signals')}
                WHERE project_code = @project_code
            )
            SELECT * EXCEPT (rn) FROM ranked WHERE rn = 1 ORDER BY campaign_type
        """

    rows = bq.run_query(sql, params)
    return [_row_to_diagnostic(r) for r in rows]


def _slim_signals(signals: Any) -> list[dict]:
    """Reduce a stored signals JSON array to sparkline-sized entries.

    The Triage Board (diagnostics redesign) needs per-signal score history
    for trends and ▲/▼ deltas. Full signal objects carry diagnostic prose
    and raw inputs — at 30 days × 12 signals that's needless payload, so
    history rows return just id / score / status per signal.
    """
    coerced = _coerce_json(signals)
    if not isinstance(coerced, list):
        return []
    return [
        {
            "id": s.get("id"),
            "score": s.get("score"),
            "status": s.get("status"),
        }
        for s in coerced
        if isinstance(s, dict) and s.get("id")
    ]


@router.get("/{project_code}/history")
async def get_diagnostic_history(
    project_code: str,
    days: int = Query(30, ge=1, le=365),
    campaign_type: str | None = Query(None),
    include_signals: bool = Query(
        False,
        description=(
            "Also return per-signal {id, score, status} for each evaluation "
            "— feeds the Triage Board's signal sparklines and deltas."
        ),
    ),
    as_of_date: date | None = Query(
        None,
        description=(
            "Anchor the history window at this date instead of today. "
            "Required by Retrospective Mode (ADAC-51) so a past-snapshot view "
            "can show the trailing N days ending at the replay date rather "
            "than today. Defaults to today when omitted."
        ),
    ),
):
    """Return health + pillar score history for sparklines.

    Window is ``[as_of_date - days, as_of_date]``. In live mode the anchor
    defaults to today; in retrospective mode it's the replay date.
    """
    anchor = as_of_date or date.today()
    signals_col = ",\n            signals" if include_signals else ""
    sql = f"""
        SELECT
            evaluation_date,
            campaign_type,
            health_score,
            health_status,
            pillars{signals_col}
        FROM {bq.table('fact_diagnostic_signals')}
        WHERE project_code = @project_code
          AND evaluation_date >= DATE_SUB(@anchor, INTERVAL @days DAY)
          AND evaluation_date <= @anchor
    """
    params = [
        bq.string_param("project_code", project_code),
        bq.scalar_param("days", "INT64", days),
        bq.date_param("anchor", anchor),
    ]
    if campaign_type:
        sql += "\n  AND campaign_type = @campaign_type"
        params.append(bq.string_param("campaign_type", campaign_type))

    sql += "\n ORDER BY evaluation_date ASC"

    rows = bq.run_query(sql, params)
    return [
        {
            "evaluation_date": (
                r["evaluation_date"].isoformat()
                if hasattr(r.get("evaluation_date"), "isoformat")
                else r.get("evaluation_date")
            ),
            "campaign_type": r.get("campaign_type"),
            "health_score": r.get("health_score"),
            "health_status": r.get("health_status"),
            "pillars": _coerce_json(r.get("pillars")) or {},
            **(
                {"signals": _slim_signals(r.get("signals"))}
                if include_signals
                else {}
            ),
        }
        for r in rows
    ]


@router.post("/{project_code}/run")
async def run_diagnostics(project_code: str):
    """Manually trigger diagnostic computation for a project as of today.

    For a date-specific replay, use the retrospective endpoint shipped in
    ADAC-51 commit 5 (``GET /api/diagnostics/as-of/{as_of_date}/project/{code}``).
    """
    try:
        # ``evaluation_date`` became required in ADAC-51 commit 2. The manual-run
        # semantic is "score right now", so pass ``date.today()`` explicitly.
        outputs = run_diagnostics_for_project(project_code, date.today())
    except Exception as e:
        logger.error("Diagnostic run failed for %s: %s", project_code, e, exc_info=True)
        raise HTTPException(500, f"Diagnostic run failed: {e}")

    if not outputs:
        return {
            "project_code": project_code,
            "status": "skipped",
            "message": "No diagnostic produced (missing media plan or no derivable flight dates).",
            "results": [],
        }

    return {
        "project_code": project_code,
        "status": "success",
        "results": [
            {
                "campaign_type": o.campaign_type.value,
                "evaluation_date": o.evaluation_date.isoformat(),
                "health_score": o.health_score,
                "health_status": o.health_status.value if o.health_status else None,
                "alerts": len(o.alerts),
            }
            for o in outputs
        ],
    }
