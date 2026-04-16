"""Benchmark API — returns applicable benchmarks for a project's objective type."""

import logging

from fastapi import APIRouter

from pydantic import BaseModel

from backend.services import bigquery_client as bq
from backend.services.objective_classifier import classify_objective, classify_project

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/benchmarks", tags=["benchmarks"])


class BenchmarkValue(BaseModel):
    benchmark_id: str
    scope: str
    platform_id: str | None = None
    metric_name: str
    metric_unit: str
    p25: float | None = None
    p50: float | None = None
    p75: float | None = None
    sample_size: int | None = None
    source: str | None = None
    notes: str | None = None


class BenchmarkResponse(BaseModel):
    project_code: str
    objective_type: str
    benchmarks: dict[str, BenchmarkValue] = {}
    platform_benchmarks: dict[str, dict[str, BenchmarkValue]] = {}


def _detect_project_objective(project_code: str) -> str:
    """Determine a project's objective type from its campaign data."""
    try:
        mp_sql = f"""
            SELECT platform_id, objective
            FROM (
                SELECT platform_id, objective,
                       ROW_NUMBER() OVER (
                           PARTITION BY line_id ORDER BY sync_version DESC
                       ) AS _rn
                FROM {bq.table('media_plan_lines')}
                WHERE project_code = @pc AND objective IS NOT NULL
            ) WHERE _rn = 1
        """
        mp_rows = bq.run_query(mp_sql, [bq.string_param("pc", project_code)])
        mp_objectives = {r["platform_id"]: r["objective"] for r in mp_rows if r.get("platform_id")}
    except Exception:
        mp_objectives = {}

    try:
        camp_sql = f"""
            SELECT DISTINCT campaign_name, platform_id
            FROM {bq.table('fact_digital_daily')}
            WHERE project_code = @pc
        """
        camp_rows = bq.run_query(camp_sql, [bq.string_param("pc", project_code)])
    except Exception:
        camp_rows = []

    objectives = []
    for r in camp_rows:
        pid = r.get("platform_id", "")
        mp_obj = mp_objectives.get(pid)
        objectives.append(classify_objective(mp_obj, r.get("campaign_name")))

    return classify_project(objectives)


@router.get("/{project_code}", response_model=BenchmarkResponse)
async def get_benchmarks(project_code: str):
    objective = _detect_project_objective(project_code)

    sql = f"""
        SELECT
            benchmark_id, scope, platform_id, metric_name, metric_unit,
            p25, p50, p75, sample_size, source, notes
        FROM {bq.table('benchmarks')}
        WHERE benchmark_type = 'industry'
          AND objective_type = @objective
          AND (valid_to IS NULL OR valid_to >= CURRENT_DATE())
        ORDER BY
            CASE WHEN platform_id IS NULL THEN 0 ELSE 1 END,
            scope, metric_name
    """
    try:
        rows = bq.run_query(sql, [bq.string_param("objective", objective)])
    except Exception:
        logger.warning("Failed to query benchmarks for %s (objective=%s)", project_code, objective, exc_info=True)
        rows = []

    cross_platform: dict[str, BenchmarkValue] = {}
    platform_specific: dict[str, dict[str, BenchmarkValue]] = {}

    for r in rows:
        bv = BenchmarkValue(
            benchmark_id=r["benchmark_id"],
            scope=r.get("scope", ""),
            platform_id=r.get("platform_id"),
            metric_name=r["metric_name"],
            metric_unit=r.get("metric_unit", ""),
            p25=float(r["p25"]) if r.get("p25") is not None else None,
            p50=float(r["p50"]) if r.get("p50") is not None else None,
            p75=float(r["p75"]) if r.get("p75") is not None else None,
            sample_size=int(r["sample_size"]) if r.get("sample_size") is not None else None,
            source=r.get("source"),
            notes=r.get("notes"),
        )
        if r.get("platform_id"):
            pid = r["platform_id"]
            if pid not in platform_specific:
                platform_specific[pid] = {}
            platform_specific[pid][bv.metric_name] = bv
        else:
            if bv.metric_name not in cross_platform:
                cross_platform[bv.metric_name] = bv

    return BenchmarkResponse(
        project_code=project_code,
        objective_type=objective,
        benchmarks=cross_platform,
        platform_benchmarks=platform_specific,
    )
