"""Benchmark API — returns applicable benchmarks for a project's objective type."""

import logging
from statistics import quantiles

from fastapi import APIRouter

from pydantic import BaseModel

from backend.services import bigquery_client as bq
from backend.services.objective_classifier import classify_objective, classify_project

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/benchmarks", tags=["benchmarks"])

# Phase 14 (Creative + Audiences redesign): the rotation view benchmarks
# hook_rate (video_views_3s / impressions) and engagement_rate
# (engagements / impressions), which have no seeded rows in the benchmarks
# table. They're computed at request time from the same PB campaign
# history (fact_digital_daily) and with the same per-campaign quartile
# pattern as the seeded cross-client ctr/vcr/cpm rows (see
# infrastructure/bigquery/seed_industry_benchmarks.sql — ">$50 spend
# each"). Campaigns that don't report the metric (NULL or hardcoded-zero
# platform columns) are excluded rather than dragging the quartiles down.
PB_HISTORY_MIN_CAMPAIGN_SPEND = 50.0
# Volume guard, mirroring F1_PER_PLATFORM_MIN_IMPRESSIONS philosophy
# (backend/services/diagnostics/conversion/funnel.py): a campaign's rate
# is too noisy to benchmark below this many impressions.
PB_HISTORY_MIN_IMPRESSIONS = 1_000
# Fewer than this many qualifying campaigns → no quartiles (a p50 of two
# campaigns isn't a benchmark; the frontend falls back gracefully).
PB_HISTORY_MIN_SAMPLE = 4


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
                  -- Plan-id-aware + multi-plan dedup guard. See
                  -- backend/routers/pacing.py for the canonical comment.
                  AND plan_id IN (
                      SELECT mp.plan_id
                      FROM {bq.table('media_plans')} mp
                      JOIN {bq.table('project_media_plans')} pmp
                        ON mp.project_code = pmp.project_code
                       AND mp.sheet_id   = pmp.sheet_id
                      WHERE mp.project_code = @pc
                        AND mp.is_current   = TRUE
                        AND pmp.is_active   = TRUE
                  )
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


def _pb_history_quartiles(objective: str) -> dict[str, BenchmarkValue]:
    """Cross-platform hook_rate / engagement_rate quartiles from PB
    campaign history, keyed by metric_name.

    Per-campaign rates (campaigns are single-platform, so each rate is
    inherently platform-honest), classified by campaign name with the same
    classifier the rest of the stack uses, filtered to the project's
    objective type — matching how the seeded cross-client rows were scoped.
    Best-effort: any failure returns {} and the seeded benchmarks still go
    out untouched.
    """
    try:
        sql = f"""
            SELECT
                campaign_id,
                ANY_VALUE(campaign_name) AS campaign_name,
                SUM(spend) AS spend,
                SUM(impressions) AS impressions,
                SUM(video_views_3s) AS video_views_3s,
                SUM(engagements) AS engagements
            FROM {bq.table('fact_digital_daily')}
            GROUP BY campaign_id
            HAVING SUM(spend) > @min_spend AND SUM(impressions) > 0
        """
        rows = bq.run_query(
            sql, [bq.scalar_param("min_spend", "FLOAT64", PB_HISTORY_MIN_CAMPAIGN_SPEND)]
        )
    except Exception:
        logger.warning("Failed to compute PB history quartiles (objective=%s)", objective, exc_info=True)
        return {}

    rates: dict[str, list[float]] = {"hook_rate": [], "engagement_rate": []}
    for r in rows:
        if classify_objective(None, r.get("campaign_name")) != objective:
            continue
        impressions = float(r.get("impressions") or 0)
        if impressions < PB_HISTORY_MIN_IMPRESSIONS:
            continue
        # Zero/NULL numerator = the platform doesn't report the metric for
        # this campaign — exclude it instead of benchmarking against 0.
        v3s = float(r.get("video_views_3s") or 0)
        if v3s > 0:
            rates["hook_rate"].append(v3s / impressions)
        eng = float(r.get("engagements") or 0)
        if eng > 0:
            rates["engagement_rate"].append(eng / impressions)

    result: dict[str, BenchmarkValue] = {}
    for metric, values in rates.items():
        if len(values) < PB_HISTORY_MIN_SAMPLE:
            continue
        # method="inclusive" matches PERCENTILE_CONT semantics — the same
        # interpolation the seeded cross-client quartiles used.
        p25, p50, p75 = quantiles(values, n=4, method="inclusive")
        result[metric] = BenchmarkValue(
            benchmark_id=f"pbh_xplat_{objective}_{metric}",
            scope="all_clients",
            platform_id=None,
            metric_name=metric,
            metric_unit="percentage",
            p25=p25,
            p50=p50,
            p75=p75,
            sample_size=len(values),
            source="pb_history",
            notes=(
                "Computed at request time from PB campaign history "
                f"(fact_digital_daily), campaigns > ${PB_HISTORY_MIN_CAMPAIGN_SPEND:.0f} "
                "spend that report the metric."
            ),
        )
    return result


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

    # Phase 14: hook_rate / engagement_rate quartiles from PB campaign
    # history. Additive only — a seeded table row for either metric wins.
    for metric, bv in _pb_history_quartiles(objective).items():
        cross_platform.setdefault(metric, bv)

    return BenchmarkResponse(
        project_code=project_code,
        objective_type=objective,
        benchmarks=cross_platform,
        platform_benchmarks=platform_specific,
    )
