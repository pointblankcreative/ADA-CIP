"""Creative + Audiences redesign endpoints (Phase 14).

Three read-only surfaces for the redesigned Creative + Audiences frontend,
at creative-variant and ad-set grain:

    GET /api/projects/{code}/creative/rotation — ranked creative variants
        with KPI rollups, 8-point daily trends, platform metric coverage,
        and window totals. window=flight (default) or 7d.
    GET /api/projects/{code}/creative/matrix   — creative × platform cells.
    GET /api/projects/{code}/audiences/matrix  — ad-set (audience) rows with
        media-plan roles plus audience × creative cells.

Conventions inherited from the performance router:

  * Creative variants are alias-resolved through creative_variant_aliases
    (exact or LIKE ad_name_pattern match, optional platform pin), falling
    back to the same regex normalization (strip leading 5-digit project
    code, strip trailing WxH dimensions).
  * Reach/frequency comes from fact_adset_daily at ad-set grain — latest
    snapshot in range, reach + frequency from the SAME row (AI-103) — and
    RF_EXCLUDED_PLATFORMS (AI-120 StackAdapt stopgap) never contribute.
  * Rate metrics are nullable: None means "not reported / insufficient
    volume" and renders as an em-dash (AI-029 pattern), never 0.

Volume guard: rate metrics (hook/completion/engagement/ctr) are nulled when
the row's window impressions sit under MIN_RATE_IMPRESSIONS — the same
"too noisy to score" philosophy as F1_PER_PLATFORM_MIN_IMPRESSIONS in
backend/services/diagnostics/conversion/funnel.py. Spend and impressions
always survive the guard.
"""

import logging
import re
from datetime import date, timedelta

from fastapi import APIRouter, HTTPException, Query
from google.cloud import exceptions as gcp_exceptions

from backend.models.creative import (
    AudienceMatrixCell,
    AudienceMatrixResponse,
    AudienceRow,
    CreativeCoverage,
    CreativeMatrixCell,
    CreativeMatrixResponse,
    CreativeRotationResponse,
    CreativeRotationRow,
    CreativeTotals,
    CreativeTrend,
    MatrixPlatform,
)
# AI-120 single source of truth — when the StackAdapt direct-API supplement
# ships, emptying the set in performance.py un-hides R&F here too.
from backend.routers.performance import RF_EXCLUDED_PLATFORMS
from backend.services import bigquery_client as bq
from backend.services.objective_classifier import classify_objective, classify_project

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/projects", tags=["creative"])


def _float(v, default=0.0) -> float:
    return float(v) if v is not None else default

def _int(v, default=0) -> int:
    return int(v) if v is not None else default


# Minimum window impressions before a creative's / cell's rate metrics
# (hook_rate, completion_rate, engagement_rate, ctr) are shown. Mirrors
# F1_PER_PLATFORM_MIN_IMPRESSIONS in diagnostics/conversion/funnel.py:
# below this the rates are too noisy to be meaningful, so the frontend
# gets None (em-dash) while spend / impressions stay visible.
MIN_RATE_IMPRESSIONS = 1_000

# Trend sparklines: last N daily points, oldest → newest. A series with
# fewer than TREND_MIN_POINTS usable days returns [] — one point is a
# dot, not a trend.
TREND_POINTS = 8
TREND_MIN_POINTS = 2

VALID_WINDOWS = ("flight", "7d")


def _rf_excluded_param():
    """Array query param for the AI-120 R&F exclusion clauses (same shape
    as the performance router's helper)."""
    return bq.array_param("rf_excluded", "STRING", sorted(RF_EXCLUDED_PLATFORMS))


def _window_filter(window: str, as_of: date) -> tuple[str, list]:
    """Date clause + params for the requested window.

    flight → no date restriction (the project's whole flight history);
    7d → the 7 calendar days ending at as_of (the latest data date, NOT
    today — stale data must not silently shrink the window).
    """
    if window == "7d":
        start = as_of - timedelta(days=6)
        return "date >= @window_start", [bq.date_param("window_start", start)]
    return "1=1", []


def _resolve_as_of(project_code: str) -> date:
    """Latest fact_digital_daily date for the project (404 when empty)."""
    sql = f"""
        SELECT MAX(date) AS max_date
        FROM {bq.table('fact_digital_daily')}
        WHERE project_code = @project_code
    """
    rows = bq.run_query(sql, [bq.string_param("project_code", project_code)])
    if not rows or rows[0].get("max_date") is None:
        raise HTTPException(
            404,
            f"No performance data found for project {project_code}",
        )
    return rows[0]["max_date"]


def _load_media_plan_objectives(project_code: str) -> dict[str, list[str]]:
    """Load objectives from media_plan_lines for a project, keyed by platform.

    Same shape as the performance router's loader: a list per platform
    because a single platform can carry lines with different objectives.
    """
    try:
        sql = f"""
            SELECT platform_id, objective
            FROM (
                SELECT platform_id, objective,
                       ROW_NUMBER() OVER (
                           PARTITION BY line_id ORDER BY sync_version DESC
                       ) AS _rn
                FROM {bq.table('media_plan_lines')}
                WHERE project_code = @project_code
                  AND objective IS NOT NULL
                  -- Plan-id-aware + multi-plan dedup guard. See
                  -- backend/routers/pacing.py for the canonical comment.
                  AND plan_id IN (
                      SELECT mp.plan_id
                      FROM {bq.table('media_plans')} mp
                      JOIN {bq.table('project_media_plans')} pmp
                        ON mp.project_code = pmp.project_code
                       AND mp.sheet_id   = pmp.sheet_id
                      WHERE mp.project_code = @project_code
                        AND mp.is_current   = TRUE
                        AND pmp.is_active   = TRUE
                  )
            ) WHERE _rn = 1
        """
        rows = bq.run_query(sql, [bq.string_param("project_code", project_code)])
        result: dict[str, list[str]] = {}
        for r in rows:
            pid = r.get("platform_id")
            obj = r.get("objective")
            if pid and obj:
                result.setdefault(pid, []).append(obj)
        return result
    except (gcp_exceptions.GoogleCloudError, gcp_exceptions.NotFound) as e:
        logger.warning("Failed to fetch objectives for project %s: %s", project_code, e, exc_info=True)
        return {}


def _detect_objective(project_code: str) -> str:
    """Project objective via the performance router's derivation: classify
    each active campaign from media-plan line objectives + campaign-name
    keywords, then roll up with classify_project."""
    media_plan_objectives = _load_media_plan_objectives(project_code)
    try:
        camp_sql = f"""
            SELECT DISTINCT campaign_name, platform_id
            FROM {bq.table('fact_digital_daily')}
            WHERE project_code = @project_code
        """
        camp_rows = bq.run_query(camp_sql, [bq.string_param("project_code", project_code)])
    except (gcp_exceptions.GoogleCloudError, gcp_exceptions.NotFound) as e:
        logger.warning("Failed to fetch campaigns for project %s: %s", project_code, e, exc_info=True)
        camp_rows = []

    objectives: list[str] = []
    for r in camp_rows:
        pid = r.get("platform_id", "")
        mp_objs = media_plan_objectives.get(pid, [])
        if mp_objs:
            line_classifications = [
                classify_objective(mp_obj, r.get("campaign_name"))
                for mp_obj in mp_objs
            ]
            obj = classify_objective(None, r.get("campaign_name"))
            if obj == "mixed" and line_classifications:
                obj = classify_project(line_classifications)
        else:
            obj = classify_objective(None, r.get("campaign_name"))
        objectives.append(obj)

    return classify_project(objectives)


def _alias_resolution(source: str) -> tuple[str, str]:
    """Alias join + variant expression for a CTE aliased `source`.

    Mirrors the performance router's /creatives endpoint exactly: probe the
    creative_variant_aliases table (it may not exist yet), join on exact or
    LIKE ad_name_pattern with an optional platform pin, and fall back to
    regex normalization (strip leading 5-digit project code, strip trailing
    WxH dimensions).
    """
    try:
        bq.run_query(f"SELECT 1 FROM {bq.table('creative_variant_aliases')} LIMIT 0", [])
        alias_join = f"""
            LEFT JOIN {bq.table('creative_variant_aliases')} cva
                ON cva.project_code = @project_code
                AND ({source}.ad_name = cva.ad_name_pattern OR {source}.ad_name LIKE cva.ad_name_pattern)
                AND (cva.platform_id IS NULL OR cva.platform_id = '' OR cva.platform_id = {source}.platform_id)
        """
        alias_col = "cva.creative_variant"
    except (gcp_exceptions.GoogleCloudError, gcp_exceptions.NotFound) as e:
        logger.warning("Creative aliases table not found or query failed; proceeding with ads without aliases: %s", e, exc_info=True)
        alias_join = ""
        alias_col = "NULL"

    variant_expr = f"""COALESCE(
                    {alias_col},
                    TRIM(REGEXP_REPLACE(
                        REGEXP_REPLACE({source}.ad_name,
                            r'^\\d{{5}}\\s*[-_]\\s*', ''),
                        r'\\s*[-_]?\\s*\\d+x\\d+\\s*$', ''
                    ))
                )"""
    return alias_join, variant_expr


# ── shared queries ────────────────────────────────────────────────────


def _query_creative_platform_cells(
    project_code: str,
    date_clause: str,
    date_params: list,
    alias_join: str,
    variant_expr: str,
) -> list[dict]:
    """Per-(creative_variant, platform_id) rollup with adset-grain frequency
    weights. The freq_weighted / freq_impressions pair lets callers compute
    an impressions-weighted frequency at any rollup level; both are NULL
    where no adset snapshot was joinable (→ frequency renders as None)."""
    params = [bq.string_param("project_code", project_code)] + date_params
    params.append(_rf_excluded_param())
    sql = f"""
        WITH ad_agg AS (
            SELECT
                f.ad_id,
                ANY_VALUE(f.ad_name) AS ad_name,
                f.platform_id,
                f.campaign_id,
                f.ad_set_id,
                SUM(f.spend) AS spend,
                SUM(f.impressions) AS impressions,
                SUM(f.clicks) AS clicks,
                SUM(f.conversions) AS conversions,
                SUM(f.engagements) AS engagements,
                SUM(f.video_views) AS video_views,
                SUM(f.video_completions) AS video_completions,
                SUM(f.video_views_3s) AS video_views_3s
            FROM {bq.table('fact_digital_daily')} f
            WHERE f.project_code = @project_code AND {date_clause}
                AND f.ad_name IS NOT NULL AND f.ad_name != ''
            GROUP BY f.ad_id, f.platform_id, f.campaign_id, f.ad_set_id
        ),
        aliased AS (
            SELECT ad_agg.*,
                {variant_expr} AS creative_variant
            FROM ad_agg
            {alias_join}
        ),
        -- AI-103 semantics: frequency snapshots are rolling windows, so the
        -- honest value for a range is the LATEST snapshot per adset.
        adset_reach AS (
            SELECT platform_id, campaign_id, ad_set_id, frequency
            FROM {bq.table('fact_adset_daily')}
            WHERE project_code = @project_code AND {date_clause}
              AND ad_set_id IS NOT NULL
              -- AI-120: StackAdapt R&F excluded pending direct-API supplement
              AND platform_id NOT IN UNNEST(@rf_excluded)
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY platform_id, campaign_id, ad_set_id
                ORDER BY date DESC, loaded_at DESC
            ) = 1
        )
        SELECT
            a.creative_variant,
            a.platform_id,
            SUM(a.spend) AS spend,
            SUM(a.impressions) AS impressions,
            SUM(a.clicks) AS clicks,
            SUM(a.conversions) AS conversions,
            SUM(a.engagements) AS engagements,
            SUM(a.video_views) AS video_views,
            SUM(a.video_completions) AS video_completions,
            SUM(a.video_views_3s) AS video_views_3s,
            SUM(IF(ar.frequency IS NOT NULL, ar.frequency * a.impressions, NULL)) AS freq_weighted,
            SUM(IF(ar.frequency IS NOT NULL, a.impressions, NULL)) AS freq_impressions
        FROM aliased a
        LEFT JOIN adset_reach ar
            ON a.platform_id = ar.platform_id
            AND a.campaign_id = ar.campaign_id
            AND a.ad_set_id = ar.ad_set_id
        GROUP BY a.creative_variant, a.platform_id
        ORDER BY spend DESC
    """
    return bq.run_query(sql, params)


def _coverage_from_cells(cells: list[dict]) -> tuple[CreativeCoverage, set[str], set[str]]:
    """Platform metric coverage from window data: a platform "reports" a
    metric when its window sum is > 0 (the metric_platforms pattern in the
    performance router — hardcoded-zero and NULL columns both fail it).

    Returns (coverage, hook_platforms, engagement_platforms); completion
    needs no platform set downstream because its denominator (video_views)
    is only non-zero where the platform reports video.
    """
    sums: dict[str, dict[str, float]] = {}
    for c in cells:
        s = sums.setdefault(c["platform_id"], {
            "video_views_3s": 0.0, "video_views": 0.0,
            "video_completions": 0.0, "engagements": 0.0,
        })
        s["video_views_3s"] += _float(c.get("video_views_3s"))
        s["video_views"] += _float(c.get("video_views"))
        s["video_completions"] += _float(c.get("video_completions"))
        s["engagements"] += _float(c.get("engagements"))

    hook = {p for p, s in sums.items() if s["video_views_3s"] > 0}
    completion = {
        p for p, s in sums.items()
        if s["video_views"] > 0 and s["video_completions"] > 0
    }
    engagement = {p for p, s in sums.items() if s["engagements"] > 0}
    return (
        CreativeCoverage(
            hook=sorted(hook),
            completion=sorted(completion),
            engagement=sorted(engagement),
        ),
        hook,
        engagement,
    )


def _rate_kpis(
    agg: dict,
    hook_platforms: set[str],
    engagement_platforms: set[str],
) -> dict:
    """Derived KPI fields from an accumulator built by _accumulate.

    Coverage-aware denominators: hook_rate and engagement_rate divide by
    impressions on reporting platforms ONLY, so a variant that also runs
    on a non-reporting platform isn't unfairly diluted. completion_rate
    reuses the vcr definition (video_completions / video_views). The
    volume guard nulls all four rates under MIN_RATE_IMPRESSIONS.
    """
    impressions = agg["impressions"]
    guard_ok = impressions >= MIN_RATE_IMPRESSIONS
    is_video = (
        agg["video_views"] > 0
        or agg["video_completions"] > 0
        or agg["video_views_3s"] > 0
    )

    ctr = (
        agg["clicks"] / impressions
        if guard_ok and impressions > 0 else None
    )
    # "Null if no 3s data": video creatives only, and only where 3s views
    # actually landed on a reporting platform.
    hook_rate = (
        agg["hook_3s"] / agg["hook_impressions"]
        if guard_ok and is_video and agg["hook_impressions"] > 0 and agg["hook_3s"] > 0
        else None
    )
    completion_rate = (
        agg["video_completions"] / agg["video_views"]
        if guard_ok and agg["video_views"] > 0 else None
    )
    engagement_rate = (
        agg["eng_engagements"] / agg["eng_impressions"]
        if guard_ok and agg["eng_impressions"] > 0 else None
    )
    cpm = agg["spend"] / impressions * 1000 if impressions > 0 else None
    cpa = agg["spend"] / agg["conversions"] if agg["conversions"] > 0 else None
    frequency = (
        agg["freq_weighted"] / agg["freq_impressions"]
        if agg["freq_impressions"] else None
    )
    return {
        "type": "video" if is_video else "static",
        "ctr": ctr,
        "hook_rate": hook_rate,
        "completion_rate": completion_rate,
        "engagement_rate": engagement_rate,
        "cpm": cpm,
        "cpa": cpa,
        "frequency": frequency,
    }


def _new_accumulator() -> dict:
    return {
        "spend": 0.0, "impressions": 0, "clicks": 0, "conversions": 0.0,
        "video_views": 0, "video_completions": 0, "video_views_3s": 0,
        "hook_3s": 0, "hook_impressions": 0,
        "eng_engagements": 0, "eng_impressions": 0,
        "freq_weighted": 0.0, "freq_impressions": 0,
        "platforms": set(),
    }


def _accumulate(
    agg: dict,
    cell: dict,
    hook_platforms: set[str],
    engagement_platforms: set[str],
) -> None:
    """Fold one (variant, platform) cell into an accumulator."""
    pid = cell["platform_id"]
    imp = _int(cell.get("impressions"))
    agg["spend"] += _float(cell.get("spend"))
    agg["impressions"] += imp
    agg["clicks"] += _int(cell.get("clicks"))
    agg["conversions"] += _float(cell.get("conversions"))
    agg["video_views"] += _int(cell.get("video_views"))
    agg["video_completions"] += _int(cell.get("video_completions"))
    agg["video_views_3s"] += _int(cell.get("video_views_3s"))
    if pid in hook_platforms:
        agg["hook_3s"] += _int(cell.get("video_views_3s"))
        agg["hook_impressions"] += imp
    if pid in engagement_platforms:
        agg["eng_engagements"] += _int(cell.get("engagements"))
        agg["eng_impressions"] += imp
    if cell.get("freq_impressions"):
        agg["freq_weighted"] += _float(cell.get("freq_weighted"))
        agg["freq_impressions"] += _int(cell.get("freq_impressions"))
    agg["platforms"].add(pid)


def _trend_series(points: list[tuple[str, float | None]]) -> list[float]:
    """Sparkline series from (date_key, value) pairs: drop None values,
    keep the last TREND_POINTS in date order (oldest → newest), and return
    [] when fewer than TREND_MIN_POINTS usable days remain."""
    usable = [v for _, v in sorted(points) if v is not None]
    series = usable[-TREND_POINTS:]
    return [float(v) for v in series] if len(series) >= TREND_MIN_POINTS else []


def _date_key(value) -> str:
    return value.isoformat() if hasattr(value, "isoformat") else str(value)


# ── Phase 19 additive lookups (thumbnails, personas, saturation) ──────


def _load_variant_images(project_code: str, variants: list[str]) -> dict[str, str]:
    """variant → signed GCS URL for stored creative_assets rows.

    Best-effort and additive: the table may not exist yet and signing can
    fail — both degrade to {} so the rotation renders without thumbnails.
    Rows written before a project_code backfill carry NULL project_code,
    so those match any project (variants are already project-scoped here).
    """
    if not variants:
        return {}
    try:
        rows = bq.run_query(
            f"""
            SELECT variant, gcs_path
            FROM {bq.table('creative_assets')}
            WHERE status = 'stored'
              AND gcs_path IS NOT NULL
              AND variant IN UNNEST(@variants)
              AND (project_code = @project_code OR project_code IS NULL)
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY variant ORDER BY checked_at DESC
            ) = 1
            """,
            [
                bq.array_param("variants", "STRING", variants),
                bq.string_param("project_code", project_code),
            ],
        )
    except Exception as e:
        logger.warning("creative_assets lookup failed for %s: %s", project_code, e, exc_info=True)
        return {}
    if not rows:
        return {}

    # Lazy import — creative_assets imports this router's helpers, so a
    # module-level import here would be circular.
    from backend.services import creative_assets

    urls: dict[str, str] = {}
    for r in rows:
        try:
            url = creative_assets.signed_url(r["gcs_path"])
        except Exception:
            logger.warning("Signing failed for %s", r.get("gcs_path"), exc_info=True)
            url = None
        if url:
            urls[r["variant"]] = url
    return urls


def _load_adset_targeting(audience_ids: list[str]) -> dict[str, dict]:
    """audience_key → {persona, pool_size} from the Phase 19 targeting
    sync. Best-effort: a missing table just means every persona is None."""
    if not audience_ids:
        return {}
    try:
        rows = bq.run_query(
            f"""
            SELECT audience_key, persona, pool_size
            FROM {bq.table('adset_targeting')}
            WHERE audience_key IN UNNEST(@audience_keys)
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY audience_key ORDER BY fetched_at DESC
            ) = 1
            """,
            [bq.array_param("audience_keys", "STRING", audience_ids)],
        )
        return {r["audience_key"]: r for r in rows if r.get("audience_key")}
    except Exception as e:
        logger.warning("adset_targeting lookup failed: %s", e, exc_info=True)
        return {}


def _load_audience_reach(project_code: str) -> dict[tuple[str, str], int]:
    """(platform_id, ad_set_name) → reach for the saturation numerator.

    AI-103 semantics, same as frequency in this router: the LATEST
    snapshot per (platform, campaign, ad_set), then summed across the
    audience's ad sets. AI-120 platforms never contribute. Best-effort:
    {} on failure and saturation stays None.
    """
    try:
        rows = bq.run_query(
            f"""
            WITH latest AS (
                SELECT platform_id, campaign_id, ad_set_id, ad_set_name, reach
                FROM {bq.table('fact_adset_daily')}
                WHERE project_code = @project_code
                  AND ad_set_id IS NOT NULL
                  AND ad_set_name IS NOT NULL
                  AND reach IS NOT NULL
                  -- AI-120: StackAdapt R&F excluded pending direct-API supplement
                  AND platform_id NOT IN UNNEST(@rf_excluded)
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY platform_id, campaign_id, ad_set_id
                    ORDER BY date DESC, loaded_at DESC
                ) = 1
            )
            SELECT platform_id, ad_set_name, SUM(reach) AS reach
            FROM latest
            GROUP BY platform_id, ad_set_name
            """,
            [bq.string_param("project_code", project_code), _rf_excluded_param()],
        )
        return {
            (r["platform_id"], r.get("ad_set_name") or ""): _int(r.get("reach"))
            for r in rows
            if r.get("reach")
        }
    except Exception as e:
        logger.warning("Audience reach lookup failed for %s: %s", project_code, e, exc_info=True)
        return {}


# ── 1. creative rotation ──────────────────────────────────────────────


@router.get("/{project_code}/creative/rotation", response_model=CreativeRotationResponse)
async def get_creative_rotation(
    project_code: str,
    window: str = Query("flight", description="flight (whole flight) or 7d"),
):
    """Ranked creative variants (spend DESC) with KPI rollups, platform
    metric coverage, daily trend sparklines, and window totals."""
    if window not in VALID_WINDOWS:
        raise HTTPException(422, f"window must be one of {', '.join(VALID_WINDOWS)}")

    as_of = _resolve_as_of(project_code)
    date_clause, date_params = _window_filter(window, as_of)
    objective = _detect_objective(project_code)
    alias_join, variant_expr = _alias_resolution("ad_agg")

    cells = _query_creative_platform_cells(
        project_code, date_clause, date_params, alias_join, variant_expr
    )
    coverage, hook_platforms, engagement_platforms = _coverage_from_cells(cells)

    # ── per-variant + total rollups ─────────────────────────────────
    by_variant: dict[str, dict] = {}
    totals_agg = _new_accumulator()
    for c in cells:
        variant = c.get("creative_variant") or "Unknown"
        agg = by_variant.setdefault(variant, _new_accumulator())
        _accumulate(agg, c, hook_platforms, engagement_platforms)
        _accumulate(totals_agg, c, hook_platforms, engagement_platforms)
    total_spend = totals_agg["spend"]

    # ── daily trend query (per variant × date, same window) ────────
    daily_params = [bq.string_param("project_code", project_code)] + date_params
    daily_params.append(_rf_excluded_param())
    daily_sql = f"""
        WITH ad_agg AS (
            SELECT
                f.date,
                f.ad_id,
                ANY_VALUE(f.ad_name) AS ad_name,
                f.platform_id,
                f.campaign_id,
                f.ad_set_id,
                SUM(f.spend) AS spend,
                SUM(f.impressions) AS impressions,
                SUM(f.clicks) AS clicks,
                SUM(f.conversions) AS conversions,
                SUM(f.video_views) AS video_views,
                SUM(f.video_completions) AS video_completions
            FROM {bq.table('fact_digital_daily')} f
            WHERE f.project_code = @project_code AND {date_clause}
                AND f.ad_name IS NOT NULL AND f.ad_name != ''
            GROUP BY f.date, f.ad_id, f.platform_id, f.campaign_id, f.ad_set_id
        ),
        aliased AS (
            SELECT ad_agg.*,
                {variant_expr} AS creative_variant
            FROM ad_agg
            {alias_join}
        ),
        -- Daily frequency snapshots (not latest-in-range): one row per
        -- adset per day, loaded_at tiebreak only.
        adset_daily AS (
            SELECT date, platform_id, campaign_id, ad_set_id, frequency
            FROM {bq.table('fact_adset_daily')}
            WHERE project_code = @project_code AND {date_clause}
              AND ad_set_id IS NOT NULL
              -- AI-120: StackAdapt R&F excluded pending direct-API supplement
              AND platform_id NOT IN UNNEST(@rf_excluded)
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY date, platform_id, campaign_id, ad_set_id
                ORDER BY loaded_at DESC
            ) = 1
        )
        SELECT
            a.creative_variant,
            a.date,
            SAFE_DIVIDE(SUM(a.clicks), NULLIF(SUM(a.impressions), 0)) AS ctr,
            SAFE_DIVIDE(SUM(a.video_completions), NULLIF(SUM(a.video_views), 0)) AS completion_rate,
            SAFE_DIVIDE(SUM(a.spend), NULLIF(SUM(a.conversions), 0)) AS cpa,
            SAFE_DIVIDE(
                SUM(IF(ad.frequency IS NOT NULL, ad.frequency * a.impressions, NULL)),
                NULLIF(SUM(IF(ad.frequency IS NOT NULL, a.impressions, NULL)), 0)
            ) AS frequency
        FROM aliased a
        LEFT JOIN adset_daily ad
            ON a.date = ad.date
            AND a.platform_id = ad.platform_id
            AND a.campaign_id = ad.campaign_id
            AND a.ad_set_id = ad.ad_set_id
        GROUP BY a.creative_variant, a.date
        ORDER BY a.date
    """
    daily_rows = bq.run_query(daily_sql, daily_params)

    # primary trend metric: completion for awareness, CPA otherwise
    primary_metric = "completion_rate" if objective == "awareness" else "cpa"
    trend_points: dict[str, dict[str, list[tuple[str, float | None]]]] = {}
    for r in daily_rows:
        variant = r.get("creative_variant") or "Unknown"
        dk = _date_key(r["date"])
        series = trend_points.setdefault(
            variant, {"ctr": [], "frequency": [], "primary": []}
        )
        series["ctr"].append((dk, r.get("ctr")))
        series["frequency"].append((dk, r.get("frequency")))
        series["primary"].append((dk, r.get(primary_metric)))

    # ── build response (spend DESC = rotation rank) ─────────────────
    creatives: list[CreativeRotationRow] = []
    for variant, agg in sorted(
        by_variant.items(), key=lambda kv: kv[1]["spend"], reverse=True
    ):
        kpis = _rate_kpis(agg, hook_platforms, engagement_platforms)
        points = trend_points.get(variant, {"ctr": [], "frequency": [], "primary": []})
        creatives.append(
            CreativeRotationRow(
                variant=variant,
                type=kpis["type"],
                platforms=sorted(agg["platforms"]),
                spend=agg["spend"],
                spend_share=agg["spend"] / total_spend if total_spend > 0 else 0.0,
                impressions=agg["impressions"],
                frequency=kpis["frequency"],
                hook_rate=kpis["hook_rate"],
                completion_rate=kpis["completion_rate"],
                engagement_rate=kpis["engagement_rate"],
                ctr=kpis["ctr"],
                clicks=agg["clicks"],
                cpm=kpis["cpm"],
                conversions=agg["conversions"],
                cpa=kpis["cpa"],
                trend=CreativeTrend(
                    ctr=_trend_series(points["ctr"]),
                    frequency=_trend_series(points["frequency"]),
                    primary=_trend_series(points["primary"]),
                ),
            )
        )

    # Phase 19 (additive): signed thumbnail URLs for variants the asset
    # sync has stored a still for. Missing table / signing failure just
    # means image_url stays None everywhere.
    image_urls = _load_variant_images(project_code, [c.variant for c in creatives])
    for row in creatives:
        row.image_url = image_urls.get(row.variant)

    totals_kpis = _rate_kpis(totals_agg, hook_platforms, engagement_platforms)
    return CreativeRotationResponse(
        project_code=project_code,
        objective=objective,
        window=window,
        as_of=as_of,
        creatives=creatives,
        coverage=coverage,
        totals=CreativeTotals(
            spend=totals_agg["spend"],
            impressions=totals_agg["impressions"],
            frequency=totals_kpis["frequency"],
            hook_rate=totals_kpis["hook_rate"],
            completion_rate=totals_kpis["completion_rate"],
            engagement_rate=totals_kpis["engagement_rate"],
            ctr=totals_kpis["ctr"],
            clicks=totals_agg["clicks"],
            cpm=totals_kpis["cpm"],
            conversions=totals_agg["conversions"],
            cpa=totals_kpis["cpa"],
        ),
    )


# ── 2. creative × platform matrix ─────────────────────────────────────


@router.get("/{project_code}/creative/matrix", response_model=CreativeMatrixResponse)
async def get_creative_matrix(project_code: str):
    """Creative × platform cells over the whole flight. Cells where a
    variant doesn't run on a platform are absent. The volume guard nulls a
    cell's rates but keeps its spend/impressions."""
    alias_join, variant_expr = _alias_resolution("ad_agg")
    cells = _query_creative_platform_cells(
        project_code, "1=1", [], alias_join, variant_expr
    )
    _, hook_platforms, engagement_platforms = _coverage_from_cells(cells)

    platform_spend: dict[str, float] = {}
    variant_spend: dict[str, float] = {}
    matrix: dict[str, dict[str, CreativeMatrixCell]] = {}
    for c in cells:
        variant = c.get("creative_variant") or "Unknown"
        pid = c["platform_id"]
        spend = _float(c.get("spend"))
        impressions = _int(c.get("impressions"))
        platform_spend[pid] = platform_spend.get(pid, 0.0) + spend
        variant_spend[variant] = variant_spend.get(variant, 0.0) + spend

        guard_ok = impressions >= MIN_RATE_IMPRESSIONS
        clicks = _int(c.get("clicks"))
        conversions = _float(c.get("conversions"))
        vv = _int(c.get("video_views"))
        vc = _int(c.get("video_completions"))
        v3s = _int(c.get("video_views_3s"))
        engagements = _int(c.get("engagements"))
        matrix.setdefault(variant, {})[pid] = CreativeMatrixCell(
            spend=spend,
            impressions=impressions,
            hook_rate=(
                v3s / impressions
                if guard_ok and pid in hook_platforms and v3s > 0 and impressions > 0
                else None
            ),
            completion_rate=vc / vv if guard_ok and vv > 0 else None,
            engagement_rate=(
                engagements / impressions
                if guard_ok and pid in engagement_platforms and impressions > 0
                else None
            ),
            ctr=clicks / impressions if guard_ok and impressions > 0 else None,
            cpm=spend / impressions * 1000 if impressions > 0 else None,
            conversions=conversions,
            cpa=spend / conversions if conversions > 0 else None,
        )

    total_spend = sum(platform_spend.values())
    return CreativeMatrixResponse(
        project_code=project_code,
        platforms=[
            MatrixPlatform(
                platform_id=pid,
                spend=spend,
                share=spend / total_spend if total_spend > 0 else 0.0,
            )
            for pid, spend in sorted(
                platform_spend.items(), key=lambda kv: kv[1], reverse=True
            )
        ],
        creatives=[
            v for v, _ in sorted(
                variant_spend.items(), key=lambda kv: kv[1], reverse=True
            )
        ],
        cells=matrix,
    )


# ── 3. audiences × creative matrix ────────────────────────────────────


def _audience_id(ad_set_name: str, platform_id: str) -> str:
    """Stable slug of adset_name + platform — the audience's frontend key."""
    return re.sub(r"[^a-z0-9]+", "-", f"{ad_set_name} {platform_id}".lower()).strip("-")


def _load_audience_roles(project_code: str) -> dict[str, str]:
    """audience_name (normalized) → audience_type from current media-plan
    lines. Best-effort: an empty dict just means every role comes back None."""
    try:
        sql = f"""
            SELECT audience_name, audience_type
            FROM (
                SELECT audience_name, audience_type,
                       ROW_NUMBER() OVER (
                           PARTITION BY line_id ORDER BY sync_version DESC
                       ) AS _rn
                FROM {bq.table('media_plan_lines')}
                WHERE project_code = @project_code
                  AND audience_name IS NOT NULL
                  -- Plan-id-aware + multi-plan dedup guard. See
                  -- backend/routers/pacing.py for the canonical comment.
                  AND plan_id IN (
                      SELECT mp.plan_id
                      FROM {bq.table('media_plans')} mp
                      JOIN {bq.table('project_media_plans')} pmp
                        ON mp.project_code = pmp.project_code
                       AND mp.sheet_id   = pmp.sheet_id
                      WHERE mp.project_code = @project_code
                        AND mp.is_current   = TRUE
                        AND pmp.is_active   = TRUE
                  )
            ) WHERE _rn = 1
        """
        rows = bq.run_query(sql, [bq.string_param("project_code", project_code)])
        roles: dict[str, str] = {}
        for r in rows:
            name = (r.get("audience_name") or "").strip().lower()
            if name and r.get("audience_type"):
                roles[name] = r["audience_type"]
        return roles
    except (gcp_exceptions.GoogleCloudError, gcp_exceptions.NotFound) as e:
        logger.warning("Failed to fetch audience roles for project %s: %s", project_code, e, exc_info=True)
        return {}


def _match_role(ad_set_name: str | None, roles: dict[str, str]) -> str | None:
    """audience_type for an adset: exact normalized match first, then
    containment either way (planner audience names rarely match platform
    adset names character-for-character). Longest plan name wins so the
    most specific line decides. None when nothing matches."""
    if not ad_set_name or not roles:
        return None
    key = ad_set_name.strip().lower()
    if key in roles:
        return roles[key]
    for name in sorted(roles, key=len, reverse=True):
        if name in key or key in name:
            return roles[name]
    return None


@router.get("/{project_code}/audiences/matrix", response_model=AudienceMatrixResponse)
async def get_audience_matrix(project_code: str):
    """Ad-set (audience) rows with media-plan roles and frequency trends,
    plus audience × creative cells, over the whole flight."""
    roles = _load_audience_roles(project_code)

    # ── audience rollup (adset grain → adset_name × platform) ──────
    # Mirrors the /adsets endpoint's AI-103 reach join: latest snapshot at
    # adset grain, campaign-grain fallback ONLY for platforms that report
    # reach at campaign level (NULL ad_set_id rows), AI-120 exclusion in
    # both CTEs. Frequency is impressions-weighted across the adset's
    # (campaign, ad_set_id) snapshots where joinable, else NULL.
    aud_params = [bq.string_param("project_code", project_code), _rf_excluded_param()]
    aud_sql = f"""
        WITH adset_metrics AS (
            SELECT
                f.platform_id,
                f.campaign_id,
                f.ad_set_id,
                ANY_VALUE(f.ad_set_name) AS ad_set_name,
                SUM(f.spend) AS spend,
                SUM(f.impressions) AS impressions,
                SUM(f.clicks) AS clicks,
                SUM(f.conversions) AS conversions,
                SUM(f.engagements) AS engagements,
                SUM(f.video_views) AS video_views,
                SUM(f.video_completions) AS video_completions,
                SUM(f.video_views_3s) AS video_views_3s
            FROM {bq.table('fact_digital_daily')} f
            WHERE f.project_code = @project_code
                AND f.ad_set_name IS NOT NULL AND f.ad_set_name != ''
            GROUP BY f.platform_id, f.campaign_id, f.ad_set_id
        ),
        adset_reach AS (
            SELECT platform_id, campaign_id, ad_set_id, frequency
            FROM {bq.table('fact_adset_daily')}
            WHERE project_code = @project_code
              AND ad_set_id IS NOT NULL
              -- AI-120: StackAdapt R&F excluded pending direct-API supplement
              AND platform_id NOT IN UNNEST(@rf_excluded)
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY platform_id, campaign_id, ad_set_id
                ORDER BY date DESC, loaded_at DESC
            ) = 1
        ),
        campaign_reach AS (
            SELECT platform_id, campaign_id, frequency
            FROM {bq.table('fact_adset_daily')}
            WHERE project_code = @project_code
              AND ad_set_id IS NULL
              -- AI-120: StackAdapt R&F excluded pending direct-API supplement
              AND platform_id NOT IN UNNEST(@rf_excluded)
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY platform_id, campaign_id
                ORDER BY date DESC, loaded_at DESC
            ) = 1
        )
        SELECT
            m.platform_id,
            m.ad_set_name,
            SUM(m.spend) AS spend,
            SUM(m.impressions) AS impressions,
            SUM(m.clicks) AS clicks,
            SUM(m.conversions) AS conversions,
            SUM(m.engagements) AS engagements,
            SUM(m.video_views) AS video_views,
            SUM(m.video_completions) AS video_completions,
            SUM(m.video_views_3s) AS video_views_3s,
            SUM(IF(COALESCE(ar.frequency, cr.frequency) IS NOT NULL,
                   COALESCE(ar.frequency, cr.frequency) * m.impressions, NULL)) AS freq_weighted,
            SUM(IF(COALESCE(ar.frequency, cr.frequency) IS NOT NULL,
                   m.impressions, NULL)) AS freq_impressions
        FROM adset_metrics m
        LEFT JOIN adset_reach ar
            ON m.platform_id = ar.platform_id
            AND m.campaign_id = ar.campaign_id
            AND m.ad_set_id = ar.ad_set_id
        LEFT JOIN campaign_reach cr
            ON m.platform_id = cr.platform_id
            AND m.campaign_id = cr.campaign_id
            AND ar.ad_set_id IS NULL
        GROUP BY m.platform_id, m.ad_set_name
        ORDER BY spend DESC
    """
    audience_rows = bq.run_query(aud_sql, aud_params)

    # ── daily frequency trend per (platform, adset_name) ────────────
    trend_params = [bq.string_param("project_code", project_code), _rf_excluded_param()]
    trend_sql = f"""
        WITH snapshots AS (
            SELECT date, platform_id, campaign_id, ad_set_id, ad_set_name,
                   frequency, impressions
            FROM {bq.table('fact_adset_daily')}
            WHERE project_code = @project_code
              AND ad_set_name IS NOT NULL
              AND frequency IS NOT NULL
              -- AI-120: StackAdapt R&F excluded pending direct-API supplement
              AND platform_id NOT IN UNNEST(@rf_excluded)
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY date, platform_id, campaign_id, ad_set_id
                ORDER BY loaded_at DESC
            ) = 1
        )
        SELECT
            date,
            platform_id,
            ad_set_name,
            SAFE_DIVIDE(SUM(frequency * COALESCE(impressions, 1)),
                        NULLIF(SUM(COALESCE(impressions, 1)), 0)) AS frequency
        FROM snapshots
        GROUP BY date, platform_id, ad_set_name
        ORDER BY date
    """
    freq_trends: dict[tuple[str, str], list[tuple[str, float | None]]] = {}
    for r in bq.run_query(trend_sql, trend_params):
        key = (r["platform_id"], r.get("ad_set_name") or "")
        freq_trends.setdefault(key, []).append((_date_key(r["date"]), r.get("frequency")))

    # ── audience × creative cells (ad grain, alias-resolved) ────────
    alias_join, variant_expr = _alias_resolution("ad_agg")
    cell_params = [bq.string_param("project_code", project_code)]
    cell_sql = f"""
        WITH ad_agg AS (
            SELECT
                f.ad_id,
                ANY_VALUE(f.ad_name) AS ad_name,
                f.platform_id,
                f.ad_set_name,
                SUM(f.spend) AS spend,
                SUM(f.impressions) AS impressions,
                SUM(f.clicks) AS clicks,
                SUM(f.conversions) AS conversions,
                SUM(f.engagements) AS engagements,
                SUM(f.video_views) AS video_views,
                SUM(f.video_completions) AS video_completions,
                SUM(f.video_views_3s) AS video_views_3s
            FROM {bq.table('fact_digital_daily')} f
            WHERE f.project_code = @project_code
                AND f.ad_name IS NOT NULL AND f.ad_name != ''
                AND f.ad_set_name IS NOT NULL AND f.ad_set_name != ''
            GROUP BY f.ad_id, f.platform_id, f.ad_set_name
        ),
        aliased AS (
            SELECT ad_agg.*,
                {variant_expr} AS creative_variant
            FROM ad_agg
            {alias_join}
        )
        SELECT
            a.creative_variant,
            a.platform_id,
            a.ad_set_name,
            SUM(a.spend) AS spend,
            SUM(a.impressions) AS impressions,
            SUM(a.clicks) AS clicks,
            SUM(a.conversions) AS conversions,
            SUM(a.engagements) AS engagements,
            SUM(a.video_views) AS video_views,
            SUM(a.video_completions) AS video_completions,
            SUM(a.video_views_3s) AS video_views_3s
        FROM aliased a
        GROUP BY a.creative_variant, a.platform_id, a.ad_set_name
        ORDER BY spend DESC
    """
    cell_rows = bq.run_query(cell_sql, cell_params)
    _, hook_platforms, engagement_platforms = _coverage_from_cells(cell_rows)

    # Audience-level coverage from the audience rollup itself (it includes
    # unnamed ads the cell query filters out).
    aud_eng_platforms = {
        r["platform_id"] for r in audience_rows if _int(r.get("engagements")) > 0
    }

    audiences: list[AudienceRow] = []
    for r in audience_rows:
        name = r.get("ad_set_name") or ""
        pid = r["platform_id"]
        impressions = _int(r.get("impressions"))
        spend = _float(r.get("spend"))
        conversions = _float(r.get("conversions"))
        vv = _int(r.get("video_views"))
        vc = _int(r.get("video_completions"))
        guard_ok = impressions >= MIN_RATE_IMPRESSIONS
        freq_imp = _int(r.get("freq_impressions"))
        audiences.append(
            AudienceRow(
                id=_audience_id(name, pid),
                name=name,
                platform_id=pid,
                role=_match_role(name, roles),
                spend=spend,
                # AI-120: NULL frequency for excluded platforms (the SQL
                # already excludes them; the guard keeps it explicit).
                frequency=(
                    _float(r.get("freq_weighted")) / freq_imp
                    if freq_imp and pid not in RF_EXCLUDED_PLATFORMS else None
                ),
                frequency_trend=_trend_series(freq_trends.get((pid, name), [])),
                impressions=impressions,
                ctr=(
                    _int(r.get("clicks")) / impressions
                    if guard_ok and impressions > 0 else None
                ),
                completion_rate=vc / vv if guard_ok and vv > 0 else None,
                engagement_rate=(
                    _int(r.get("engagements")) / impressions
                    if guard_ok and pid in aud_eng_platforms and impressions > 0
                    else None
                ),
                conversions=conversions,
                cpa=spend / conversions if conversions > 0 else None,
            )
        )

    # ── Phase 19 (additive): persona / pool / saturation ────────────
    # Both lookups are best-effort and run AFTER the existing queries so
    # the canned-response call order in older tests is undisturbed.
    targeting = _load_adset_targeting([a.id for a in audiences])
    reach_by_adset = _load_audience_reach(project_code)
    for a in audiences:
        t = targeting.get(a.id)
        if t:
            a.persona = t.get("persona")
            pool = t.get("pool_size")
            a.pool_size = int(pool) if pool is not None else None
        reach = reach_by_adset.get((a.platform_id, a.name))
        # Null unless both sides exist — a saturation guess helps nobody.
        if a.pool_size and a.pool_size > 0 and reach:
            a.saturation = reach / a.pool_size

    variant_spend: dict[str, float] = {}
    cells: dict[str, dict[str, AudienceMatrixCell]] = {}
    for c in cell_rows:
        variant = c.get("creative_variant") or "Unknown"
        pid = c["platform_id"]
        aud_id = _audience_id(c.get("ad_set_name") or "", pid)
        spend = _float(c.get("spend"))
        impressions = _int(c.get("impressions"))
        clicks = _int(c.get("clicks"))
        conversions = _float(c.get("conversions"))
        vv = _int(c.get("video_views"))
        vc = _int(c.get("video_completions"))
        v3s = _int(c.get("video_views_3s"))
        engagements = _int(c.get("engagements"))
        variant_spend[variant] = variant_spend.get(variant, 0.0) + spend

        guard_ok = impressions >= MIN_RATE_IMPRESSIONS
        cells.setdefault(aud_id, {})[variant] = AudienceMatrixCell(
            spend=spend,
            impressions=impressions,
            hook_rate=(
                v3s / impressions
                if guard_ok and pid in hook_platforms and v3s > 0 and impressions > 0
                else None
            ),
            completion_rate=vc / vv if guard_ok and vv > 0 else None,
            engagement_rate=(
                engagements / impressions
                if guard_ok and pid in engagement_platforms and impressions > 0
                else None
            ),
            ctr=clicks / impressions if guard_ok and impressions > 0 else None,
            conversions=conversions,
            cpa=spend / conversions if conversions > 0 else None,
        )

    return AudienceMatrixResponse(
        project_code=project_code,
        audiences=audiences,
        creatives=[
            v for v, _ in sorted(
                variant_spend.items(), key=lambda kv: kv[1], reverse=True
            )
        ],
        cells=cells,
    )
