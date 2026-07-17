"""Per-platform data-freshness primitive (P-FRESH-PACE).

Consolidates the two pre-existing staleness reads —
``backend.routers.admin.data_freshness`` (the admin Pipeline panel) and
``backend.services.daily_job._check_data_staleness`` (the daily sweep) — into one
``as_of``-aware function so pacing, the admin endpoint, and the daily job all
score freshness the same way.

A platform is STALE if EITHER of these holds, THEN gated by a flight-end guard:

  * Absolute 36h floor — ``age_hours > DATA_STALE_HOURS`` (hours since
    ``latest_loaded_at``; falls back to ``(as_of_date - latest_data_date).days *
    24`` when ``loaded_at`` is missing, the same fallback daily_job uses today).
  * Relative-to-freshest — ``latest_data_date`` lags the MAX ``latest_data_date``
    across all platforms by more than ``RELATIVE_LAG_DAYS`` days.

  * flight_end guard — when ``project_code`` is supplied, a platform whose media
    plan lines have ALL ended as of ``as_of_date`` (no still-live non-direct
    line) is never flagged stale: it is EXPECTED to stop reporting. The global
    (no-project) call skips the guard.

The function does its own BigQuery reads but keeps every datetime comparison in
Python and returns plain dicts, so callers can filter/reshape freely.
"""

import logging
from datetime import date, datetime, timezone

from backend.services import bigquery_client as bq

logger = logging.getLogger(__name__)

# Absolute floor: no fresh load in this many hours ⇒ stale. Mirrors
# ``backend.services.daily_job.DATA_STALE_HOURS`` and the "Data Stale" default
# in CLAUDE.md §10 (no data > 36 hours).
DATA_STALE_HOURS = 36

# Relative floor: a platform whose newest data date trails the freshest
# platform's newest date by more than this many days has stopped producing new
# days even if its last load timestamp is recent.
RELATIVE_LAG_DAYS = 1


def _age_hours(
    as_of_date: date,
    latest_loaded_at,
    latest_data_date: date | None,
) -> float:
    """Hours since a platform last loaded fresh data.

    Prefer the load timestamp; fall back to ``(as_of_date - latest_data_date)``
    in whole days × 24 when the load timestamp is absent — identical to the
    fallback ``daily_job._check_data_staleness`` uses today. A platform with no
    data at all reads as very stale (999h).
    """
    if latest_loaded_at is not None and hasattr(latest_loaded_at, "timestamp"):
        now = datetime.now(timezone.utc)
        return (now.timestamp() - latest_loaded_at.timestamp()) / 3600
    if latest_data_date is not None:
        return (as_of_date - latest_data_date).days * 24
    return 999.0


def _coerce_date(value) -> date | None:
    if value is None:
        return None
    if isinstance(value, date):
        return value
    if isinstance(value, str) and value:
        try:
            return date.fromisoformat(value[:10])
        except ValueError:
            return None
    return None


def _live_platforms_for_project(project_code: str, as_of_date: date) -> set[str]:
    """Platforms whose project has at least one still-live, non-direct media plan
    line (``flight_end >= as_of_date``) as of ``as_of_date``.

    Mirrors the pacing inclusion rule (``COALESCE(is_direct_override, is_direct)
    = FALSE`` — NULL is excluded, same as pacing.py) and guards against stale
    plan_ids / retired phases via the current-media_plans × active-project-media
    _plans JOIN. MAX(flight_end) per platform is dedup-safe on its own, so this
    intentionally avoids the per-line ROW_NUMBER dedup CTE (keeps this file out
    of the plan_id_dedup_guard registry).
    """
    sql = f"""
        SELECT l.platform_id, MAX(l.flight_end) AS max_flight_end
        FROM {bq.table('media_plan_lines')} l
        JOIN {bq.table('media_plans')} mp
          ON l.plan_id = mp.plan_id AND mp.is_current = TRUE
        JOIN {bq.table('project_media_plans')} pmp
          ON mp.project_code = pmp.project_code
         AND mp.sheet_id     = pmp.sheet_id
         AND pmp.is_active    = TRUE
        WHERE l.project_code = @project_code
          AND l.platform_id IS NOT NULL
          AND COALESCE(l.is_direct_override, l.is_direct) = FALSE
        GROUP BY l.platform_id
    """
    try:
        rows = bq.run_query(sql, [bq.string_param("project_code", project_code)])
    except Exception as e:  # pragma: no cover - defensive
        logger.warning(
            "Flight-end guard query failed for %s (guard disabled): %s",
            project_code, e,
        )
        return set()

    live: set[str] = set()
    for r in rows:
        mfe = _coerce_date(r.get("max_flight_end"))
        if mfe is not None and mfe >= as_of_date:
            live.add(r["platform_id"])
    return live


def compute_platform_freshness(
    as_of_date: date,
    project_code: str | None = None,
) -> list[dict]:
    """Return one freshness dict per platform in ``fact_digital_daily``.

    Each dict carries: ``platform_id``, ``latest_data_date`` (date|None),
    ``latest_loaded_at`` (raw|None), ``age_hours`` (float), ``is_stale`` (bool),
    ``stale_reason`` (str|None), plus ``total_days`` / ``total_rows`` for the
    admin panel. See the module docstring for the staleness rule.

    IMPORTANT — scoping: when ``project_code`` is supplied the freshness
    MEASUREMENT itself is scoped to that project (``WHERE project_code = ...``),
    not just the flight-end guard. The agency runs 5-15 concurrent campaigns
    sharing platforms; an unscoped MAX(date) over ``fact_digital_daily`` reads a
    platform as globally fresh whenever ANY other project loaded it today, which
    would hide a project whose own line on that platform has stopped reporting
    (the 26023 Meta case: Meta stopped 06-24 with a plan to 07-19, but another
    live project keeps 'meta' globally fresh). The relative-to-freshest
    comparison is then made within the project's own platforms.
    ``project_code=None`` keeps the global, agency-wide sweep (daily_job's outage
    detection) across all platforms.
    """
    where_clause = ""
    params = None
    if project_code is not None:
        where_clause = "WHERE project_code = @project_code"
        params = [bq.string_param("project_code", project_code)]

    sql = f"""
        SELECT
            platform_id,
            MAX(date)      AS latest_data_date,
            MAX(loaded_at) AS latest_loaded_at,
            COUNT(DISTINCT date) AS total_days,
            COUNT(*)             AS total_rows
        FROM {bq.table('fact_digital_daily')}
        {where_clause}
        GROUP BY platform_id
        ORDER BY platform_id
    """
    rows = bq.run_query(sql, params)

    parsed = []
    for r in rows:
        parsed.append({
            "platform_id": r["platform_id"],
            "latest_data_date": _coerce_date(r.get("latest_data_date")),
            "latest_loaded_at": r.get("latest_loaded_at"),
            "total_days": r.get("total_days", 0),
            "total_rows": r.get("total_rows", 0),
        })

    data_dates = [p["latest_data_date"] for p in parsed if p["latest_data_date"]]
    max_data_date = max(data_dates) if data_dates else None

    # flight_end guard: only meaningful when scoped to a project. None means the
    # global call — every platform is eligible for the staleness flags.
    live_platforms: set[str] | None = None
    if project_code is not None:
        live_platforms = _live_platforms_for_project(project_code, as_of_date)

    results = []
    for p in parsed:
        pid = p["platform_id"]
        ldd = p["latest_data_date"]
        age = _age_hours(as_of_date, p["latest_loaded_at"], ldd)

        abs_stale = age > DATA_STALE_HOURS
        lag_days = (max_data_date - ldd).days if (max_data_date and ldd) else 0
        rel_stale = lag_days > RELATIVE_LAG_DAYS

        is_stale = bool(abs_stale or rel_stale)
        stale_reason: str | None = None

        # flight_end guard — a platform with no still-live line for this project
        # is EXPECTED to have stopped; never flag it stale.
        if is_stale and live_platforms is not None and pid not in live_platforms:
            is_stale = False
        elif is_stale:
            parts = []
            if abs_stale:
                parts.append(f"no fresh load in {age:.0f}h (>{DATA_STALE_HOURS}h)")
            if rel_stale:
                parts.append(
                    f"latest data {ldd} lags freshest {max_data_date} by {lag_days}d"
                )
            stale_reason = "; ".join(parts)

        results.append({
            "platform_id": pid,
            "latest_data_date": ldd,
            "latest_loaded_at": p["latest_loaded_at"],
            "age_hours": round(age, 1),
            "is_stale": is_stale,
            "stale_reason": stale_reason,
            "total_days": p["total_days"],
            "total_rows": p["total_rows"],
        })

    return results
