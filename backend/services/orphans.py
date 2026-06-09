"""Orphan Project Auto-Discovery.

Scans the fact tables for ``project_code`` values that appear in spend/activity
data but do NOT have a matching row in ``dim_projects``. These are "orphans" —
campaigns that have been ingested but never configured in CIP.

The Overview page surfaces orphans with a single **Configure** CTA (a redirect
to ``/admin/projects/new?code={project_code}``). Suppression is intentionally
NOT a UI action — to stop a code surfacing you add a row to the
``dismissed_orphans`` control table in BigQuery by hand. The ``level`` column
gives two tiers:

- ``dismissed`` — hidden from the active panel, still listed under "show
  dismissed" so you can see what you've set aside.
- ``archived`` — hidden from BOTH the active panel and the dismissed list, for
  codes that will never be recoverable into CIP and would otherwise show
  confusing partial data. The spend data stays intact in the fact tables; the
  control-table row is the record of why it was set aside.

Doing this by table edit (rather than a button) is deliberate: nobody can
suppress a code by accident, and every suppression is an explicit, attributable
row.

Scanner unions three sources so we catch all forms of activity:
- ``fact_digital_daily`` — digital spend, impressions, clicks, etc.
- ``fact_dooh_daily`` — DOOH (Perion) spend
- ``fact_adset_daily`` — reach/frequency (no spend column)
"""

from __future__ import annotations

import logging
from typing import Any

from backend.services import bigquery_client as bq

logger = logging.getLogger(__name__)

# Suppression tiers stored in the dismissed_orphans.level column.
LEVEL_DISMISSED = "dismissed"
LEVEL_ARCHIVED = "archived"

_SCHEMA_ENSURED = False


def _ensure_schema() -> None:
    """Idempotently add the ``level`` column to dismissed_orphans.

    Mirrors the self-healing migration in services/pacing.py so a deploy never
    depends on a separate manual ALTER landing first. Best-effort: a failure
    here only logs; the scan that follows surfaces a real error if the column
    is genuinely missing.
    """
    global _SCHEMA_ENSURED
    if _SCHEMA_ENSURED:
        return
    from google.cloud import bigquery

    from backend.config import settings

    client = bigquery.Client(project=settings.gcp_project_id, location=settings.gcp_region)
    try:
        client.query(
            f"ALTER TABLE {bq.table('dismissed_orphans')} "
            "ADD COLUMN IF NOT EXISTS level STRING"
        ).result()
    except Exception as e:  # noqa: BLE001 — never block a scan on migration
        if "Already Exists" not in str(e) and "Duplicate" not in str(e):
            logger.warning("dismissed_orphans schema migration warning: %s", e)
    _SCHEMA_ENSURED = True


def _hydrate_row(row: dict) -> dict[str, Any]:
    """Coerce BigQuery row types to JSON-serialisable primitives."""
    by_platform = row.get("by_platform") or []
    normalised_platforms: list[dict[str, Any]] = []
    for p in by_platform:
        # Row objects come back as dict-like; make sure floats are floats.
        spend = p.get("spend") if isinstance(p, dict) else p["spend"]
        row_count = p.get("row_count") if isinstance(p, dict) else p["row_count"]
        platform_id = p.get("platform_id") if isinstance(p, dict) else p["platform_id"]
        normalised_platforms.append({
            "platform_id": platform_id,
            "spend": float(spend) if spend is not None else 0.0,
            "row_count": int(row_count) if row_count is not None else 0,
        })

    total_spend = row.get("total_spend")
    total_rows = row.get("total_rows")
    first_date = row.get("first_date")
    last_date = row.get("last_date")
    dismissed_at = row.get("dismissed_at")

    return {
        "project_code": row["project_code"],
        "total_spend": float(total_spend) if total_spend is not None else 0.0,
        "total_rows": int(total_rows) if total_rows is not None else 0,
        "first_date": first_date.isoformat() if first_date else None,
        "last_date": last_date.isoformat() if last_date else None,
        "by_platform": normalised_platforms,
        "dismissed": dismissed_at is not None,
        "dismissed_at": dismissed_at.isoformat() if dismissed_at else None,
        "dismissed_by": row.get("dismissed_by"),
        "dismissed_reason": row.get("dismissed_reason"),
        "level": row.get("level"),
    }


def scan_orphans(include_dismissed: bool = False) -> list[dict[str, Any]]:
    """Return all project_codes with spend/activity that aren't in dim_projects.

    Visibility follows the suppression ``level`` in ``dismissed_orphans``:
    - not in the table              → always returned
    - level ``dismissed`` (or NULL) → returned only when ``include_dismissed``
    - level ``archived``            → never returned (hidden from both views)

    Sorted by total_spend DESC, then project_code.
    """
    _ensure_schema()
    sql = f"""
    WITH digital_activity AS (
      SELECT project_code, platform_id,
             SUM(spend) AS spend,
             COUNT(*) AS row_count,
             MIN(date) AS first_date,
             MAX(date) AS last_date
      FROM {bq.table('fact_digital_daily')}
      WHERE project_code IS NOT NULL
      GROUP BY project_code, platform_id
    ),
    dooh_activity AS (
      SELECT project_code, platform_id,
             SUM(spend) AS spend,
             COUNT(*) AS row_count,
             MIN(date) AS first_date,
             MAX(date) AS last_date
      FROM {bq.table('fact_dooh_daily')}
      WHERE project_code IS NOT NULL
      GROUP BY project_code, platform_id
    ),
    adset_activity AS (
      -- fact_adset_daily is reach/frequency only (no spend column).
      -- We still count it so projects with only ad-set data surface as orphans.
      SELECT project_code, platform_id,
             CAST(0 AS NUMERIC) AS spend,
             COUNT(*) AS row_count,
             MIN(date) AS first_date,
             MAX(date) AS last_date
      FROM {bq.table('fact_adset_daily')}
      WHERE project_code IS NOT NULL
      GROUP BY project_code, platform_id
    ),
    combined AS (
      SELECT * FROM digital_activity
      UNION ALL
      SELECT * FROM dooh_activity
      UNION ALL
      SELECT * FROM adset_activity
    ),
    by_platform AS (
      SELECT
        project_code,
        platform_id,
        SUM(spend) AS spend,
        SUM(row_count) AS row_count,
        MIN(first_date) AS first_date,
        MAX(last_date) AS last_date
      FROM combined
      GROUP BY project_code, platform_id
    ),
    per_project AS (
      SELECT
        project_code,
        SUM(spend) AS total_spend,
        SUM(row_count) AS total_rows,
        MIN(first_date) AS first_date,
        MAX(last_date) AS last_date,
        ARRAY_AGG(
          STRUCT(platform_id, spend, row_count)
          ORDER BY spend DESC, row_count DESC
        ) AS by_platform
      FROM by_platform
      GROUP BY project_code
    )
    SELECT
      p.project_code,
      p.total_spend,
      p.total_rows,
      p.first_date,
      p.last_date,
      p.by_platform,
      d.dismissed_at,
      d.dismissed_by,
      d.reason AS dismissed_reason,
      d.level
    FROM per_project p
    LEFT JOIN {bq.table('dim_projects')} dp USING (project_code)
    LEFT JOIN {bq.table('dismissed_orphans')} d USING (project_code)
    WHERE dp.project_code IS NULL
      AND (
        d.project_code IS NULL
        OR (@include_dismissed AND COALESCE(d.level, '{LEVEL_DISMISSED}') = '{LEVEL_DISMISSED}')
      )
    ORDER BY p.total_spend DESC, p.project_code
    """

    params = [bq.scalar_param("include_dismissed", "BOOL", include_dismissed)]
    rows = bq.run_query(sql, params=params)
    return [_hydrate_row(r) for r in rows]


def get_orphan(project_code: str) -> dict[str, Any] | None:
    """Fetch a single orphan by project_code (includes dismissed, not archived)."""
    for r in scan_orphans(include_dismissed=True):
        if r["project_code"] == project_code:
            return r
    return None
