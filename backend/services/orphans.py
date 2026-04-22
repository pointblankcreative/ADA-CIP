"""Orphan Project Auto-Discovery.

Scans the fact tables for ``project_code`` values that appear in spend/activity
data but do NOT have a matching row in ``dim_projects``. These are "orphans" —
campaigns that have been ingested but never configured in CIP.

The Overview page surfaces orphans with two CTAs:

- **Configure** — redirect to ``/admin/projects/new?code={project_code}`` so
  the existing project-creation form can prefill the code. We do not create
  a skeleton ``dim_projects`` row here; a project_code without client +
  project_name + dates is not a real project.
- **Dismiss** — insert a row into ``dismissed_orphans``. Permanent until the
  user explicitly un-dismisses. Typical reason: "test account spend" or
  "client code mismatch, real code is 25XXX".

Scanner unions three sources so we catch all forms of activity:

- ``fact_digital_daily`` — digital spend, impressions, clicks, etc.
- ``fact_dooh_daily`` — DOOH (Perion) spend
- ``fact_adset_daily`` — reach/frequency (no spend column)

A project is an orphan if it has at least one row in any of the three and is
missing from ``dim_projects``. By default dismissed orphans are hidden; pass
``include_dismissed=True`` to surface them for un-dismiss UI.
"""

from __future__ import annotations

import logging
from typing import Any

from backend.services import bigquery_client as bq

logger = logging.getLogger(__name__)


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
    }


def scan_orphans(include_dismissed: bool = False) -> list[dict[str, Any]]:
    """Return all project_codes with spend/activity that aren't in dim_projects.

    Sorted by total_spend DESC, then project_code. If ``include_dismissed`` is
    True, dismissed orphans are included (for an un-dismiss UI).
    """
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
      d.reason AS dismissed_reason
    FROM per_project p
    LEFT JOIN {bq.table('dim_projects')} dp USING (project_code)
    LEFT JOIN {bq.table('dismissed_orphans')} d USING (project_code)
    WHERE dp.project_code IS NULL
      AND (@include_dismissed OR d.project_code IS NULL)
    ORDER BY p.total_spend DESC, p.project_code
    """

    params = [bq.scalar_param("include_dismissed", "BOOL", include_dismissed)]
    rows = bq.run_query(sql, params=params)
    return [_hydrate_row(r) for r in rows]


def get_orphan(project_code: str) -> dict[str, Any] | None:
    """Fetch a single orphan by project_code (include dismissed)."""
    all_rows = scan_orphans(include_dismissed=True)
    for r in all_rows:
        if r["project_code"] == project_code:
            return r
    return None


def dismiss(
    project_code: str,
    dismissed_by: str | None = None,
    reason: str | None = None,
) -> dict[str, Any]:
    """Mark a project_code as dismissed. Idempotent (MERGE).

    Returns the hydrated orphan row after dismissal, or a minimal record if
    the underlying activity has since disappeared.
    """
    sql = f"""
    MERGE {bq.table('dismissed_orphans')} t
    USING (
      SELECT
        @project_code     AS project_code,
        CURRENT_TIMESTAMP() AS dismissed_at,
        @dismissed_by     AS dismissed_by,
        @reason           AS reason
    ) s
    ON t.project_code = s.project_code
    WHEN NOT MATCHED THEN
      INSERT (project_code, dismissed_at, dismissed_by, reason)
      VALUES (s.project_code, s.dismissed_at, s.dismissed_by, s.reason)
    WHEN MATCHED THEN
      UPDATE SET
        dismissed_at = s.dismissed_at,
        dismissed_by = s.dismissed_by,
        reason       = s.reason
    """

    params = [
        bq.string_param("project_code", project_code),
        bq.string_param("dismissed_by", dismissed_by or ""),
        bq.string_param("reason", reason or ""),
    ]
    bq.run_query(sql, params=params)

    # Return the hydrated orphan (may be None if activity has been purged).
    existing = get_orphan(project_code)
    if existing is not None:
        return existing
    return {
        "project_code": project_code,
        "total_spend": 0.0,
        "total_rows": 0,
        "first_date": None,
        "last_date": None,
        "by_platform": [],
        "dismissed": True,
        "dismissed_at": None,
        "dismissed_by": dismissed_by,
        "dismissed_reason": reason,
    }


def undismiss(project_code: str) -> bool:
    """Remove a dismissal row. Returns True if a row was removed."""
    from google.cloud import bigquery

    sql = f"""
    DELETE FROM {bq.table('dismissed_orphans')}
    WHERE project_code = @project_code
    """
    client = bq.get_client()
    result = client.query(
        sql,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[bq.string_param("project_code", project_code)],
        ),
    ).result()
    return (result.num_dml_affected_rows or 0) > 0
