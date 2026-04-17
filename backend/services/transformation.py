"""Transformation orchestrator: Funnel.io → fact_digital_daily.

Cross-region strategy: the source table (core_funnel_export.funnel_data) lives
in US, while the target (cip.fact_digital_daily) is in northamerica-northeast1.
BigQuery cannot run a single MERGE across regions, so we:
  1. SELECT transformed data via a US-region job
  2. DELETE the target date range in Montreal
  3. INSERT the rows via load_table_from_json in Montreal
"""

import logging
import uuid
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

from google.cloud import bigquery
from google.cloud import exceptions as gcp_exceptions

from backend.config import settings

logger = logging.getLogger(__name__)

SQL_DIR = Path(__file__).resolve().parent.parent.parent / "ingestion" / "transformation"
DAILY_SQL = SQL_DIR / "transform_funnel_to_unified.sql"
FULL_SQL = SQL_DIR / "transform_funnel_to_unified_full_history.sql"

TARGET_TABLE = f"{settings.gcp_project_id}.{settings.bigquery_dataset}.fact_digital_daily"
LOG_TABLE = f"`{settings.gcp_project_id}.{settings.bigquery_dataset}.ingestion_log`"


def _us_client() -> bigquery.Client:
    return bigquery.Client(project=settings.gcp_project_id)


def _mtl_client() -> bigquery.Client:
    return bigquery.Client(
        project=settings.gcp_project_id,
        location=settings.gcp_region,
    )


def _extract_select(sql_text: str) -> str:
    """Strip the MERGE clause and the cross-region LEFT JOIN from the SQL,
    returning just the CTE + SELECT that can run in the US region.

    The campaign_project_mapping JOIN is applied as a post-load step in Montreal
    because it references a Montreal-region table.
    """
    import re
    cte_part = sql_text.split("MERGE INTO")[0].rstrip()
    # Remove the LEFT JOIN to campaign_project_mapping (Montreal table can't be
    # referenced from a US-region job). The mapping fallback is applied post-load.
    cte_part = re.sub(
        r'\s*LEFT\s+JOIN\s+`point-blank-ada\.cip\.campaign_project_mapping`\s+cpm\s+'
        r'ON\s+pd\.platform_id\s*=\s*cpm\.platform_id\s+'
        r'AND\s+pd\.campaign_name\s+LIKE\s+cpm\.campaign_name\s*',
        '\n',
        cte_part,
        flags=re.IGNORECASE | re.DOTALL,
    )
    # Remove the cpm.project_code fallback from COALESCE — keep just the two regex extracts
    cte_part = re.sub(
        r',\s*\n\s*cpm\.project_code\s*\n',
        '\n',
        cte_part,
    )
    return cte_part + """
SELECT * EXCEPT(rn) FROM (
  SELECT
      date,
      platform_id,
      campaign_id,
      COALESCE(ad_set_id, '') AS ad_set_id,
      COALESCE(ad_id, '') AS ad_id,
      campaign_name,
      ad_set_name,
      ad_name,
      account_id,
      account_name,
      project_code,
      CAST(spend AS NUMERIC) AS spend,
      impressions,
      clicks,
      reach,
      frequency,
      video_views,
      video_completions,
      CAST(conversions AS NUMERIC) AS conversions,
      engagements,
      cpm,
      cpc,
      ctr,
      -- Diagnostic signal columns
      video_views_3s,
      thruplay,
      video_q25,
      video_q50,
      video_q75,
      video_q100,
      post_engagement,
      post_reactions,
      post_comments,
      outbound_clicks,
      landing_page_views,
      CAST(registrations AS NUMERIC) AS registrations,
      CAST(leads AS NUMERIC) AS leads,
      CAST(on_platform_leads AS NUMERIC) AS on_platform_leads,
      CAST(contacts AS NUMERIC) AS contacts,
      CAST(donations AS NUMERIC) AS donations,
      campaign_objective,
      viewability_measured,
      viewability_viewed,
      ingestion_source,
      loaded_at,
      ROW_NUMBER() OVER (
        PARTITION BY date, platform_id, campaign_id,
                     COALESCE(ad_set_id, ''), COALESCE(ad_id, '')
        ORDER BY spend DESC, impressions DESC
      ) AS rn
  FROM enriched_data
)
WHERE rn = 1
"""


def _serialize_row(row: dict) -> dict:
    """Convert BigQuery Row types to JSON-safe values."""
    out = {}
    for k, v in row.items():
        if isinstance(v, (date, datetime)):
            out[k] = v.isoformat()
        elif isinstance(v, Decimal):
            out[k] = float(v)
        else:
            out[k] = v
    return out


def _log_run(
    mtl: bigquery.Client,
    log_id: str,
    mode: str,
    started_at: datetime,
    status: str,
    rows: int = 0,
    date_start: date | None = None,
    date_end: date | None = None,
    error: str | None = None,
):
    sql = f"""
        INSERT INTO {LOG_TABLE}
            (log_id, source_platform, connector_name, run_started_at,
             run_completed_at, status, rows_fetched, rows_upserted,
             date_range_start, date_range_end, error_message)
        VALUES
            (@log_id, 'funnel_io', @connector, @started,
             CURRENT_TIMESTAMP(), @status, @rows, @rows,
             @ds, @de, @error)
    """
    params = [
        bigquery.ScalarQueryParameter("log_id", "STRING", log_id),
        bigquery.ScalarQueryParameter("connector", "STRING", f"transform_{mode}"),
        bigquery.ScalarQueryParameter("started", "TIMESTAMP", started_at.isoformat()),
        bigquery.ScalarQueryParameter("status", "STRING", status),
        bigquery.ScalarQueryParameter("rows", "INT64", rows),
        bigquery.ScalarQueryParameter("ds", "DATE", date_start.isoformat() if date_start else None),
        bigquery.ScalarQueryParameter("de", "DATE", date_end.isoformat() if date_end else None),
        bigquery.ScalarQueryParameter("error", "STRING", error),
    ]
    job_config = bigquery.QueryJobConfig(query_parameters=params)
    mtl.query(sql, job_config=job_config).result()


def _apply_mapping_fallback(mtl: bigquery.Client) -> None:
    """Apply campaign_project_mapping overrides to rows with NULL project_code.

    This runs entirely in Montreal, updating fact_digital_daily rows where
    the regex extraction failed but a manual mapping exists.
    """
    sql = f"""
        UPDATE `{TARGET_TABLE}` f
        SET f.project_code = m.project_code
        FROM `{settings.gcp_project_id}.{settings.bigquery_dataset}.campaign_project_mapping` m
        WHERE f.project_code IS NULL
          AND f.platform_id = m.platform_id
          AND f.campaign_name LIKE m.campaign_name
    """
    try:
        result = mtl.query(sql).result()
        logger.info("  Mapping fallback applied (updated rows with campaign_project_mapping)")
    except (gcp_exceptions.GoogleCloudError, gcp_exceptions.NotFound) as e:
        logger.warning("  Mapping fallback query failed (non-fatal): %s", e, exc_info=True)


def run_transformation(mode: str = "daily") -> dict:
    """Run the funnel → fact_digital_daily transformation.

    Args:
        mode: "daily" (last 7 days) or "full" (all history).

    Returns:
        Summary dict with row counts and platform breakdown.
    """
    log_id = str(uuid.uuid4())
    started_at = datetime.now(timezone.utc)
    sql_path = DAILY_SQL if mode == "daily" else FULL_SQL

    if not sql_path.exists():
        raise FileNotFoundError(f"SQL file not found: {sql_path}")

    sql_text = sql_path.read_text()
    select_sql = _extract_select(sql_text)

    us = _us_client()
    mtl = _mtl_client()

    try:
        # Step 1: SELECT in US
        logger.info("Transformation [%s] started — reading from funnel_data (US)…", mode)
        job = us.query(select_sql)
        rows_raw = job.result()
        data = [_serialize_row(dict(r)) for r in rows_raw]
        row_count = len(data)
        logger.info("  Fetched %d rows from funnel_data", row_count)

        if row_count == 0:
            _log_run(mtl, log_id, mode, started_at, "success", 0)
            return {"status": "success", "mode": mode, "rows_loaded": 0, "platforms": {}}

        # Determine date range
        dates = [r["date"] for r in data if r.get("date")]
        min_date = min(dates) if dates else None
        max_date = max(dates) if dates else None
        if isinstance(min_date, str):
            min_date = date.fromisoformat(min_date)
        if isinstance(max_date, str):
            max_date = date.fromisoformat(max_date)

        # Step 2: DELETE target rows in Montreal before re-loading
        if min_date:
            if mode == "full":
                logger.info("  FULL mode: truncating fact_digital_daily before reload")
                mtl.query(f"TRUNCATE TABLE `{TARGET_TABLE}`").result()
            else:
                logger.info("  Deleting existing rows for %s → %s", min_date, max_date)
                mtl.query(
                    f"DELETE FROM `{TARGET_TABLE}` WHERE date >= @min_d AND date <= @max_d",
                    job_config=bigquery.QueryJobConfig(query_parameters=[
                        bigquery.ScalarQueryParameter("min_d", "DATE", min_date.isoformat()),
                        bigquery.ScalarQueryParameter("max_d", "DATE", max_date.isoformat()),
                    ]),
                ).result()

        # Step 3: Load into Montreal
        logger.info("  Loading %d rows into fact_digital_daily (Montreal)…", row_count)
        load_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )
        load_job = mtl.load_table_from_json(data, TARGET_TABLE, job_config=load_config)
        load_job.result()
        loaded = load_job.output_rows or row_count

        # Platform breakdown
        platform_counts: dict[str, int] = {}
        for r in data:
            pid = r.get("platform_id", "unknown")
            platform_counts[pid] = platform_counts.get(pid, 0) + 1

        logger.info("  Transformation complete: %d rows across %d platforms",
                     loaded, len(platform_counts))

        # Step 4: Apply campaign_project_mapping fallback (runs in Montreal)
        _apply_mapping_fallback(mtl)

        _log_run(mtl, log_id, mode, started_at, "success", loaded, min_date, max_date)

        return {
            "status": "success",
            "mode": mode,
            "rows_loaded": loaded,
            "date_range": {"start": str(min_date), "end": str(max_date)},
            "platforms": platform_counts,
            "log_id": log_id,
        }

    except Exception as e:
        logger.exception("Transformation [%s] failed", mode)
        try:
            _log_run(mtl, log_id, mode, started_at, "failed", error=str(e)[:500])
        except Exception:
            logger.exception("Failed to write error to ingestion_log")
        return {
            "status": "failed",
            "mode": mode,
            "error": str(e)[:500],
            "log_id": log_id,
        }
    finally:
        us.close()
        mtl.close()
