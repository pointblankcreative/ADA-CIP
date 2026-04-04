"""GA4 transformation: Funnel.io → fact_ga4_daily.

Cross-region strategy (same as digital daily transform):
  Source: core_funnel_export.funnel_data (US region)
  Target: cip.fact_ga4_daily (northamerica-northeast1)

BigQuery cannot run a single MERGE across regions, so we:
  1. SELECT aggregated/pivoted GA4 data via a US-region job
  2. DELETE the target date range in Montreal
  3. INSERT the rows via load_table_from_json in Montreal

Can be run standalone:
    python -m ingestion.transformation.ga4_transform          # last 7 days
    python -m ingestion.transformation.ga4_transform --full   # all history
"""

import argparse
import logging
import uuid
from datetime import date, datetime, timezone
from decimal import Decimal

from google.cloud import bigquery

from backend.config import settings

logger = logging.getLogger(__name__)

TARGET_TABLE = f"{settings.gcp_project_id}.{settings.bigquery_dataset}.fact_ga4_daily"
LOG_TABLE = f"`{settings.gcp_project_id}.{settings.bigquery_dataset}.ingestion_log`"

# GA4 event types to pivot into columns
GA4_SELECT_SQL = """
SELECT
    Date AS date,
    Property_ID___GA4__Google_Analytics AS ga4_property_id,
    ANY_VALUE(Property_name___GA4__Google_Analytics) AS property_name,
    IFNULL(Session_source___GA4__Google_Analytics, '(not set)') AS session_source,
    IFNULL(Session_medium___GA4__Google_Analytics, '(not set)') AS session_medium,
    IFNULL(Session_campaign___GA4__Google_Analytics, '(not set)') AS session_campaign,
    SUM(IF(Event_name___GA4__Google_Analytics = 'session_start',
           Sessions___GA4_event_based__Google_Analytics, 0)) AS sessions,
    SUM(IF(Event_name___GA4__Google_Analytics = 'page_view',
           Views___GA4__Google_Analytics, 0)) AS page_views,
    SUM(IF(Event_name___GA4__Google_Analytics = 'first_visit',
           Event_count___GA4__Google_Analytics, 0)) AS first_visits,
    SUM(Key_events___GA4__Google_Analytics) AS key_events,
    SUM(IF(Event_name___GA4__Google_Analytics = 'sign_up',
           Event_count___GA4__Google_Analytics, 0)) AS sign_ups,
    SUM(IF(Event_name___GA4__Google_Analytics = 'scroll',
           Event_count___GA4__Google_Analytics, 0)) AS scroll_events,
    SUM(IF(Event_name___GA4__Google_Analytics = 'click',
           Event_count___GA4__Google_Analytics, 0)) AS click_events,
    SUM(IF(Event_name___GA4__Google_Analytics = 'form_start',
           Event_count___GA4__Google_Analytics, 0)) AS form_starts,
    SUM(IF(Event_name___GA4__Google_Analytics = 'form_submit',
           Event_count___GA4__Google_Analytics, 0)) AS form_submits,
    SUM(IF(Event_name___GA4__Google_Analytics = 'user_engagement',
           Event_count___GA4__Google_Analytics, 0)) AS user_engagements,
    SUM(Event_count___GA4__Google_Analytics) AS total_event_count,
    'funnel_transform' AS ingestion_source
FROM `point-blank-ada.core_funnel_export.funnel_data`
WHERE Property_ID___GA4__Google_Analytics IS NOT NULL
  AND Property_ID___GA4__Google_Analytics != ''
  {date_filter}
GROUP BY Date, Property_ID___GA4__Google_Analytics, session_source, session_medium, session_campaign
"""


def _us_client() -> bigquery.Client:
    return bigquery.Client(project=settings.gcp_project_id)


def _mtl_client() -> bigquery.Client:
    return bigquery.Client(
        project=settings.gcp_project_id,
        location=settings.gcp_region,
    )


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
        bigquery.ScalarQueryParameter("connector", "STRING", f"ga4_transform_{mode}"),
        bigquery.ScalarQueryParameter("started", "TIMESTAMP", started_at.isoformat()),
        bigquery.ScalarQueryParameter("status", "STRING", status),
        bigquery.ScalarQueryParameter("rows", "INT64", rows),
        bigquery.ScalarQueryParameter("ds", "DATE", date_start.isoformat() if date_start else None),
        bigquery.ScalarQueryParameter("de", "DATE", date_end.isoformat() if date_end else None),
        bigquery.ScalarQueryParameter("error", "STRING", error),
    ]
    job_config = bigquery.QueryJobConfig(query_parameters=params)
    mtl.query(sql, job_config=job_config).result()


def run_ga4_transformation(mode: str = "daily") -> dict:
    """Run the Funnel.io GA4 → fact_ga4_daily transformation.

    Args:
        mode: "daily" (last 7 days) or "full" (all history).

    Returns:
        Summary dict with row counts and property breakdown.
    """
    log_id = str(uuid.uuid4())
    started_at = datetime.now(timezone.utc)

    if mode == "daily":
        date_filter = "AND Date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)"
    else:
        date_filter = ""

    select_sql = GA4_SELECT_SQL.format(date_filter=date_filter)

    us = _us_client()
    mtl = _mtl_client()

    try:
        # Step 1: SELECT in US region
        logger.info("GA4 Transform [%s] started — reading from funnel_data (US)…", mode)
        job = us.query(select_sql)
        rows_raw = job.result()
        data = [_serialize_row(dict(r)) for r in rows_raw]
        row_count = len(data)
        logger.info("  Fetched %d GA4 rows from funnel_data", row_count)

        if row_count == 0:
            _log_run(mtl, log_id, mode, started_at, "success", 0)
            return {"status": "success", "mode": mode, "rows_loaded": 0, "properties": {}}

        # Determine date range
        dates = [r["date"] for r in data if r.get("date")]
        min_date = min(dates) if dates else None
        max_date = max(dates) if dates else None
        if isinstance(min_date, str):
            min_date = date.fromisoformat(min_date)
        if isinstance(max_date, str):
            max_date = date.fromisoformat(max_date)

        # Step 2: DELETE target date range in Montreal
        if min_date:
            logger.info("  Deleting existing GA4 rows for %s → %s", min_date, max_date)
            mtl.query(
                f"DELETE FROM `{TARGET_TABLE}` WHERE date >= @min_d AND date <= @max_d",
                job_config=bigquery.QueryJobConfig(query_parameters=[
                    bigquery.ScalarQueryParameter("min_d", "DATE", min_date.isoformat()),
                    bigquery.ScalarQueryParameter("max_d", "DATE", max_date.isoformat()),
                ]),
            ).result()

        # Step 3: Load into Montreal
        logger.info("  Loading %d GA4 rows into fact_ga4_daily (Montreal)…", row_count)
        load_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )
        load_job = mtl.load_table_from_json(data, TARGET_TABLE, job_config=load_config)
        load_job.result()
        loaded = load_job.output_rows or row_count

        # Property breakdown
        property_counts: dict[str, int] = {}
        for r in data:
            pid = r.get("ga4_property_id", "unknown")
            property_counts[pid] = property_counts.get(pid, 0) + 1

        logger.info("  GA4 transform complete: %d rows across %d properties",
                     loaded, len(property_counts))

        _log_run(mtl, log_id, mode, started_at, "success", loaded, min_date, max_date)

        return {
            "status": "success",
            "mode": mode,
            "rows_loaded": loaded,
            "date_range": {"start": str(min_date), "end": str(max_date)},
            "properties": property_counts,
            "log_id": log_id,
        }

    except Exception as e:
        logger.exception("GA4 Transform [%s] failed", mode)
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


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    parser = argparse.ArgumentParser(description="GA4 Funnel → fact_ga4_daily transform")
    parser.add_argument("--full", action="store_true", help="Full history backfill (default: last 7 days)")
    args = parser.parse_args()
    result = run_ga4_transformation("full" if args.full else "daily")
    print(result)
