"""Ad-set / campaign reach & frequency: Funnel.io → fact_adset_daily.

Cross-region pattern (same as ga4_transform):
  US-region SELECT → DELETE Montreal date range → load_table_from_json (Montreal).

Run:
    python -m ingestion.transformation.adset_transform
    python -m ingestion.transformation.adset_transform --full
"""

import argparse
import logging
import uuid
from datetime import date, datetime, timezone
from decimal import Decimal

from google.cloud import bigquery

from backend.config import settings

logger = logging.getLogger(__name__)

TARGET_TABLE = f"{settings.gcp_project_id}.{settings.bigquery_dataset}.fact_adset_daily"
LOG_TABLE = f"`{settings.gcp_project_id}.{settings.bigquery_dataset}.ingestion_log`"
FUNNEL_TABLE = "point-blank-ada.core_funnel_export.funnel_data"
CPM_TABLE = "point-blank-ada.cip.campaign_project_mapping"

# Project code regex aligned with main digital transform
PC = r"(?:^|_|\s|-)(2[0-9]\d{3})(?:_|\s|-|$)"

ADSET_SELECT_SQL = """
SELECT
  ed.date,
  ed.project_code,
  ed.platform_id,
  ed.account_id,
  ed.campaign_id,
  ed.campaign_name,
  ed.ad_set_id,
  ed.ad_set_name,
  ed.reach,
  ed.frequency,
  ed.reach_window,
  ed.impressions,
  ed.video_views,
  ed.video_completions,
  'funnel_transform' AS ingestion_source,
  CURRENT_TIMESTAMP() AS loaded_at
FROM (
  SELECT
    b.date,
    COALESCE(
      REGEXP_EXTRACT(b.campaign_name, r'{pc}'),
      REGEXP_EXTRACT(b.ad_set_name, r'{pc}'),
      cpm.project_code
    ) AS project_code,
    b.platform_id,
    b.account_id,
    b.campaign_id,
    b.campaign_name,
    b.ad_set_id,
    b.ad_set_name,
    b.reach,
    b.frequency,
    b.reach_window,
    b.impressions,
    b.video_views,
    b.video_completions
  FROM (
    -- Meta: ad-set grain (no ad-level row)
    SELECT
      CAST(Date AS DATE) AS date,
      'meta' AS platform_id,
      Ad_Account_ID__Facebook_Ads AS account_id,
      Campaign_ID__Facebook_Ads AS campaign_id,
      Campaign_Name__Facebook_Ads AS campaign_name,
      Ad_Set_ID__Facebook_Ads AS ad_set_id,
      Ad_Set_Name__Facebook_Ads AS ad_set_name,
      CAST(Reach___7_Day_Ad_Set__Facebook_Ads AS INT64) AS reach,
      CAST(Frequency___7_Day_Ad_Set__Facebook_Ads AS FLOAT64) AS frequency,
      '7d' AS reach_window,
      CAST(Impressions__Facebook_Ads AS INT64) AS impressions,
      CAST(Video_Plays__Facebook_Ads AS INT64) AS video_views,
      CAST(Video_thruplay__Facebook_Ads AS INT64) AS video_completions
    FROM `{funnel}`
    WHERE Date {date_filter}
      AND Campaign_ID__Facebook_Ads IS NOT NULL
      AND Ad_Set_Name__Facebook_Ads IS NOT NULL
      AND Ad_Name__Facebook_Ads IS NULL
      AND (
        Reach___7_Day_Ad_Set__Facebook_Ads IS NOT NULL
        OR Frequency___7_Day_Ad_Set__Facebook_Ads IS NOT NULL
      )

    UNION ALL

    -- StackAdapt: creative = reach grain (1d window)
    SELECT
      CAST(Date AS DATE) AS date,
      'stackadapt' AS platform_id,
      Advertiser_ID__StackAdapt AS account_id,
      Campaign_ID__StackAdapt AS campaign_id,
      Campaign__StackAdapt AS campaign_name,
      Creative_ID__StackAdapt AS ad_set_id,
      Creative__StackAdapt AS ad_set_name,
      CAST(Unique_impressions_1_Day_Creative__StackAdapt AS INT64) AS reach,
      CAST(Frequency_1_Day_Creative__StackAdapt AS FLOAT64) AS frequency,
      '1d' AS reach_window,
      CAST(Impressions__StackAdapt AS INT64) AS impressions,
      CAST(Video_started__StackAdapt AS INT64) AS video_views,
      CAST(Video_completed_95__StackAdapt AS INT64) AS video_completions
    FROM `{funnel}`
    WHERE Date {date_filter}
      AND Campaign_ID__StackAdapt IS NOT NULL
      AND Creative_ID__StackAdapt IS NOT NULL
      AND Creative__StackAdapt IS NOT NULL
      AND (
        Unique_impressions_1_Day_Creative__StackAdapt IS NOT NULL
        OR Frequency_1_Day_Creative__StackAdapt IS NOT NULL
      )

    UNION ALL

    -- TikTok: ad group grain without ad row
    SELECT
      CAST(Date AS DATE) AS date,
      'tiktok' AS platform_id,
      Advertiser_ID__TikTok AS account_id,
      Campaign_ID__TikTok AS campaign_id,
      Campaign_name__TikTok AS campaign_name,
      Adgroup_ID__TikTok AS ad_set_id,
      Adgroup_name__TikTok AS ad_set_name,
      CAST(Reach___7_Day_Adgroup__TikTok AS INT64) AS reach,
      CAST(Frequency___7_Day_Adgroup__TikTok AS FLOAT64) AS frequency,
      '7d' AS reach_window,
      CAST(Impressions__TikTok AS INT64) AS impressions,
      CAST(NULL AS INT64) AS video_views,
      CAST(NULL AS INT64) AS video_completions
    FROM `{funnel}`
    WHERE Date {date_filter}
      AND Campaign_ID__TikTok IS NOT NULL
      AND Adgroup_name__TikTok IS NOT NULL
      AND Ad_Name__TikTok IS NULL
      AND (
        Reach___7_Day_Adgroup__TikTok IS NOT NULL
        OR Frequency___7_Day_Adgroup__TikTok IS NOT NULL
      )

    UNION ALL

    -- Reddit: ad group grain without ad row
    SELECT
      CAST(Date AS DATE) AS date,
      'reddit' AS platform_id,
      Account_ID__Reddit AS account_id,
      Campaign_ID__Reddit AS campaign_id,
      Campaign_Name__Reddit AS campaign_name,
      Ad_Group_ID__Reddit AS ad_set_id,
      Ad_Group_Name__Reddit AS ad_set_name,
      CAST(Reach___7_Days_Adgroup__Reddit AS INT64) AS reach,
      CAST(Frequency___7_Days_Adgroup__Reddit AS FLOAT64) AS frequency,
      '7d' AS reach_window,
      CAST(Impressions__Reddit AS INT64) AS impressions,
      CAST(Video_Starts__Reddit AS INT64) AS video_views,
      CAST(Video_Watches_100__Reddit AS INT64) AS video_completions
    FROM `{funnel}`
    WHERE Date {date_filter}
      AND Campaign_ID__Reddit IS NOT NULL
      AND Ad_Group_Name__Reddit IS NOT NULL
      AND Ad_Name__Reddit IS NULL
      AND (
        Reach___7_Days_Adgroup__Reddit IS NOT NULL
        OR Frequency___7_Days_Adgroup__Reddit IS NOT NULL
      )

    UNION ALL

    -- Snapchat: campaign-level reach (no ad / squad on row)
    SELECT
      CAST(Date AS DATE) AS date,
      'snapchat' AS platform_id,
      Account_ID__Snapchat AS account_id,
      Campaign_ID__Snapchat AS campaign_id,
      Campaign_Name__Snapchat AS campaign_name,
      CAST(NULL AS STRING) AS ad_set_id,
      CAST(NULL AS STRING) AS ad_set_name,
      CAST(Reach___7_Day_Campaign__Snapchat AS INT64) AS reach,
      CAST(Frequency___7_Day_Campaign__Snapchat AS FLOAT64) AS frequency,
      '7d' AS reach_window,
      CAST(Impressions__Snapchat AS INT64) AS impressions,
      CAST(Video_Views_time_based__Snapchat AS INT64) AS video_views,
      CAST(NULL AS INT64) AS video_completions
    FROM `{funnel}`
    WHERE Date {date_filter}
      AND Campaign_ID__Snapchat IS NOT NULL
      AND Campaign_Name__Snapchat IS NOT NULL
      AND Ad_ID__Snapchat IS NULL
      AND (
        Reach___7_Day_Campaign__Snapchat IS NOT NULL
        OR Frequency___7_Day_Campaign__Snapchat IS NOT NULL
      )

    UNION ALL

    -- LinkedIn: campaign-level reach (no creative on row)
    SELECT
      CAST(Date AS DATE) AS date,
      'linkedin' AS platform_id,
      CAST(NULL AS STRING) AS account_id,
      Campaign_ID__LinkedIn AS campaign_id,
      Campaign__LinkedIn AS campaign_name,
      CAST(NULL AS STRING) AS ad_set_id,
      CAST(NULL AS STRING) AS ad_set_name,
      CAST(Reach___7_Day_Campaign__LinkedIn AS INT64) AS reach,
      CAST(Average_frequency___7_Day_Campaign__LinkedIn AS FLOAT64) AS frequency,
      '7d' AS reach_window,
      CAST(Impressions__LinkedIn AS INT64) AS impressions,
      CAST(NULL AS INT64) AS video_views,
      CAST(NULL AS INT64) AS video_completions
    FROM `{funnel}`
    WHERE Date {date_filter}
      AND Campaign_ID__LinkedIn IS NOT NULL
      AND Campaign__LinkedIn IS NOT NULL
      AND Creative_ID__LinkedIn IS NULL
      AND (
        Reach___7_Day_Campaign__LinkedIn IS NOT NULL
        OR Average_frequency___7_Day_Campaign__LinkedIn IS NOT NULL
      )
  ) b
  LEFT JOIN `{cpm}` cpm
    ON b.platform_id = cpm.platform_id
    AND b.campaign_name LIKE cpm.campaign_name
) ed
WHERE ed.project_code IS NOT NULL
"""


def _build_sql(date_filter: str) -> str:
    return ADSET_SELECT_SQL.format(
        pc=PC,
        funnel=FUNNEL_TABLE,
        cpm=CPM_TABLE,
        date_filter=date_filter,
    )


def _us_client() -> bigquery.Client:
    return bigquery.Client(project=settings.gcp_project_id)


def _mtl_client() -> bigquery.Client:
    return bigquery.Client(
        project=settings.gcp_project_id,
        location=settings.gcp_region,
    )


def _serialize_row(row: dict) -> dict:
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
        bigquery.ScalarQueryParameter("connector", "STRING", f"adset_transform_{mode}"),
        bigquery.ScalarQueryParameter("started", "TIMESTAMP", started_at.isoformat()),
        bigquery.ScalarQueryParameter("status", "STRING", status),
        bigquery.ScalarQueryParameter("rows", "INT64", rows),
        bigquery.ScalarQueryParameter("ds", "DATE", date_start.isoformat() if date_start else None),
        bigquery.ScalarQueryParameter("de", "DATE", date_end.isoformat() if date_end else None),
        bigquery.ScalarQueryParameter("error", "STRING", error),
    ]
    job_config = bigquery.QueryJobConfig(query_parameters=params)
    mtl.query(sql, job_config=job_config).result()


def run_adset_transformation(mode: str = "daily") -> dict:
    """Funnel.io → fact_adset_daily."""
    log_id = str(uuid.uuid4())
    started_at = datetime.now(timezone.utc)

    if mode == "daily":
        date_filter = ">= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)"
    else:
        date_filter = "IS NOT NULL"

    select_sql = _build_sql(date_filter)

    us = _us_client()
    mtl = _mtl_client()

    try:
        logger.info("Ad-set Transform [%s] — reading reach/frequency from funnel_data (US)…", mode)
        job = us.query(select_sql)
        rows_raw = job.result()
        data = [_serialize_row(dict(r)) for r in rows_raw]
        row_count = len(data)
        logger.info("  Fetched %d ad-set reach rows", row_count)

        if row_count == 0:
            _log_run(mtl, log_id, mode, started_at, "success", 0)
            return {"status": "success", "mode": mode, "rows_loaded": 0}

        dates = [r["date"] for r in data if r.get("date")]
        min_date = min(dates) if dates else None
        max_date = max(dates) if dates else None
        if isinstance(min_date, str):
            min_date = date.fromisoformat(min_date)
        if isinstance(max_date, str):
            max_date = date.fromisoformat(max_date)

        if min_date:
            logger.info("  Replacing fact_adset_daily for %s → %s", min_date, max_date)
            mtl.query(
                f"DELETE FROM `{TARGET_TABLE}` WHERE date >= @min_d AND date <= @max_d",
                job_config=bigquery.QueryJobConfig(query_parameters=[
                    bigquery.ScalarQueryParameter("min_d", "DATE", min_date.isoformat()),
                    bigquery.ScalarQueryParameter("max_d", "DATE", max_date.isoformat()),
                ]),
            ).result()

        load_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )
        load_job = mtl.load_table_from_json(data, TARGET_TABLE, job_config=load_config)
        load_job.result()
        loaded = load_job.output_rows or row_count

        _log_run(mtl, log_id, mode, started_at, "success", loaded, min_date, max_date)

        return {
            "status": "success",
            "mode": mode,
            "rows_loaded": loaded,
            "date_range": {"start": str(min_date), "end": str(max_date)},
            "log_id": log_id,
        }

    except Exception as e:
        logger.exception("Ad-set Transform [%s] failed", mode)
        try:
            _log_run(mtl, log_id, mode, started_at, "failed", error=str(e)[:500])
        except Exception:
            logger.exception("Failed to write error to ingestion_log")
        return {"status": "failed", "mode": mode, "error": str(e)[:500], "log_id": log_id}
    finally:
        us.close()
        mtl.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    parser = argparse.ArgumentParser(description="Ad-set reach → fact_adset_daily")
    parser.add_argument("--full", action="store_true", help="Full history (default: last 7 days)")
    args = parser.parse_args()
    print(run_adset_transformation("full" if args.full else "daily"))
