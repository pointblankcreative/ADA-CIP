"""StackAdapt reach/frequency direct feed (Asana 1215990005858637).

Funnel's StackAdapt reach/frequency come from a 1-day per-creative field that
overcounts true dedup reach by 7-10x, so they sit hidden behind a stopgap in
routers/performance.py. This module pulls the REAL numbers from StackAdapt's
own `reachFrequency` GraphQL API and lands them in a dedicated dataset
(cip_stackadapt.stackadapt_reach_frequency) so Funnel's spend/impressions/
clicks contract stays untouched.

Modelled on services/creative_assets.py:
  * httpx POST to the StackAdapt GraphQL endpoint, Authorization: Bearer.
  * StackAdapt returns auth/permission/schema/throttle problems as HTTP 200
    with a top-level `errors` body (data:null) — parse the body, never trust
    the status. On a throttle error the body carries
    extensions.cost.throttle.retryAfterInSeconds; honour it.
  * Per-run circuit breaker (stop after N consecutive failures), like the
    Phase 19 delivery-estimate breaker.
  * Log-and-continue throughout; ``run_sync`` NEVER raises and no-ops (returns
    a status dict) when settings.stackadapt_api_key is unset.

StackAdapt reports dedup reach only in FIXED CALENDAR buckets: `period` must be
one of {1,3,7,14,30} and is a bucket SIZE, not a rolling window — startTime/
endTime only select which calendar buckets return, and a sub-range request
returns the whole overlapping bucket. We pull three native grains: daily
(period=1), weekly (period=7), monthly (period=30). Reach is non-additive
across buckets, so the read path never sums them (see the design doc).

Field mapping (API node → table column):
  uniqueImpressions            → reach_individual
  frequency                    → frequency_individual
  impressions                  → impressions
  periodResidentialUniqueImp   → reach_household      (0 before 2026-06-03)
  periodResidentialFrequency   → frequency_household  (0 before 2026-06-03)
  periodResidentialImp         → impressions_household
  campaign.id / campaign.name  → campaign_id / campaign_name
  channel                      → channel
  periodStart / periodEnd      → period_start / period_end (DATE)
"""

import logging
import time
from collections.abc import Iterator
from datetime import date, datetime, timedelta, timezone

import httpx

from backend.config import settings
from backend.services import bigquery_client as bq

logger = logging.getLogger(__name__)

STACKADAPT_GRAPHQL_URL = "https://api.stackadapt.com/graphql"
STACKADAPT_PLATFORM_ID = "stackadapt"  # fact_digital_daily's platform_id

HTTP_TIMEOUT = 60.0
# Paging guard — a bad cursor must not loop forever inside the daily job.
MAX_PAGES = 50
# Batch this many campaign ids per reachFrequency call to keep pages sane.
CAMPAIGN_CHUNK = 100
# Native calendar grains we store: daily / weekly / monthly.
PERIOD_DAYS = (1, 7, 30)
# Per-run circuit breaker: stop hitting the API after this many consecutive
# failures (mirrors the Phase 19 delivery-estimate breaker).
FAILURE_BREAKER_THRESHOLD = 5
# Retries per request when StackAdapt asks us to wait (throttle). Non-throttle
# errors don't retry — they count against the breaker and move on.
MAX_ATTEMPTS = 4
# Fallback sleep when a throttle error carries no explicit retry hint.
DEFAULT_THROTTLE_SLEEP = 2.0
# How far back each grain looks. period only selects buckets, so a generous
# lower bound just guarantees the current + recent buckets come back.
DAILY_LOOKBACK_DAYS = 35
WEEKLY_LOOKBACK_DAYS = 35

# `$cursor` is declared explicitly (the design snippet omitted it). period is a
# fixed calendar bucket size; startTime/endTime only select which buckets.
_RF_QUERY = """
query RF($f: ReachFrequencyFilters!, $cursor: String) {
  reachFrequency(filterBy: $f, first: 100, after: $cursor) {
    totalCount
    pageInfo { hasNextPage endCursor }
    nodes {
      campaign { id name }
      channel
      periodStart periodEnd
      impressions uniqueImpressions frequency
      periodResidentialImp periodResidentialUniqueImp periodResidentialFrequency
    }
  }
}
"""


def _http() -> httpx.Client:
    return httpx.Client(timeout=HTTP_TIMEOUT)


def _chunked(items: list, size: int) -> Iterator[list]:
    for i in range(0, len(items), size):
        yield items[i:i + size]


# ── circuit breaker ────────────────────────────────────────────────────


class _CircuitBreaker:
    """Trip after ``threshold`` consecutive failures. A success resets the
    counter — the breaker only guards against a persistently broken source
    (expired token, schema drift) hammering the API for a whole run."""

    def __init__(self, threshold: int = FAILURE_BREAKER_THRESHOLD):
        self.threshold = threshold
        self.consecutive = 0
        self.tripped = False

    def record_success(self) -> None:
        self.consecutive = 0

    def record_failure(self) -> None:
        self.consecutive += 1
        if self.consecutive >= self.threshold:
            self.tripped = True
            logger.warning(
                "StackAdapt R&F: %d consecutive failures — tripping the "
                "circuit breaker for the rest of this run", self.threshold,
            )


# ── API error handling ─────────────────────────────────────────────────


def _throttle_retry_after(errors: list) -> float | None:
    """Dig extensions.cost.throttle.retryAfterInSeconds out of an HTTP-200
    `errors[]` envelope. Returns the wait in seconds, or None when this is
    not a throttle error (so the caller doesn't retry a hard failure)."""
    for err in errors or []:
        if not isinstance(err, dict):
            continue
        cost = ((err.get("extensions") or {}).get("cost") or {})
        throttle = cost.get("throttle") or {}
        retry_after = throttle.get("retryAfterInSeconds")
        if retry_after is not None:
            try:
                return float(retry_after)
            except (TypeError, ValueError):
                return DEFAULT_THROTTLE_SLEEP
    return None


def _post_rf(
    http: httpx.Client, variables: dict, breaker: _CircuitBreaker,
) -> dict | None:
    """POST one reachFrequency page. Returns the `reachFrequency` connection
    dict, or None on failure (breaker recorded). Honours a throttle
    retryAfterInSeconds from the HTTP-200 `errors[]` body; a non-throttle
    error is a hard failure — logged, counted, no retry."""
    headers = {
        "Authorization": f"Bearer {settings.stackadapt_api_key}",
        "Content-Type": "application/json",
    }
    for attempt in range(MAX_ATTEMPTS):
        try:
            resp = http.post(
                STACKADAPT_GRAPHQL_URL,
                json={"query": _RF_QUERY, "variables": variables},
                headers=headers,
            )
            resp.raise_for_status()
            payload = resp.json()
        except Exception:
            logger.warning("StackAdapt R&F request failed", exc_info=True)
            breaker.record_failure()
            return None

        # Errors arrive as HTTP 200 + an `errors` body with data:null. Parse
        # the body, not the status. A throttle error tells us how long to wait.
        errors = payload.get("errors")
        if errors:
            retry_after = _throttle_retry_after(errors)
            if retry_after is not None and attempt < MAX_ATTEMPTS - 1:
                logger.info(
                    "StackAdapt R&F throttled — waiting %.1fs (attempt %d)",
                    retry_after, attempt + 1,
                )
                time.sleep(retry_after)
                continue
            logger.warning("StackAdapt R&F GraphQL error: %s", errors)
            breaker.record_failure()
            return None

        breaker.record_success()
        return ((payload.get("data") or {}).get("reachFrequency")) or {}

    # Exhausted throttle retries.
    breaker.record_failure()
    return None


# ── row mapping ────────────────────────────────────────────────────────


def _as_date(value) -> str | None:
    """Normalise an ISO8601 date/datetime string to a 'YYYY-MM-DD' string
    the BigQuery DATE loader accepts. None passes through."""
    if not value:
        return None
    return str(value)[:10]


def _node_to_row(node: dict, period_days: int, fetched_at: str) -> dict | None:
    """Map one reachFrequency node to a stackadapt_reach_frequency row.

    Returns None when the node lacks the primary-grain keys (campaign_id,
    period_start) — those can't be MERGEd and are dropped rather than
    poisoning the load."""
    campaign = node.get("campaign") or {}
    campaign_id = campaign.get("id")
    period_start = _as_date(node.get("periodStart"))
    period_end = _as_date(node.get("periodEnd"))
    if not campaign_id or not period_start:
        return None
    return {
        "campaign_id": str(campaign_id),
        "campaign_name": campaign.get("name"),
        "channel": node.get("channel"),
        "period_days": period_days,
        "period_start": period_start,
        # period_end is NOT NULL in the table; fall back to the start bucket.
        "period_end": period_end or period_start,
        "reach_individual": node.get("uniqueImpressions"),
        "frequency_individual": node.get("frequency"),
        "reach_household": node.get("periodResidentialUniqueImp"),
        "frequency_household": node.get("periodResidentialFrequency"),
        "impressions": node.get("impressions"),
        "impressions_household": node.get("periodResidentialImp"),
        "fetched_at": fetched_at,
    }


# ── BigQuery state ─────────────────────────────────────────────────────


def _tracked_campaign_ids() -> list[str]:
    """Distinct StackAdapt campaign ids we actually track — everything that
    delivered in the last 35 days. fact_digital_daily.campaign_id for SA rows
    IS the StackAdapt campaign.id the API keys on."""
    rows = bq.run_query(
        f"""
        SELECT DISTINCT campaign_id
        FROM {bq.table('fact_digital_daily')}
        WHERE platform_id = @platform_id
          AND campaign_id IS NOT NULL AND campaign_id != ''
          AND date >= DATE_SUB(CURRENT_DATE(), INTERVAL 35 DAY)
        """,
        [bq.string_param("platform_id", STACKADAPT_PLATFORM_ID)],
    )
    return [str(r["campaign_id"]) for r in rows if r.get("campaign_id")]


# ── grain windows ──────────────────────────────────────────────────────


def _iso_dt(d: date) -> str:
    """Midnight-UTC ISO8601 datetime for a calendar date."""
    return f"{d.isoformat()}T00:00:00Z"


def _grain_window(period_days: int, today: date) -> tuple[str, str]:
    """(startTime, endTime) selecting which calendar buckets return for one
    grain. period only picks buckets, so the lower bound just has to reach far
    enough back to include the current + recent buckets:
      * daily   → last ~35 days
      * weekly  → last ~5 weeks
      * monthly → current + previous calendar month
    """
    end = _iso_dt(today)
    if period_days == 1:
        start = today - timedelta(days=DAILY_LOOKBACK_DAYS)
    elif period_days == 7:
        start = today - timedelta(days=WEEKLY_LOOKBACK_DAYS)
    else:  # monthly: from the first of last month
        first_of_this = today.replace(day=1)
        first_of_prev = (first_of_this - timedelta(days=1)).replace(day=1)
        start = first_of_prev
    return _iso_dt(start), end


# ── fetch ──────────────────────────────────────────────────────────────


def _fetch_grain(
    http: httpx.Client,
    campaign_ids: list[str],
    period_days: int,
    today: date,
    fetched_at: str,
    breaker: _CircuitBreaker,
) -> list[dict]:
    """Fetch every reach/frequency row for one grain across all campaigns,
    batched and paged. Returns mapped rows. Stops early when the breaker
    trips; a page failure ends that batch but the run continues."""
    start_time, end_time = _grain_window(period_days, today)
    rows: list[dict] = []
    for batch in _chunked(campaign_ids, CAMPAIGN_CHUNK):
        if breaker.tripped:
            break
        cursor: str | None = None
        for _ in range(MAX_PAGES):
            if breaker.tripped:
                break
            variables = {
                "f": {
                    "campaignIds": batch,
                    "startTime": start_time,
                    "endTime": end_time,
                    "period": period_days,
                },
                "cursor": cursor,
            }
            conn = _post_rf(http, variables, breaker)
            if conn is None:
                break  # this batch failed; move to the next
            for node in conn.get("nodes") or []:
                row = _node_to_row(node or {}, period_days, fetched_at)
                if row is not None:
                    rows.append(row)
            info = conn.get("pageInfo") or {}
            cursor = info.get("endCursor")
            if not info.get("hasNextPage") or not cursor:
                break
    return rows


# ── upsert (load-then-MERGE, cross-region-safe) ────────────────────────


# All metric columns updated on MERGE; the primary grain
# (campaign_id, period_days, period_start) is the match key.
_MERGE_UPDATE_COLUMNS = (
    "campaign_name",
    "channel",
    "period_end",
    "reach_individual",
    "frequency_individual",
    "reach_household",
    "frequency_household",
    "impressions",
    "impressions_household",
    "fetched_at",
)
_ALL_COLUMNS = ("campaign_id", "period_days", "period_start") + _MERGE_UPDATE_COLUMNS


def _upsert_rows(rows: list[dict]) -> int:
    """Load returned rows into a staging table via load_table_from_json, then
    MERGE into stackadapt_reach_frequency on (campaign_id, period_days,
    period_start). Both tables share the northamerica-northeast1 region, so the
    MERGE is in-region — never a cross-region INSERT...SELECT (CLAUDE.md §4.1).
    Returns the number of rows staged."""
    if not rows:
        return 0

    from google.cloud import bigquery

    client = bq.get_client()
    target = settings.stackadapt_rf_table
    staging = f"{target}_staging"

    schema = [
        bigquery.SchemaField("campaign_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("campaign_name", "STRING"),
        bigquery.SchemaField("channel", "STRING"),
        bigquery.SchemaField("period_days", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("period_start", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("period_end", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("reach_individual", "INT64"),
        bigquery.SchemaField("frequency_individual", "FLOAT64"),
        bigquery.SchemaField("reach_household", "INT64"),
        bigquery.SchemaField("frequency_household", "FLOAT64"),
        bigquery.SchemaField("impressions", "INT64"),
        bigquery.SchemaField("impressions_household", "INT64"),
        bigquery.SchemaField("fetched_at", "TIMESTAMP", mode="REQUIRED"),
    ]
    load_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    )
    client.load_table_from_json(
        rows, staging, job_config=load_config,
    ).result()

    set_clause = ",\n            ".join(
        f"{col} = s.{col}" for col in _MERGE_UPDATE_COLUMNS
    )
    insert_cols = ", ".join(_ALL_COLUMNS)
    insert_vals = ", ".join(f"s.{col}" for col in _ALL_COLUMNS)
    client.query(
        f"""
        MERGE `{target}` t
        USING `{staging}` s
          ON t.campaign_id = s.campaign_id
         AND t.period_days = s.period_days
         AND t.period_start = s.period_start
        WHEN MATCHED THEN UPDATE SET
            {set_clause}
        WHEN NOT MATCHED THEN INSERT ({insert_cols})
            VALUES ({insert_vals})
        """
    ).result()
    return len(rows)


# ── orchestration ──────────────────────────────────────────────────────


def run_sync() -> dict:
    """Pull StackAdapt reach/frequency for every tracked SA campaign across the
    three native calendar grains (daily/weekly/monthly) and MERGE-upsert them.

    Best-effort by design: NEVER raises, no-ops (returns a status dict) when
    settings.stackadapt_api_key is unset, and a persistently broken source
    trips a circuit breaker instead of hammering the API for the whole run.
    """
    if not settings.stackadapt_api_key:
        logger.info("StackAdapt R&F sync skipped — STACKADAPT_API_KEY not set")
        return {
            "status": "skipped", "reason": "no_key",
            "campaigns": 0, "rows_upserted": 0, "grains": {},
        }

    try:
        campaign_ids = _tracked_campaign_ids()
    except Exception:
        logger.warning("StackAdapt R&F sync: campaign enumeration failed", exc_info=True)
        return {"status": "error", "reason": "campaign_read_failed",
                "campaigns": 0, "rows_upserted": 0, "grains": {}}

    if not campaign_ids:
        logger.info("StackAdapt R&F sync: no tracked StackAdapt campaigns")
        return {"status": "success", "campaigns": 0, "rows_upserted": 0, "grains": {}}

    today = datetime.now(timezone.utc).date()
    fetched_at = datetime.now(timezone.utc).isoformat()
    breaker = _CircuitBreaker()
    grains: dict[str, int] = {}
    all_rows: list[dict] = []

    try:
        with _http() as http:
            for period_days in PERIOD_DAYS:
                if breaker.tripped:
                    logger.warning(
                        "StackAdapt R&F: breaker tripped — skipping period=%d",
                        period_days,
                    )
                    grains[str(period_days)] = 0
                    continue
                rows = _fetch_grain(
                    http, campaign_ids, period_days, today, fetched_at, breaker,
                )
                grains[str(period_days)] = len(rows)
                all_rows.extend(rows)
    except Exception:
        logger.warning("StackAdapt R&F sync: fetch loop crashed", exc_info=True)
        # Fall through — still try to persist whatever we gathered.

    upserted = 0
    try:
        upserted = _upsert_rows(all_rows)
    except Exception:
        logger.warning("StackAdapt R&F sync: upsert failed", exc_info=True)
        return {
            "status": "error", "reason": "upsert_failed",
            "campaigns": len(campaign_ids), "rows_upserted": 0,
            "grains": grains, "breaker_tripped": breaker.tripped,
        }

    return {
        "status": "success",
        "campaigns": len(campaign_ids),
        "rows_upserted": upserted,
        "grains": grains,
        "breaker_tripped": breaker.tripped,
    }
