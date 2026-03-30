import logging
from datetime import date

from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPIError

from backend.config import settings

logger = logging.getLogger(__name__)

_client: bigquery.Client | None = None


def get_client() -> bigquery.Client:
    global _client
    if _client is None:
        _client = bigquery.Client(
            project=settings.gcp_project_id,
            location=settings.gcp_region,
        )
        logger.info("BigQuery client initialised (project=%s, location=%s)",
                     settings.gcp_project_id, settings.gcp_region)
    return _client


def close_client() -> None:
    global _client
    if _client is not None:
        _client.close()
        _client = None
        logger.info("BigQuery client closed")


def table(name: str) -> str:
    """Fully-qualified table: `project.dataset.table`."""
    return f"`{settings.gcp_project_id}.{settings.bigquery_dataset}.{name}`"


def run_query(
    sql: str,
    params: list[bigquery.ScalarQueryParameter] | None = None,
) -> list[dict]:
    """Execute a SQL query and return rows as dicts."""
    client = get_client()
    job_config = bigquery.QueryJobConfig()
    if params:
        job_config.query_parameters = params
    try:
        rows = client.query(sql, job_config=job_config).result()
        return [dict(row) for row in rows]
    except GoogleAPIError:
        logger.exception("BigQuery query failed")
        raise


def scalar_param(name: str, type_: str, value) -> bigquery.ScalarQueryParameter:
    """Convenience wrapper for creating a ScalarQueryParameter."""
    return bigquery.ScalarQueryParameter(name, type_, value)


def string_param(name: str, value: str) -> bigquery.ScalarQueryParameter:
    return bigquery.ScalarQueryParameter(name, "STRING", value)


def date_param(name: str, value: date) -> bigquery.ScalarQueryParameter:
    return bigquery.ScalarQueryParameter(name, "DATE", value)


def ping() -> bool:
    """Lightweight connectivity check against BigQuery."""
    try:
        run_query("SELECT 1")
        return True
    except Exception:
        logger.exception("BigQuery ping failed")
        return False
