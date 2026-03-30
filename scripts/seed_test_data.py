"""Seed dim_projects and dim_clients with test data from known funnel campaigns.

Usage:
    python -m scripts.seed_test_data
    python -m scripts.seed_test_data --clear   # wipe and re-seed
"""

import argparse
import logging
from datetime import datetime, timezone

from google.cloud import bigquery

from backend.config import settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
logger = logging.getLogger(__name__)

PROJECTS = [
    {
        "project_code": "25013",
        "project_name": "BCGEU Bargaining Escalation",
        "client_id": "client-bcgeu",
        "status": "active",
        "start_date": "2025-05-01",
        "end_date": "2026-03-31",
        "net_budget": 375000.0,
    },
    {
        "project_code": "26009",
        "project_name": "CUPE OMERS",
        "client_id": "client-cupe",
        "status": "active",
        "start_date": "2026-03-05",
        "end_date": "2026-03-24",
        "net_budget": 85000.0,
    },
    {
        "project_code": "25001",
        "project_name": "BCGEU General Membership",
        "client_id": "client-bcgeu",
        "status": "active",
        "start_date": "2024-04-01",
        "end_date": "2026-06-30",
        "net_budget": 120000.0,
    },
    {
        "project_code": "25022",
        "project_name": "CUPE National Solidarity",
        "client_id": "client-cupe",
        "status": "active",
        "start_date": "2024-04-01",
        "end_date": "2026-06-30",
        "net_budget": 115000.0,
    },
    {
        "project_code": "24058",
        "project_name": "OPSEU Hospital Workers",
        "client_id": "client-opseu",
        "status": "active",
        "start_date": "2025-03-27",
        "end_date": "2026-06-30",
        "net_budget": 120000.0,
    },
    {
        "project_code": "25048",
        "project_name": "CUPW National Campaign",
        "client_id": "client-cupw",
        "status": "active",
        "start_date": "2025-10-25",
        "end_date": "2026-06-30",
        "net_budget": 95000.0,
    },
    {
        "project_code": "25037",
        "project_name": "OSSTF Provincial Bargaining",
        "client_id": "client-osstf",
        "status": "active",
        "start_date": "2026-03-01",
        "end_date": "2026-06-30",
        "net_budget": 50000.0,
    },
    {
        "project_code": "25055",
        "project_name": "NUPGE National Awareness",
        "client_id": "client-nupge",
        "status": "active",
        "start_date": "2024-04-01",
        "end_date": "2026-06-30",
        "net_budget": 45000.0,
    },
]

CLIENTS = [
    {"client_id": "client-bcgeu", "client_name": "BCGEU", "client_short_name": "BCGEU"},
    {"client_id": "client-cupe", "client_name": "CUPE", "client_short_name": "CUPE"},
    {"client_id": "client-opseu", "client_name": "OPSEU", "client_short_name": "OPSEU"},
    {"client_id": "client-cupw", "client_name": "CUPW", "client_short_name": "CUPW"},
    {"client_id": "client-osstf", "client_name": "OSSTF", "client_short_name": "OSSTF"},
    {"client_id": "client-nupge", "client_name": "NUPGE", "client_short_name": "NUPGE"},
]


def _table(name: str) -> str:
    return f"{settings.gcp_project_id}.{settings.bigquery_dataset}.{name}"


def seed(clear: bool = False) -> dict:
    mtl = bigquery.Client(project=settings.gcp_project_id, location=settings.gcp_region)
    try:
        if clear:
            logger.info("Clearing existing dim_projects and dim_clients...")
            mtl.query(f"DELETE FROM `{_table('dim_projects')}` WHERE TRUE").result()
            mtl.query(f"DELETE FROM `{_table('dim_clients')}` WHERE TRUE").result()

        now = datetime.now(timezone.utc).isoformat()
        cfg = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )

        # Seed clients
        client_records = [{**c, "created_at": now} for c in CLIENTS]
        mtl.load_table_from_json(client_records, _table("dim_clients"), job_config=cfg).result()
        logger.info("Seeded %d clients", len(client_records))

        # Seed projects — upsert by deleting existing codes first
        codes = [p["project_code"] for p in PROJECTS]
        codes_str = ", ".join(f"'{c}'" for c in codes)
        mtl.query(f"DELETE FROM `{_table('dim_projects')}` WHERE project_code IN ({codes_str})").result()

        project_records = [{**p, "updated_at": now} for p in PROJECTS]
        mtl.load_table_from_json(project_records, _table("dim_projects"), job_config=cfg).result()
        logger.info("Seeded %d projects", len(project_records))

        return {
            "status": "success",
            "clients_seeded": len(client_records),
            "projects_seeded": len(project_records),
            "project_codes": codes,
        }
    finally:
        mtl.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Seed test data into CIP")
    parser.add_argument("--clear", action="store_true", help="Clear existing data before seeding")
    args = parser.parse_args()
    result = seed(clear=args.clear)
    print(result)
