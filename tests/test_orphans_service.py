"""Unit tests for the orphan auto-discovery service.

Covers:
    - scan_orphans: UNIONs three fact sources, excludes dim_projects, and
      applies the dismissed_orphans suppression levels (dismissed / archived)
    - _hydrate_row: coerces BQ types (NUMERIC, DATE, TIMESTAMP, STRUCT[])
      to JSON-serialisable primitives and handles None
    - get_orphan: finds by code across the include-dismissed scan

Suppression is control-table-only now — there is no dismiss/undismiss write
path in the service. The service talks to BigQuery via
``backend.services.bigquery_client``; tests swap ``bq.run_query`` for a recorder
and skip the schema migration.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any

import pytest

from backend.services import bigquery_client as bq
from backend.services import orphans as svc


class FakeBQ:
    """Records every run_query call and returns canned responses."""

    def __init__(self):
        self.calls: list[tuple[str, dict]] = []
        self._queue: list[list[dict]] = []

    def queue(self, rows: list[dict]) -> None:
        self._queue.append(rows)

    def run_query(self, sql: str, params: list | None = None) -> list[dict]:
        param_dict: dict[str, Any] = {}
        for p in (params or []):
            val = getattr(p, "value", None)
            if val is None:
                val = getattr(p, "values", None)
            param_dict[p.name] = val
        self.calls.append((sql, param_dict))
        stripped = sql.lstrip()
        is_read = stripped.upper().startswith("SELECT") or stripped.upper().startswith("WITH")
        if is_read and self._queue:
            return self._queue.pop(0)
        return []


@pytest.fixture
def fake_bq(monkeypatch) -> FakeBQ:
    fb = FakeBQ()
    monkeypatch.setattr(bq, "run_query", fb.run_query)
    # Skip the one-time ALTER TABLE migration (would hit a real BQ client).
    monkeypatch.setattr(svc, "_SCHEMA_ENSURED", True)
    return fb


def _orphan_row(
    project_code: str = "23061",
    total_spend: float = 192069.10,
    total_rows: int = 10627,
    first_date=date(2024, 3, 28),
    last_date=date(2026, 3, 28),
    by_platform=None,
    dismissed_at=None,
    dismissed_by=None,
    dismissed_reason=None,
    level=None,
) -> dict:
    """Mimic a row as returned by the scanner SQL."""
    if by_platform is None:
        by_platform = [
            {"platform_id": "stackadapt", "spend": 89050.44, "row_count": 1737},
            {"platform_id": "meta", "spend": 72924.24, "row_count": 7264},
        ]
    return {
        "project_code": project_code,
        "total_spend": total_spend,
        "total_rows": total_rows,
        "first_date": first_date,
        "last_date": last_date,
        "by_platform": by_platform,
        "dismissed_at": dismissed_at,
        "dismissed_by": dismissed_by,
        "dismissed_reason": dismissed_reason,
        "level": level,
    }


# ── scan_orphans ────────────────────────────────────────────────────────────


def test_scan_orphans_default_excludes_suppressed(fake_bq):
    fake_bq.queue([_orphan_row()])

    rows = svc.scan_orphans()

    assert len(rows) == 1
    assert rows[0]["project_code"] == "23061"
    assert rows[0]["dismissed"] is False
    assert rows[0]["total_spend"] == 192069.10
    assert rows[0]["first_date"] == "2024-03-28"
    assert len(rows[0]["by_platform"]) == 2

    _, params = fake_bq.calls[0]
    assert params["include_dismissed"] is False


def test_scan_orphans_include_dismissed_true(fake_bq):
    fake_bq.queue([])
    svc.scan_orphans(include_dismissed=True)
    _, params = fake_bq.calls[0]
    assert params["include_dismissed"] is True


def test_scan_orphans_unions_all_three_fact_sources(fake_bq):
    fake_bq.queue([])
    svc.scan_orphans()
    sql, _ = fake_bq.calls[0]
    assert "fact_digital_daily" in sql
    assert "fact_dooh_daily" in sql
    assert "fact_adset_daily" in sql
    assert "dim_projects" in sql
    assert "dismissed_orphans" in sql
    assert "dp.project_code IS NULL" in sql


def test_scan_orphans_sql_applies_level_logic(fake_bq):
    """archived rows stay out of both views; dismissed only show under
    include_dismissed. The WHERE clause must key off the level column."""
    fake_bq.queue([])
    svc.scan_orphans(include_dismissed=True)
    sql, _ = fake_bq.calls[0]
    assert "COALESCE(d.level, 'dismissed') = 'dismissed'" in sql
    assert "@include_dismissed" in sql
    assert "d.level" in sql


def test_scan_orphans_hydrates_dismissed_row(fake_bq):
    dismissed_at = datetime(2026, 4, 21, 10, 0, 0, tzinfo=timezone.utc)
    fake_bq.queue([
        _orphan_row(
            dismissed_at=dismissed_at,
            dismissed_by="frazer@pointblank.co",
            dismissed_reason="old test account",
            level="dismissed",
        )
    ])

    rows = svc.scan_orphans(include_dismissed=True)
    assert rows[0]["dismissed"] is True
    assert rows[0]["dismissed_at"] == dismissed_at.isoformat()
    assert rows[0]["dismissed_by"] == "frazer@pointblank.co"
    assert rows[0]["dismissed_reason"] == "old test account"
    assert rows[0]["level"] == "dismissed"


def test_scan_orphans_handles_null_dates_and_spend(fake_bq):
    fake_bq.queue([_orphan_row(
        total_spend=None, total_rows=None, first_date=None, last_date=None,
        by_platform=[],
    )])
    rows = svc.scan_orphans()
    assert rows[0]["total_spend"] == 0.0
    assert rows[0]["total_rows"] == 0
    assert rows[0]["first_date"] is None
    assert rows[0]["by_platform"] == []
    assert rows[0]["level"] is None


def test_scan_orphans_handles_null_spend_in_platform_breakdown(fake_bq):
    fake_bq.queue([_orphan_row(by_platform=[
        {"platform_id": "meta", "spend": None, "row_count": 196},
    ])])
    rows = svc.scan_orphans()
    assert rows[0]["by_platform"][0]["spend"] == 0.0
    assert rows[0]["by_platform"][0]["row_count"] == 196


# ── get_orphan ──────────────────────────────────────────────────────────────


def test_get_orphan_finds_by_code(fake_bq):
    fake_bq.queue([
        _orphan_row(project_code="23061"),
        _orphan_row(project_code="24022", total_spend=75000.0, by_platform=[]),
    ])
    found = svc.get_orphan("24022")
    assert found is not None
    assert found["project_code"] == "24022"
    assert found["total_spend"] == 75000.0


def test_get_orphan_returns_none_when_missing(fake_bq):
    fake_bq.queue([_orphan_row(project_code="23061")])
    assert svc.get_orphan("99999") is None
