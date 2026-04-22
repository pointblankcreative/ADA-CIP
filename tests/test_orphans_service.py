"""Unit tests for the orphan auto-discovery service.

Covers:
    - scan_orphans: UNIONs three fact sources, excludes dim_projects
      and dismissed_orphans unless include_dismissed=True
    - _hydrate_row: coerces BQ types (NUMERIC, DATE, TIMESTAMP, STRUCT[])
      to JSON-serialisable primitives and handles None
    - dismiss: runs a MERGE; returns hydrated orphan when activity exists
    - undismiss: returns False when no row was deleted

The service talks to BigQuery via ``backend.services.bigquery_client``;
tests swap ``bq.run_query`` for a recorder that returns canned rows.
``undismiss`` uses the raw client (for num_dml_affected_rows), so that
path is tested with a stubbed client.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any

import pytest

from backend.services import bigquery_client as bq
from backend.services import orphans as svc


# ── Fake BQ layer ────────────────────────────────────────────────────────────


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

    def find(self, needle: str, n: int = 0) -> tuple[str, dict]:
        matches = [c for c in self.calls if needle in c[0]]
        assert matches, f"No call contained {needle!r}. Calls: {[c[0][:80] for c in self.calls]}"
        return matches[n]


@pytest.fixture
def fake_bq(monkeypatch) -> FakeBQ:
    fb = FakeBQ()
    monkeypatch.setattr(bq, "run_query", fb.run_query)
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
    }


# ── scan_orphans ────────────────────────────────────────────────────────────


def test_scan_orphans_default_excludes_dismissed(fake_bq):
    fake_bq.queue([_orphan_row()])

    rows = svc.scan_orphans()

    assert len(rows) == 1
    assert rows[0]["project_code"] == "23061"
    assert rows[0]["dismissed"] is False
    assert rows[0]["total_spend"] == 192069.10
    assert rows[0]["first_date"] == "2024-03-28"
    assert rows[0]["last_date"] == "2026-03-28"
    assert len(rows[0]["by_platform"]) == 2
    assert rows[0]["by_platform"][0]["platform_id"] == "stackadapt"

    # Default: include_dismissed=False should be the param value
    _, params = fake_bq.calls[0]
    assert params["include_dismissed"] is False


def test_scan_orphans_include_dismissed_true(fake_bq):
    fake_bq.queue([])
    svc.scan_orphans(include_dismissed=True)

    _, params = fake_bq.calls[0]
    assert params["include_dismissed"] is True


def test_scan_orphans_unions_all_three_fact_sources(fake_bq):
    """Sanity: the query references all three fact tables + LEFT JOINs the
    dim_projects exclusion + dismissed_orphans exclusion."""
    fake_bq.queue([])
    svc.scan_orphans()
    sql, _ = fake_bq.calls[0]
    assert "fact_digital_daily" in sql
    assert "fact_dooh_daily" in sql
    assert "fact_adset_daily" in sql
    assert "dim_projects" in sql
    assert "dismissed_orphans" in sql
    # Orphan = missing from dim_projects
    assert "dp.project_code IS NULL" in sql


def test_scan_orphans_hydrates_dismissed_row(fake_bq):
    dismissed_at = datetime(2026, 4, 21, 10, 0, 0, tzinfo=timezone.utc)
    fake_bq.queue([
        _orphan_row(
            dismissed_at=dismissed_at,
            dismissed_by="frazer@pointblank.co",
            dismissed_reason="old test account",
        )
    ])

    rows = svc.scan_orphans(include_dismissed=True)
    assert rows[0]["dismissed"] is True
    assert rows[0]["dismissed_at"] == dismissed_at.isoformat()
    assert rows[0]["dismissed_by"] == "frazer@pointblank.co"
    assert rows[0]["dismissed_reason"] == "old test account"


def test_scan_orphans_handles_null_dates_and_spend(fake_bq):
    fake_bq.queue([_orphan_row(
        total_spend=None,
        total_rows=None,
        first_date=None,
        last_date=None,
        by_platform=[],
    )])
    rows = svc.scan_orphans()
    assert rows[0]["total_spend"] == 0.0
    assert rows[0]["total_rows"] == 0
    assert rows[0]["first_date"] is None
    assert rows[0]["last_date"] is None
    assert rows[0]["by_platform"] == []


def test_scan_orphans_handles_null_spend_in_platform_breakdown(fake_bq):
    """fact_adset_daily contributes row_count but spend may arrive as None."""
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


# ── dismiss ─────────────────────────────────────────────────────────────────


def test_dismiss_runs_merge(fake_bq):
    # First call: MERGE (returns []). Second call: get_orphan -> scan_orphans.
    fake_bq.queue([])  # MERGE result (write — actually always [])
    fake_bq.queue([_orphan_row(
        project_code="23061",
        dismissed_at=datetime(2026, 4, 21, 10, 0, 0, tzinfo=timezone.utc),
        dismissed_by="frazer@pointblank.co",
        dismissed_reason="old test account",
    )])

    result = svc.dismiss(
        project_code="23061",
        dismissed_by="frazer@pointblank.co",
        reason="old test account",
    )

    # MERGE should have been the first call
    merge_sql, merge_params = fake_bq.calls[0]
    assert "MERGE" in merge_sql
    assert "dismissed_orphans" in merge_sql
    assert merge_params["project_code"] == "23061"
    assert merge_params["dismissed_by"] == "frazer@pointblank.co"
    assert merge_params["reason"] == "old test account"

    assert result["dismissed"] is True
    assert result["project_code"] == "23061"


def test_dismiss_returns_minimal_record_when_activity_gone(fake_bq):
    """If orphan activity has been purged but we still dismiss it, the
    return value should carry the dismissal metadata we just wrote."""
    fake_bq.queue([])  # MERGE
    fake_bq.queue([])  # scan_orphans finds nothing

    result = svc.dismiss(
        project_code="99999",
        dismissed_by="frazer@pointblank.co",
        reason="bogus",
    )

    assert result["project_code"] == "99999"
    assert result["dismissed"] is True
    assert result["dismissed_by"] == "frazer@pointblank.co"
    assert result["dismissed_reason"] == "bogus"
    assert result["total_spend"] == 0.0


def test_dismiss_coerces_none_params_to_empty_string(fake_bq):
    """BigQuery STRING params require a value; we pass '' for unknown user."""
    fake_bq.queue([])
    fake_bq.queue([])
    svc.dismiss(project_code="23061", dismissed_by=None, reason=None)

    _, merge_params = fake_bq.calls[0]
    assert merge_params["dismissed_by"] == ""
    assert merge_params["reason"] == ""


# ── undismiss ───────────────────────────────────────────────────────────────


class _FakeQueryJob:
    def __init__(self, affected: int):
        self.num_dml_affected_rows = affected

    def result(self):
        return self


class _FakeClient:
    """Minimal stand-in for the real BQ client used by undismiss."""

    def __init__(self, affected: int):
        self._affected = affected
        self.last_sql: str | None = None
        self.last_params: list = []

    def query(self, sql, job_config=None):
        self.last_sql = sql
        self.last_params = list(job_config.query_parameters) if job_config else []
        return _FakeQueryJob(self._affected)


def test_undismiss_returns_true_when_row_removed(monkeypatch):
    fake_client = _FakeClient(affected=1)
    monkeypatch.setattr(bq, "get_client", lambda: fake_client)

    assert svc.undismiss("23061") is True
    assert "DELETE FROM" in fake_client.last_sql
    assert "dismissed_orphans" in fake_client.last_sql
    assert fake_client.last_params[0].value == "23061"


def test_undismiss_returns_false_when_no_rows_affected(monkeypatch):
    fake_client = _FakeClient(affected=0)
    monkeypatch.setattr(bq, "get_client", lambda: fake_client)
    assert svc.undismiss("99999") is False


def test_undismiss_treats_none_affected_as_zero(monkeypatch):
    fake_client = _FakeClient(affected=None)
    monkeypatch.setattr(bq, "get_client", lambda: fake_client)
    assert svc.undismiss("99999") is False
