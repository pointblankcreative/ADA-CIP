"""Tests for project endpoints' pacing_percentage rollup (AI-001).

The projects router computes `pacing_percentage` in BigQuery from the
`budget_tracking` LEFT JOIN (latest snapshot per project, excluding
pending/not_started lines from both numerator and denominator — matches the
conservative-estimate ethos already in routers/pacing.py).

These tests stub bq.run_query so the SQL execution itself is not exercised;
they verify (a) the router wires `pacing_percentage` from the row dict into
the response model, (b) null is preserved when the budget_tracking JOIN
returns no rows, (c) pending lines are excluded from the rollup (enforced
inside the SQL — we assert by inspecting the emitted SQL), and (d) list and
detail endpoints both populate the field.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.routers import projects as projects_router


# ── Helpers ──────────────────────────────────────────────────────────


def _make_app() -> FastAPI:
    app = FastAPI()
    app.include_router(projects_router.router)
    return app


class QueryRecorder:
    """Stub for bq.run_query that records every call and returns canned rows."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, list]] = []
        self.responses: list[list[dict]] = []

    def __call__(self, sql: str, params=None):
        self.calls.append((sql, list(params or [])))
        if self.responses:
            return self.responses.pop(0)
        return []


def _string_param(name, value):
    return ("string", name, value)


def _table(name):
    return f"`dummy.{name}`"


def _detail_row(**overrides):
    """A minimal dim_projects row joined with the budget_tracking rollup,
    shaped the way the get_project SQL would return it."""
    base = {
        "project_code": "26018",
        "project_name": "Test Project",
        "client_id": "client-1",
        "client_name": "Test Client",
        "campaign_type": "mixed",
        "status": "active",
        "start_date": None,
        "end_date": None,
        "net_budget": 100000.0,
        "currency": "CAD",
        "media_plan_sheet_id": None,
        "slack_channel_id": None,
        "total_spend": 25000.0,
        "pacing_percentage": 59.7,
        "days_remaining": 30,
        "platforms_active": 3,
        "first_data_date": None,
        "last_data_date": None,
        "created_at": None,
        "updated_at": None,
    }
    base.update(overrides)
    return base


def _summary_row(**overrides):
    """A minimal list_projects row."""
    base = {
        "project_code": "26018",
        "project_name": "Test Project",
        "client_name": "Test Client",
        "status": "active",
        "start_date": None,
        "end_date": None,
        "net_budget": 100000.0,
        "total_spend": 25000.0,
        "pacing_percentage": 59.7,
        "days_remaining": 30,
        "recently_ended": False,
        "updated_at": None,
    }
    base.update(overrides)
    return base


# ── get_project ──────────────────────────────────────────────────────


def test_get_project_includes_pacing_percentage_when_budget_tracking_present():
    """Happy path: the budget_tracking JOIN returns a row with a non-null
    pacing_percentage; the router must surface it on the ProjectDetail."""
    rec = QueryRecorder()
    rec.responses = [[_detail_row(pacing_percentage=59.7)]]

    with patch.object(projects_router.bq, "run_query", side_effect=rec), \
         patch.object(projects_router.bq, "string_param", _string_param), \
         patch.object(projects_router.bq, "table", _table):
        client = TestClient(_make_app())
        resp = client.get("/api/projects/26018")

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["pacing_percentage"] == 59.7
    assert body["status"] == "active"
    assert body["total_spend"] == 25000.0


def test_get_project_returns_null_pacing_when_no_budget_tracking_rows():
    """When the budget_tracking LEFT JOIN produces no row (newly-created
    project, daily pipeline hasn't run yet, etc), the SELECT returns NULL
    for pacing_percentage. The frontend then renders "No Data" instead of
    the misleading "Pending" badge — the whole point of AI-001."""
    rec = QueryRecorder()
    rec.responses = [[_detail_row(pacing_percentage=None, total_spend=0.0)]]

    with patch.object(projects_router.bq, "run_query", side_effect=rec), \
         patch.object(projects_router.bq, "string_param", _string_param), \
         patch.object(projects_router.bq, "table", _table):
        client = TestClient(_make_app())
        resp = client.get("/api/projects/26099")

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["pacing_percentage"] is None


def test_get_project_excludes_pending_lines_from_rollup():
    """Pending/not_started lines must be filtered out of BOTH numerator and
    denominator. We can't run the SQL here, but we can assert that the
    rollup CTE excludes them via `line_status NOT IN ('pending','not_started')`
    on BOTH the actual_spend_to_date and planned_spend_to_date sums.

    This mirrors the conservative-estimate pattern in routers/pacing.py
    (see comment around `# C2: Conservative approach — exclude pending lines`).
    """
    rec = QueryRecorder()
    rec.responses = [[_detail_row()]]

    with patch.object(projects_router.bq, "run_query", side_effect=rec), \
         patch.object(projects_router.bq, "string_param", _string_param), \
         patch.object(projects_router.bq, "table", _table):
        client = TestClient(_make_app())
        resp = client.get("/api/projects/26018")

    assert resp.status_code == 200
    assert len(rec.calls) == 1
    sql = rec.calls[0][0]
    assert "budget_tracking" in sql, (
        "get_project SQL must JOIN budget_tracking for pacing_percentage"
    )
    # Both sums must guard with the pending-exclusion IF()
    pending_guard = (
        "line_status NOT IN ('pending','not_started')"
    )
    occurrences = sql.count(pending_guard)
    assert occurrences >= 2, (
        f"Expected pending exclusion on BOTH actual + planned sums, "
        f"found {occurrences} occurrences in:\n{sql}"
    )
    # The CASE on pacing_percentage must compute the ratio from these two.
    assert "SAFE_DIVIDE" in sql
    assert "bt_planned" in sql
    assert "bt_actual" in sql


# ── list_projects ────────────────────────────────────────────────────


def test_list_projects_populates_pacing_percentage_for_each_row():
    """List endpoint must populate pacing_percentage on every ProjectSummary,
    not just the detail view. The home page card grid is the second-biggest
    surface affected by AI-001."""
    rec = QueryRecorder()
    rec.responses = [[
        _summary_row(project_code="26018", pacing_percentage=59.7),
        _summary_row(project_code="26009", pacing_percentage=105.2),
        # Project that has no budget_tracking rows yet → null percentage.
        _summary_row(project_code="26099", pacing_percentage=None,
                     total_spend=0.0),
    ]]

    with patch.object(projects_router.bq, "run_query", side_effect=rec), \
         patch.object(projects_router.bq, "string_param", _string_param), \
         patch.object(projects_router.bq, "table", _table):
        client = TestClient(_make_app())
        resp = client.get("/api/projects/")

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert len(body) == 3
    by_code = {row["project_code"]: row for row in body}
    assert by_code["26018"]["pacing_percentage"] == 59.7
    assert by_code["26009"]["pacing_percentage"] == 105.2
    assert by_code["26099"]["pacing_percentage"] is None

    # And the SQL must include the budget_tracking rollup join.
    assert len(rec.calls) == 1
    sql = rec.calls[0][0]
    assert "budget_tracking" in sql
    assert "SAFE_DIVIDE" in sql
