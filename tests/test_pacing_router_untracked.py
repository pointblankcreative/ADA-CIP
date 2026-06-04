"""Tests for the AI-002 "untracked platforms" bucket on GET /api/pacing/{code}.

The pacing engine only ever queries spend for platforms it has media plan
lines for, so spend on an unplanned platform (e.g. a StackAdapt buy missing
from the synced plan — AI-022) silently disappeared from the Pacing tab while
still counting toward the project header's fact_digital_daily total. The
fix adds an explicit per-platform "untracked" bucket to the response:

  - included in the spent/remaining math (conservative — never overstate
    remaining budget): ``total_actual_all_platforms``
  - EXCLUDED from ``overall_pacing_percentage`` (no planned baseline)
  - listed per-platform in ``untracked_platforms`` so the failure mode is
    self-announcing instead of discoverable only by manual cross-checking.

These tests stub bq.run_query with a QueryRecorder (same pattern as
tests/test_projects_router_pacing.py) so SQL execution isn't exercised; the
SQL-shape tests lock in the mandatory dedup-guard pattern instead.
"""

from __future__ import annotations

from unittest.mock import patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.routers import pacing as pacing_router


# ── Helpers ──────────────────────────────────────────────────────────


def _make_app() -> FastAPI:
    app = FastAPI()
    app.include_router(pacing_router.router)
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


def _date_param(name, value):
    return ("date", name, value)


def _array_param(name, type_, values):
    return ("array", name, type_, list(values))


def _table(name):
    return f"`dummy.{name}`"


def _patches(rec):
    return (
        patch.object(pacing_router.bq, "run_query", side_effect=rec),
        patch.object(pacing_router.bq, "string_param", _string_param),
        patch.object(pacing_router.bq, "date_param", _date_param),
        patch.object(pacing_router.bq, "array_param", _array_param),
        patch.object(pacing_router.bq, "table", _table),
    )


def _project_row(net_budget=12750.0):
    return {"project_code": "26018", "net_budget": net_budget}


def _tracking_row(**overrides):
    """One budget_tracking row joined with mpl_dedup metadata, shaped the way
    the get_pacing tracking SQL returns it. Defaults mirror 26018's lines."""
    base = {
        "date": "2026-06-03",
        "line_id": "plan-26018-4f0de153-line-000",
        "line_code": "",
        "platform_id": "meta",
        "channel_category": "Digital",
        "line_status": "active",
        "planned_budget": 2145.0,
        "planned_spend_to_date": 1800.0,
        "actual_spend_to_date": 2784.57,
        "remaining_budget": -639.57,
        "remaining_days": 2,
        "pacing_percentage": 139.1,
        "daily_budget_required": None,
        "is_over_pacing": True,
        "is_under_pacing": False,
        "bundle_id": None,
        "bundle_role": None,
        "audience_name": "Members",
        "flight_start": "2026-05-07",
        "flight_end": "2026-06-05",
        "sheet_id": "sheet-1",
        "phase_label": None,
        "phase_display_order": 1,
    }
    base.update(overrides)
    return base


def _three_tracked_lines():
    return [
        _tracking_row(),
        _tracking_row(
            line_id="plan-26018-4f0de153-line-001",
            planned_budget=3510.0,
            planned_spend_to_date=2950.0,
            actual_spend_to_date=4556.57,
        ),
        _tracking_row(
            line_id="plan-26018-4f0de153-line-002",
            platform_id="google_ads",
            planned_budget=1100.0,
            planned_spend_to_date=1025.0,
            actual_spend_to_date=429.79,
            pacing_percentage=41.9,
            is_over_pacing=False,
            is_under_pacing=True,
        ),
    ]


def _untracked_row(**overrides):
    base = {
        "platform_id": "stackadapt",
        "spend": 3002.72,
        "first_date": "2026-05-13",
        "last_date": "2026-06-02",
    }
    base.update(overrides)
    return base


# ── Response shape ───────────────────────────────────────────────────


def test_untracked_platform_appears_in_response():
    """Spend on a platform with no media plan line surfaces as an explicit
    untracked bucket: counted in total_actual_all_platforms, listed in
    untracked_platforms, NOT blended into overall_pacing_percentage."""
    rec = QueryRecorder()
    rec.responses = [
        [_project_row()],              # dim_projects
        _three_tracked_lines(),        # budget_tracking + mpl_dedup
        [_untracked_row()],            # AI-002 untracked query
        # no bundle-members query: no bundle_ids on the tracked rows
    ]

    p1, p2, p3, p4, p5 = _patches(rec)
    with p1, p2, p3, p4, p5:
        client = TestClient(_make_app())
        resp = client.get("/api/pacing/26018")

    assert resp.status_code == 200, resp.text
    body = resp.json()

    assert body["untracked_spend"] == 3002.72
    assert len(body["untracked_platforms"]) == 1
    assert body["untracked_platforms"][0]["platform_id"] == "stackadapt"
    assert body["untracked_platforms"][0]["spend"] == 3002.72

    tracked_total = 2784.57 + 4556.57 + 429.79
    assert abs(body["total_actual_to_date"] - tracked_total) < 0.01
    assert (
        abs(body["total_actual_all_platforms"] - (tracked_total + 3002.72))
        < 0.01
    )

    # overall_pacing_percentage must come from tracked lines ONLY — the
    # untracked bucket has no planned baseline so it can't move the %.
    expected_pct = round(tracked_total / (1800.0 + 2950.0 + 1025.0) * 100, 1)
    assert body["overall_pacing_percentage"] == expected_pct


def test_no_untracked_spend_zero_bucket():
    """When every spending platform has a media plan line, the bucket is
    empty and total_actual_all_platforms == total_actual_to_date."""
    rec = QueryRecorder()
    rec.responses = [
        [_project_row()],
        _three_tracked_lines(),
        [],  # untracked query: nothing
    ]

    p1, p2, p3, p4, p5 = _patches(rec)
    with p1, p2, p3, p4, p5:
        client = TestClient(_make_app())
        resp = client.get("/api/pacing/26018")

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["untracked_spend"] == 0
    assert body["untracked_platforms"] == []
    assert body["total_actual_all_platforms"] == body["total_actual_to_date"]


def test_untracked_does_not_change_overall_pacing_vs_no_untracked_case():
    """Same tracked rows, with vs without an untracked platform — the
    overall_pacing_percentage must be byte-identical."""
    def run(untracked_rows):
        rec = QueryRecorder()
        rec.responses = [
            [_project_row()],
            _three_tracked_lines(),
            untracked_rows,
        ]
        p1, p2, p3, p4, p5 = _patches(rec)
        with p1, p2, p3, p4, p5:
            client = TestClient(_make_app())
            return client.get("/api/pacing/26018").json()

    with_untracked = run([_untracked_row()])
    without_untracked = run([])
    assert (
        with_untracked["overall_pacing_percentage"]
        == without_untracked["overall_pacing_percentage"]
    )


# ── SQL shape (locks in the mandatory dedup-guard pattern) ───────────


def test_untracked_query_sql_shape():
    """The untracked query must define "tracked" with the standard
    ROW_NUMBER dedup + plan_id IN (current media_plans × active
    project_media_plans) guard, and exclude zero-spend platforms.

    Locks in the pattern from feedback_mpl_dedup.md the same way the AI-001
    tests lock in the pending exclusion.
    """
    rec = QueryRecorder()
    rec.responses = [
        [_project_row()],
        _three_tracked_lines(),
        [_untracked_row()],
    ]

    p1, p2, p3, p4, p5 = _patches(rec)
    with p1, p2, p3, p4, p5:
        client = TestClient(_make_app())
        resp = client.get("/api/pacing/26018")

    assert resp.status_code == 200
    # calls: [0] dim_projects, [1] tracking, [2] untracked
    untracked_sql = rec.calls[2][0]
    assert "fact_digital_daily" in untracked_sql
    assert "ROW_NUMBER() OVER" in untracked_sql
    assert "plan_id IN" in untracked_sql
    assert "mp.is_current   = TRUE" in untracked_sql
    assert "pmp.is_active   = TRUE" in untracked_sql
    assert "NOT IN (SELECT platform_id FROM tracked_platforms)" in untracked_sql
    assert "HAVING SUM(f.spend) > 0" in untracked_sql


def test_untracked_respects_as_of_date():
    """Retrospective clamp: with ?as_of_date= the untracked query must carry
    a date param and a `f.date <= @as_of_date` clause so the replay never
    peeks past the snapshot date."""
    rec = QueryRecorder()
    rec.responses = [
        [_project_row()],
        [
            _tracking_row(date="2026-05-20", actual_spend_to_date=1883.49),
        ],
        [_untracked_row(spend=945.0, last_date="2026-05-20")],
    ]

    p1, p2, p3, p4, p5 = _patches(rec)
    with p1, p2, p3, p4, p5:
        client = TestClient(_make_app())
        resp = client.get("/api/pacing/26018?as_of_date=2026-05-20")

    assert resp.status_code == 200, resp.text
    untracked_sql, untracked_params = rec.calls[2]
    assert "f.date <= @as_of_date" in untracked_sql
    assert any(
        p[0] == "date" and p[1] == "as_of_date" for p in untracked_params
    ), f"expected an as_of_date date param, got {untracked_params}"


def test_untracked_query_omits_date_clamp_in_live_mode():
    """Live mode (no as_of_date) must not clamp — the bucket should reflect
    all warehouse spend, matching the header's semantics."""
    rec = QueryRecorder()
    rec.responses = [
        [_project_row()],
        _three_tracked_lines(),
        [],
    ]

    p1, p2, p3, p4, p5 = _patches(rec)
    with p1, p2, p3, p4, p5:
        client = TestClient(_make_app())
        resp = client.get("/api/pacing/26018")

    assert resp.status_code == 200
    untracked_sql, untracked_params = rec.calls[2]
    assert "f.date <= @as_of_date" not in untracked_sql
    assert not any(p[1] == "as_of_date" for p in untracked_params)


# ── Empty budget_tracking still reports untracked spend ──────────────


def test_empty_budget_tracking_still_reports_untracked():
    """A project whose pacing engine never ran but which has fact spend must
    not show $0 — the untracked bucket must survive the empty fallback path.

    (Live mode here; the retrospective compute-on-miss path is covered in
    tests/test_pacing_router_retro.py.)
    """
    rec = QueryRecorder()
    rec.responses = [
        [_project_row()],
        [],                  # no budget_tracking rows at all
        [_untracked_row()],  # but real spend exists in the warehouse
    ]

    p1, p2, p3, p4, p5 = _patches(rec)
    with p1, p2, p3, p4, p5:
        client = TestClient(_make_app())
        resp = client.get("/api/pacing/26018")

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["untracked_spend"] == 3002.72
    assert body["untracked_platforms"][0]["platform_id"] == "stackadapt"
    assert body["total_actual_all_platforms"] == 3002.72
    # Tracked totals stay zero — there's nothing tracked.
    assert body["total_actual_to_date"] == 0
    assert body["overall_pacing_percentage"] == 0
