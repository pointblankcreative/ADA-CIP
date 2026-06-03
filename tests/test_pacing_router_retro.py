"""Tests for the AI-070/071/072 retrospective fixes on GET /api/pacing/{code}.

Bug cluster (observed on 26018 @ 2026-05-19):
  - AI-070: requesting a date with no stored budget_tracking row returned a
    default-zeros PacingResponse — "all zeros" on the Historical Pacing view.
  - AI-071: that fallback hardcoded ``as_of_date=date.today()`` — the "AS OF"
    heading contradicted the page banner's replay date.
  - AI-072: ``lines`` defaulted to [] so the line-by-line table was empty,
    even though the retro endpoint was computing (and discarding) the exact
    replay needed.

Fixes under test:
  1. Compute-on-miss: when ``as_of_date`` has no stored row, the router runs
     ``run_pacing_for_project(..., skip_writes=True)`` and serves its
     ``lines`` (joined with plan metadata), flagged ``replayed=True``.
  2. Honest empty state: when the replay is impossible too, the response
     carries ``snapshot_missing=True``, echoes the REQUESTED date (never
     today), and reports ``earliest_snapshot_date`` so the UI can render
     "snapshots begin YYYY-MM-DD".
  3. Stored rows always win: compute-on-miss never shadows a stored snapshot,
     and the live path (no as_of_date) never replays.
"""

from __future__ import annotations

from datetime import date
from unittest.mock import patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.routers import pacing as pacing_router


# ── Helpers (same QueryRecorder pattern as test_pacing_router_untracked) ──


def _make_app() -> FastAPI:
    app = FastAPI()
    app.include_router(pacing_router.router)
    return app


class QueryRecorder:
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


def _replay_line(**overrides):
    """A tracking row as run_pacing_for_project builds it (step 4 of the
    engine) — budget_tracking column shape, date as ISO string."""
    base = {
        "date": "2026-05-19",
        "project_code": "26018",
        "line_id": "plan-26018-4f0de153-line-000",
        "line_code": "",
        "platform_id": "meta",
        "channel_category": "Digital",
        "line_status": "active",
        "planned_budget": 2145.0,
        "planned_spend_to_date": 840.0,
        "actual_spend_to_date": 752.1,
        "remaining_budget": 1392.9,
        "remaining_days": 17,
        "pacing_percentage": 89.5,
        "daily_budget_required": 81.9,
        "is_over_pacing": False,
        "is_under_pacing": False,
        "bundle_id": None,
        "bundle_role": None,
    }
    base.update(overrides)
    return base


def _meta_row(line_id, **overrides):
    base = {
        "line_id": line_id,
        "audience_name": "Members",
        "flight_start": "2026-05-07",
        "flight_end": "2026-06-05",
        "sheet_id": "sheet-1",
        "phase_label": None,
        "phase_display_order": 1,
        "phase_is_active": True,
    }
    base.update(overrides)
    return base


def _stored_row(**overrides):
    """A budget_tracking row joined with mpl metadata, as the stored read
    path's tracking SQL returns it."""
    base = {
        **_replay_line(),
        **{k: v for k, v in _meta_row("plan-26018-4f0de153-line-000").items()
           if k != "line_id"},
    }
    base.update(overrides)
    return base


# ── Compute-on-miss replay (AI-070 / AI-072) ─────────────────────────


def test_compute_on_miss_serves_replay_lines():
    """No stored row for the requested date but the replay is computable →
    200 with non-empty lines, replayed=True, snapshot_missing=False, and
    as_of_date == the REQUESTED date."""
    rec = QueryRecorder()
    rec.responses = [
        [_project_row()],   # dim_projects
        [],                 # tracking: no stored row for 2026-05-19
        [],                 # untracked: nothing
        # (replay itself is mocked — no recorder traffic)
        [_meta_row("plan-26018-4f0de153-line-000")],  # _attach_plan_metadata
        # bundle-members query never fires (no bundle_ids)
    ]

    captured = {}

    def fake_replay(project_code, as_of_date, skip_writes=False):
        captured["project_code"] = project_code
        captured["as_of_date"] = as_of_date
        captured["skip_writes"] = skip_writes
        return {
            "project_code": project_code,
            "lines_processed": 1,
            "alerts": 0,
            "lines": [_replay_line()],
        }

    p1, p2, p3, p4, p5 = _patches(rec)
    with p1, p2, p3, p4, p5, patch.object(
        pacing_router, "run_pacing_for_project", side_effect=fake_replay
    ):
        client = TestClient(_make_app())
        resp = client.get("/api/pacing/26018?as_of_date=2026-05-19")

    assert resp.status_code == 200, resp.text
    body = resp.json()

    # AI-071: the response echoes the requested date, never today.
    assert body["as_of_date"] == "2026-05-19"
    assert body["replayed"] is True
    assert body["snapshot_missing"] is False

    # AI-072: the replay's per-line rows reach the response, with plan
    # metadata joined on.
    assert len(body["lines"]) == 1
    line = body["lines"][0]
    assert line["line_id"] == "plan-26018-4f0de153-line-000"
    assert line["actual_spend_to_date"] == 752.1
    assert line["audience_name"] == "Members"
    assert line["flight_start"] == "2026-05-07"
    assert line["sheet_id"] == "sheet-1"

    # AI-070: KPI totals are non-zero.
    assert body["total_actual_to_date"] == 752.1
    assert body["total_planned_to_date"] == 840.0
    assert body["overall_pacing_percentage"] == round(752.1 / 840.0 * 100, 1)

    # The replay was invoked correctly: requested date + skip_writes=True so
    # the read path never pollutes budget_tracking or fires alerts.
    assert captured["as_of_date"] == date(2026, 5, 19)
    assert captured["skip_writes"] is True


def test_replay_failure_falls_back_to_honest_empty_state():
    """If the replay itself raises, the endpoint must not 500 — it falls
    back to the honest empty state."""
    rec = QueryRecorder()
    rec.responses = [
        [_project_row()],
        [],                          # no stored rows
        [],                          # untracked
        [{"d": date(2026, 5, 20)}],  # earliest-snapshot query
    ]

    def exploding_replay(*a, **k):
        raise RuntimeError("BQ exploded")

    p1, p2, p3, p4, p5 = _patches(rec)
    with p1, p2, p3, p4, p5, patch.object(
        pacing_router, "run_pacing_for_project", side_effect=exploding_replay
    ):
        client = TestClient(_make_app())
        resp = client.get("/api/pacing/26018?as_of_date=2026-05-19")

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["snapshot_missing"] is True
    assert body["as_of_date"] == "2026-05-19"


# ── Honest empty state (AI-070 / AI-071) ─────────────────────────────


def test_empty_state_echoes_requested_date_and_earliest_snapshot():
    """No stored row AND no computable replay (no media plan) → honest empty
    state: snapshot_missing=True, as_of_date == requested date (NOT today),
    earliest_snapshot_date from MIN(date)."""
    rec = QueryRecorder()
    rec.responses = [
        [_project_row()],
        [],                          # no stored rows for 2026-05-01
        [],                          # untracked
        [{"d": date(2026, 5, 20)}],  # earliest-snapshot query
    ]

    def no_lines_replay(project_code, as_of_date, skip_writes=False):
        return {
            "project_code": project_code,
            "lines_processed": 0,
            "alerts": 0,
            "lines": [],
        }

    p1, p2, p3, p4, p5 = _patches(rec)
    with p1, p2, p3, p4, p5, patch.object(
        pacing_router, "run_pacing_for_project", side_effect=no_lines_replay
    ):
        client = TestClient(_make_app())
        resp = client.get("/api/pacing/26018?as_of_date=2026-05-01")

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["snapshot_missing"] is True
    assert body["as_of_date"] == "2026-05-01", (
        "AI-071: the empty state must echo the REQUESTED date, not today"
    )
    assert body["earliest_snapshot_date"] == "2026-05-20"
    assert body["lines"] == []
    assert body["replayed"] is False
    # Budget still surfaces so the UI can show context.
    assert body["net_budget"] == 12750.0


def test_empty_state_earliest_none_when_no_history_at_all():
    rec = QueryRecorder()
    rec.responses = [
        [_project_row()],
        [],
        [],
        [{"d": None}],  # MIN(date) over zero rows → NULL
    ]

    def no_lines_replay(*a, **k):
        return {"project_code": "26018", "lines_processed": 0, "alerts": 0,
                "lines": []}

    p1, p2, p3, p4, p5 = _patches(rec)
    with p1, p2, p3, p4, p5, patch.object(
        pacing_router, "run_pacing_for_project", side_effect=no_lines_replay
    ):
        client = TestClient(_make_app())
        resp = client.get("/api/pacing/26018?as_of_date=2026-04-01")

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["snapshot_missing"] is True
    assert body["earliest_snapshot_date"] is None


# ── Stored rows always win; live path never replays ──────────────────


def test_stored_snapshot_is_never_shadowed_by_replay():
    """A date WITH a stored budget_tracking row must be served from storage —
    run_pacing_for_project must not be invoked, and replayed stays False.
    Regression guard that compute-on-miss never shadows stored rows."""
    rec = QueryRecorder()
    rec.responses = [
        [_project_row()],
        [_stored_row(date="2026-05-25", actual_spend_to_date=3290.0,
                     planned_spend_to_date=3600.0)],
        [],  # untracked
    ]

    p1, p2, p3, p4, p5 = _patches(rec)
    with p1, p2, p3, p4, p5, patch.object(
        pacing_router, "run_pacing_for_project",
        side_effect=AssertionError("replay must not run when a stored row exists"),
    ):
        client = TestClient(_make_app())
        resp = client.get("/api/pacing/26018?as_of_date=2026-05-25")

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["as_of_date"] == "2026-05-25"
    assert body["replayed"] is False
    assert body["snapshot_missing"] is False
    assert body["total_actual_to_date"] == 3290.0


def test_live_path_never_replays_on_empty():
    """Live mode (as_of_date omitted) with no budget_tracking rows must NOT
    attempt a replay — the empty state is the correct live answer ('run the
    pacing engine'), and replaying on every live page view of a fresh
    project would be a silent BQ cost."""
    rec = QueryRecorder()
    rec.responses = [
        [_project_row()],
        [],            # no rows at all
        [],            # untracked
        [{"d": None}],  # earliest-snapshot query
    ]

    p1, p2, p3, p4, p5 = _patches(rec)
    with p1, p2, p3, p4, p5, patch.object(
        pacing_router, "run_pacing_for_project",
        side_effect=AssertionError("live path must not replay"),
    ):
        client = TestClient(_make_app())
        resp = client.get("/api/pacing/26018")

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["snapshot_missing"] is True
    assert body["replayed"] is False


def test_live_path_unchanged_with_stored_rows():
    """Live path (no as_of_date) with stored rows: latest row served,
    replayed=False — byte-compatible with the pre-fix contract apart from
    the additive fields."""
    rec = QueryRecorder()
    rec.responses = [
        [_project_row()],
        [_stored_row(date="2026-06-03")],
        [],  # untracked
    ]

    p1, p2, p3, p4, p5 = _patches(rec)
    with p1, p2, p3, p4, p5, patch.object(
        pacing_router, "run_pacing_for_project",
        side_effect=AssertionError("live path must not replay"),
    ):
        client = TestClient(_make_app())
        resp = client.get("/api/pacing/26018")

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["as_of_date"] == "2026-06-03"
    assert body["replayed"] is False
    assert body["snapshot_missing"] is False
    assert len(body["lines"]) == 1


# ── Replay metadata join ─────────────────────────────────────────────


def test_replay_lines_sorted_like_stored_path():
    """Replayed rows must come back in the stored path's ORDER BY
    (phase_display_order NULLS LAST, sheet_id, platform_id, bundle_id,
    line_code) so phase grouping renders identically either way."""
    rec = QueryRecorder()
    rec.responses = [
        [_project_row()],
        [],   # no stored rows
        [],   # untracked
        [     # metadata for both lines — phase 2 line listed first on purpose
            _meta_row("line-b", sheet_id="sheet-2", phase_display_order=2,
                      audience_name="Phase2 line"),
            _meta_row("line-a", sheet_id="sheet-1", phase_display_order=1,
                      audience_name="Phase1 line"),
        ],
    ]

    def fake_replay(project_code, as_of_date, skip_writes=False):
        return {
            "project_code": project_code,
            "lines_processed": 2,
            "alerts": 0,
            # Engine order: phase-2 line first — must be re-sorted.
            "lines": [
                _replay_line(line_id="line-b", platform_id="stackadapt"),
                _replay_line(line_id="line-a", platform_id="meta"),
            ],
        }

    p1, p2, p3, p4, p5 = _patches(rec)
    with p1, p2, p3, p4, p5, patch.object(
        pacing_router, "run_pacing_for_project", side_effect=fake_replay
    ):
        client = TestClient(_make_app())
        resp = client.get("/api/pacing/26018?as_of_date=2026-05-19")

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert [l["line_id"] for l in body["lines"]] == ["line-a", "line-b"]
    # Phase summaries aggregate per sheet, ordered by display_order.
    assert [p["sheet_id"] for p in body["phases"]] == ["sheet-1", "sheet-2"]
