"""Tests for the full-reimport progress plumbing (ADA 1215990005858989).

Two backend pieces, both in backend/routers/admin.py:

  1. GET /api/admin/transformation-status — reads the newest transform_<mode>
     ingestion_log row and returns a purpose-built status shape the Pipeline
     UI's poll loop consumes (running / success / failed / not-found).

  2. POST /api/admin/run-transformation FULL-mode concurrency guard — FULL mode
     TRUNCATEs fact_digital_daily, so two overlapping runs corrupt the table.
     Before starting a FULL run we check ingestion_log for a recent still-
     'running' transform_full row: if one is ACTIVE we return HTTP 409 with the
     in-flight run; an OLD 'running' row (a crashed/OOM-killed run, per ADAC-63)
     is treated as stale so it can't wedge the button forever; DELTA mode is
     never guarded.

Deterministic: admin_router.bq.run_query is monkeypatched to a stub that records
(sql, params) and returns fabricated ingestion_log rows — no BigQuery. The
blocking transform is monkeypatched to a lambda; api_run_transformation runs it
via asyncio.to_thread, so we drive the coroutine with asyncio.run.
"""

import asyncio
from datetime import datetime, timedelta, timezone

import pytest
from fastapi import HTTPException

from backend.routers import admin as admin_router


# ── helpers ──────────────────────────────────────────────────────────


def _stub_run_query(rows):
    """A run_query stub that records calls and always returns ``rows``."""
    calls = []

    def qr(sql, params=None):
        calls.append((sql, params))
        return rows

    qr.calls = calls
    return qr


def _row(status, started_at, *, completed_at=None, rows_upserted=0, error=None):
    return {
        "log_id": "log-123",
        "connector_name": "transform_full",
        "status": status,
        "run_started_at": started_at,
        "run_completed_at": completed_at,
        "rows_upserted": rows_upserted,
        "error_message": error,
    }


def _minutes_ago(mins):
    return datetime.now(timezone.utc) - timedelta(minutes=mins)


# ── GET /transformation-status ───────────────────────────────────────


def test_status_running_row(monkeypatch):
    started = _minutes_ago(3)
    qr = _stub_run_query([_row("running", started)])
    monkeypatch.setattr(admin_router.bq, "run_query", qr)

    res = asyncio.run(admin_router.get_transformation_status(mode="full"))

    assert res["found"] is True
    assert res["status"] == "running"
    assert res["mode"] == "full"
    assert res["finished_at"] is None
    assert res["error"] is None
    assert res["started_at"] is not None
    # read-only, newest-first, single row
    sql, params = qr.calls[0]
    assert "run_started_at DESC" in sql
    assert "LIMIT 1" in sql
    assert params[0].name == "connector"
    assert params[0].value == "transform_full"


def test_status_success_row(monkeypatch):
    qr = _stub_run_query(
        [_row("success", _minutes_ago(10), completed_at=_minutes_ago(2), rows_upserted=48231)]
    )
    monkeypatch.setattr(admin_router.bq, "run_query", qr)

    res = asyncio.run(admin_router.get_transformation_status(mode="full"))

    assert res["status"] == "success"
    assert res["rows"] == 48231
    assert res["finished_at"] is not None
    assert res["error"] is None


def test_status_failed_row(monkeypatch):
    qr = _stub_run_query(
        [_row("failed", _minutes_ago(5), completed_at=_minutes_ago(1), error="boom: OOM")]
    )
    monkeypatch.setattr(admin_router.bq, "run_query", qr)

    res = asyncio.run(admin_router.get_transformation_status(mode="full"))

    assert res["status"] == "failed"
    assert res["error"] == "boom: OOM"


def test_status_empty_table(monkeypatch):
    qr = _stub_run_query([])
    monkeypatch.setattr(admin_router.bq, "run_query", qr)

    res = asyncio.run(admin_router.get_transformation_status(mode="full"))

    assert res["found"] is False
    assert res["status"] is None


def test_status_daily_mode_uses_transform_daily_connector(monkeypatch):
    qr = _stub_run_query([])
    monkeypatch.setattr(admin_router.bq, "run_query", qr)

    asyncio.run(admin_router.get_transformation_status(mode="daily"))

    _sql, params = qr.calls[0]
    assert params[0].value == "transform_daily"


# ── POST /run-transformation FULL-mode concurrency guard ─────────────


def _recording_transform(calls):
    """A run_transformation stub that appends the mode it was invoked with."""

    def _fn(mode):
        calls.append(mode)
        return {"status": "success", "mode": mode}

    return _fn


def test_full_starts_when_no_prior_run(monkeypatch):
    """Empty ingestion_log → no active run → the transform starts."""
    qr = _stub_run_query([])
    monkeypatch.setattr(admin_router.bq, "run_query", qr)
    calls = []
    monkeypatch.setattr(admin_router, "run_transformation", _recording_transform(calls))

    res = asyncio.run(admin_router.api_run_transformation("full"))

    assert res["status"] == "success"
    assert calls == ["full"]


def test_full_starts_when_latest_is_completed(monkeypatch):
    """A prior 'success' row (even recent) is not active → the transform starts."""
    qr = _stub_run_query([_row("success", _minutes_ago(2), completed_at=_minutes_ago(1))])
    monkeypatch.setattr(admin_router.bq, "run_query", qr)
    calls = []
    monkeypatch.setattr(admin_router, "run_transformation", _recording_transform(calls))

    res = asyncio.run(admin_router.api_run_transformation("full"))

    assert res["status"] == "success"
    assert calls == ["full"]


def test_full_409_when_active_recent_running(monkeypatch):
    """A recent still-'running' transform_full row blocks a second start (409)
    and hands back the in-flight run so the UI can attach its poll to it."""
    qr = _stub_run_query([_row("running", _minutes_ago(4))])
    monkeypatch.setattr(admin_router.bq, "run_query", qr)
    started = {"count": 0}
    monkeypatch.setattr(
        admin_router, "run_transformation",
        lambda mode: started.__setitem__("count", started["count"] + 1) or {"status": "success"},
    )

    with pytest.raises(HTTPException) as exc:
        asyncio.run(admin_router.api_run_transformation("full"))

    assert exc.value.status_code == 409
    assert exc.value.detail["run"]["status"] == "running"
    assert exc.value.detail["run"]["started_at"] is not None
    # crucially, the second transform was NOT launched
    assert started["count"] == 0


def test_full_409_when_active_running_from_iso_string(monkeypatch):
    """run_started_at delivered as an ISO string (not a datetime) is still parsed
    for the age check — a recent one still blocks."""
    qr = _stub_run_query([_row("running", _minutes_ago(4).isoformat())])
    monkeypatch.setattr(admin_router.bq, "run_query", qr)
    monkeypatch.setattr(admin_router, "run_transformation", lambda mode: {"status": "success"})

    with pytest.raises(HTTPException) as exc:
        asyncio.run(admin_router.api_run_transformation("full"))

    assert exc.value.status_code == 409


def test_full_starts_when_running_row_is_stale(monkeypatch):
    """An OLD 'running' row (past the active window) is treated as stale/dead —
    a crashed run must not wedge the button forever — so the transform starts."""
    qr = _stub_run_query([_row("running", _minutes_ago(120))])
    monkeypatch.setattr(admin_router.bq, "run_query", qr)
    started = {"count": 0}
    monkeypatch.setattr(
        admin_router, "run_transformation",
        lambda mode: started.__setitem__("count", started["count"] + 1) or {"status": "success"},
    )

    res = asyncio.run(admin_router.api_run_transformation("full"))

    assert res["status"] == "success"
    assert started["count"] == 1


def test_daily_mode_is_never_guarded(monkeypatch):
    """DELTA (daily) mode must not run the FULL concurrency guard at all — even
    if a transform_full run were active, daily still starts and never queries."""
    qr = _stub_run_query([_row("running", _minutes_ago(1))])
    monkeypatch.setattr(admin_router.bq, "run_query", qr)
    monkeypatch.setattr(admin_router, "run_transformation", lambda mode: {"status": "success", "mode": mode})

    res = asyncio.run(admin_router.api_run_transformation("daily"))

    assert res["status"] == "success"
    assert res["mode"] == "daily"
    # the guard SELECT was never issued for daily mode
    assert qr.calls == []
