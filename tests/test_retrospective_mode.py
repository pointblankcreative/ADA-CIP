"""Tests for Retrospective Mode (ADAC-51).

Commit 1 scope: engine_version column + Pre-ADA backfill + config plumbing.
Later commits add coverage for the as_of_date thread, snapshots service, and
the retrospective router.
"""

from __future__ import annotations

from datetime import date

import pytest

from backend.config import settings
from backend.services.diagnostics import models
from backend.services.diagnostics.models import (
    CampaignType,
    DiagnosticOutput,
)


# ── engine_version on DiagnosticOutput ──────────────────────────────


def _make_output(**overrides) -> DiagnosticOutput:
    """Minimal DiagnosticOutput for serialization tests."""
    kwargs: dict = {
        "project_code": "25013",
        "campaign_type": CampaignType.PERSUASION,
        "evaluation_date": date(2026, 4, 23),
        "flight_day": 10,
        "flight_total_days": 30,
    }
    kwargs.update(overrides)
    return DiagnosticOutput(**kwargs)


def test_diagnostic_output_defaults_engine_version_from_settings(monkeypatch):
    """New DiagnosticOutput instances read settings.engine_version via the helper.

    We patch the indirection helper rather than mutating settings so the test
    is hermetic: other tests constructing outputs in the same run see the real
    settings value.
    """
    monkeypatch.setattr(models, "_current_engine_version", lambda: "sha-abc123")
    output = _make_output()
    assert output.engine_version == "sha-abc123"


def test_diagnostic_output_explicit_engine_version_wins():
    """Callers constructing with an explicit engine_version (e.g. retrospective
    replay resurrecting a historical row) should keep their value."""
    output = _make_output(engine_version="Pre-ADA")
    assert output.engine_version == "Pre-ADA"


def test_to_bq_row_includes_engine_version(monkeypatch):
    """to_bq_row serializes engine_version so _store_results writes the column."""
    monkeypatch.setattr(models, "_current_engine_version", lambda: "sha-abc123")
    output = _make_output()
    row = output.to_bq_row()
    assert row["engine_version"] == "sha-abc123"
    # spec_version stays orthogonal — both should be present.
    assert row["spec_version"] == "1.1"


def test_to_bq_row_preserves_pre_ada_tag():
    """Rows resurrected from BQ for downstream comparison keep their original tag."""
    output = _make_output(engine_version="Pre-ADA")
    row = output.to_bq_row()
    assert row["engine_version"] == "Pre-ADA"


# ── Settings ────────────────────────────────────────────────────────


def test_settings_engine_version_defaults_to_dev_when_unset(monkeypatch):
    """Without the ENGINE_VERSION env var, settings fall back to 'dev'.

    We construct a fresh Settings instance with the env var cleared so the
    test doesn't depend on whatever the global `settings` was initialised
    with at module-import time.
    """
    from backend.config import Settings
    monkeypatch.delenv("ENGINE_VERSION", raising=False)
    s = Settings()
    assert s.engine_version == "dev"


def test_settings_engine_version_reads_from_env(monkeypatch):
    """The ENGINE_VERSION env var (set by Cloud Build) overrides the default."""
    from backend.config import Settings
    monkeypatch.setenv("ENGINE_VERSION", "sha-deadbeef")
    s = Settings()
    assert s.engine_version == "sha-deadbeef"


# ── evaluation_date is required (commit 2) ──────────────────────────


def test_run_diagnostics_for_project_requires_evaluation_date():
    """Callers must explicitly supply evaluation_date.

    Locked in by ADAC-51 commit 2. The previous ``= None`` default let callers
    silently score "today-shaped" results even in retrospective contexts — the
    new contract forces a conscious decision at every call site.
    """
    from backend.services.diagnostics.engine import run_diagnostics_for_project
    with pytest.raises(TypeError, match="evaluation_date"):
        run_diagnostics_for_project("25013")  # type: ignore[call-arg]


def test_run_all_diagnostics_requires_evaluation_date():
    """Same contract applies to the sweep entry point."""
    from backend.services.diagnostics.engine import run_all_diagnostics
    with pytest.raises(TypeError, match="evaluation_date"):
        run_all_diagnostics()  # type: ignore[call-arg]


# ── pacing as_of_date + skip_writes (commit 3) ──────────────────────


def test_run_pacing_for_project_requires_as_of_date():
    """Pacing entry point now requires an explicit anchor date (commit 3)."""
    from backend.services.pacing import run_pacing_for_project
    with pytest.raises(TypeError, match="as_of_date"):
        run_pacing_for_project("25013")  # type: ignore[call-arg]


def test_run_all_active_requires_as_of_date():
    from backend.services.pacing import run_all_active
    with pytest.raises(TypeError, match="as_of_date"):
        run_all_active()  # type: ignore[call-arg]


def test_pacing_skip_writes_suppresses_budget_tracking_and_alerts():
    """Retrospective callers set skip_writes=True so replays don't pollute
    budget_tracking with reconstructed rows or fire alerts about past state.
    """
    from datetime import date as _date, timedelta
    from unittest.mock import MagicMock, patch

    from backend.services.pacing import run_pacing_for_project

    today = _date.today()
    flight_start = today - timedelta(days=5)
    flight_end = today + timedelta(days=15)

    line = {
        "line_id": "retro-line-01",
        "line_code": "TEST",
        "platform_id": "meta",
        "channel_category": "Digital",
        "budget": 10000.0,
        "flight_start": flight_start.isoformat(),
        "flight_end": flight_end.isoformat(),
    }
    blocking_weeks = [{
        "line_id": "retro-line-01",
        "week_start": flight_start.isoformat(),
        "is_active": True,
    }]

    with patch("backend.services.pacing.bq") as mock_bq, \
         patch("backend.services.pacing._write_budget_tracking") as mock_write_bt, \
         patch("backend.services.pacing._write_alerts") as mock_write_alerts:

        mock_bq.table.return_value = "dummy_table"
        mock_bq.string_param.return_value = MagicMock()
        mock_bq.scalar_param.return_value = MagicMock()
        mock_bq.date_param.return_value = MagicMock()

        call_count = [0]

        def mock_run_query(sql, params=None):
            call_count[0] += 1
            if call_count[0] == 1:
                return [line]
            elif call_count[0] == 2:
                return blocking_weeks
            return []

        mock_bq.run_query.side_effect = mock_run_query

        run_pacing_for_project("TEST01", today, skip_writes=True)

        # Neither write should have been called when skip_writes=True.
        mock_write_bt.assert_not_called()
        mock_write_alerts.assert_not_called()


# ── snapshots service (commit 4) ────────────────────────────────────


def test_find_snapshot_returns_empty_on_miss(monkeypatch):
    """A miss returns an empty list, not None. Callers can then decide to
    recompute without having to special-case None."""
    from backend.services import snapshots

    captured = {}

    def mock_run_query(sql, params):
        captured["sql"] = sql
        captured["params"] = params
        return []

    monkeypatch.setattr(snapshots.bq, "run_query", mock_run_query)
    monkeypatch.setattr(snapshots.bq, "string_param", lambda *a, **k: ("string", a, k))
    monkeypatch.setattr(snapshots.bq, "date_param", lambda *a, **k: ("date", a, k))
    monkeypatch.setattr(snapshots.bq, "table", lambda n: f"`dummy.{n}`")

    rows = snapshots.find_snapshot("25013", date(2026, 4, 15), engine_version="sha-abc")
    assert rows == []
    # Sanity: the query filters on all three key parts.
    assert "project_code" in captured["sql"]
    assert "evaluation_date" in captured["sql"]
    assert "engine_version" in captured["sql"]


def test_find_snapshot_returns_latest_per_campaign_type(monkeypatch):
    """When multiple rows exist for the same (project, date, version, type),
    the query picks the latest computed_at via ROW_NUMBER."""
    from backend.services import snapshots

    expected_rows = [
        {"campaign_type": "persuasion", "health_score": 81.0, "engine_version": "sha-abc"},
        {"campaign_type": "conversion", "health_score": 65.0, "engine_version": "sha-abc"},
    ]

    monkeypatch.setattr(snapshots.bq, "run_query", lambda sql, params: expected_rows)
    monkeypatch.setattr(snapshots.bq, "string_param", lambda *a, **k: object())
    monkeypatch.setattr(snapshots.bq, "date_param", lambda *a, **k: object())
    monkeypatch.setattr(snapshots.bq, "table", lambda n: f"`dummy.{n}`")

    rows = snapshots.find_snapshot("25013", date(2026, 4, 15), engine_version="sha-abc")
    assert len(rows) == 2
    assert rows[0]["campaign_type"] == "persuasion"
    assert rows[1]["campaign_type"] == "conversion"


def test_find_snapshot_defaults_to_current_engine_version(monkeypatch):
    """When engine_version is None, the helper uses settings.engine_version.

    This is the common caller intent — 'give me the cache hit if I've
    computed this date with today's code'.
    """
    from backend.services import snapshots

    monkeypatch.setattr(models, "_current_engine_version", lambda: "sha-live-deploy")
    # The settings singleton doesn't auto-update when _current_engine_version
    # changes, so also patch settings.engine_version for this test.
    monkeypatch.setattr(snapshots.settings, "engine_version", "sha-live-deploy")

    captured_params = {}

    def mock_run_query(sql, params):
        for p in params:
            # Each param is (kind, (name, value), {}) from the lambdas above.
            if p[0] == "string" and p[1][0] == "engine_version":
                captured_params["engine_version"] = p[1][1]
        return []

    monkeypatch.setattr(snapshots.bq, "run_query", mock_run_query)
    monkeypatch.setattr(snapshots.bq, "string_param", lambda name, value: ("string", (name, value), {}))
    monkeypatch.setattr(snapshots.bq, "date_param", lambda name, value: ("date", (name, value), {}))
    monkeypatch.setattr(snapshots.bq, "table", lambda n: f"`dummy.{n}`")

    snapshots.find_snapshot("25013", date(2026, 4, 15))  # no engine_version
    assert captured_params["engine_version"] == "sha-live-deploy"


def test_find_or_compute_hits_cache(monkeypatch):
    """Cache hit path: find_snapshot returns rows, compute_and_store is never called."""
    from backend.services import snapshots

    cached = [{"campaign_type": "persuasion", "health_score": 81.0}]
    monkeypatch.setattr(snapshots, "find_snapshot", lambda *a, **k: cached)
    # compute_and_store must NOT be called — explode if it is.
    monkeypatch.setattr(
        snapshots, "compute_and_store",
        lambda *a, **k: (_ for _ in ()).throw(AssertionError("should not recompute on hit")),
    )

    result = snapshots.find_or_compute("25013", date(2026, 4, 15))
    assert result == cached


def test_find_or_compute_computes_on_miss(monkeypatch):
    """Cache miss path: compute_and_store is called, its output is serialized via to_bq_row."""
    from backend.services import snapshots

    monkeypatch.setattr(snapshots, "find_snapshot", lambda *a, **k: [])

    fake_output = _make_output(engine_version="sha-abc")

    calls = []

    def fake_compute(project_code, as_of_date):
        calls.append((project_code, as_of_date))
        return [fake_output]

    monkeypatch.setattr(snapshots, "compute_and_store", fake_compute)

    result = snapshots.find_or_compute("25013", date(2026, 4, 15))

    assert len(calls) == 1
    assert calls[0] == ("25013", date(2026, 4, 15))
    assert len(result) == 1
    assert result[0]["project_code"] == "25013"
    assert result[0]["engine_version"] == "sha-abc"


def test_find_or_compute_bypass_cache_forces_recompute(monkeypatch):
    """bypass_cache=True skips find_snapshot entirely — used by the ADAC-37
    batch backfill so it's immune to stale cache hits."""
    from backend.services import snapshots

    # find_snapshot MUST NOT be called when bypass_cache=True. Explode if it is.
    def forbidden(*args, **kwargs):
        raise AssertionError("find_snapshot must not be called when bypass_cache=True")

    monkeypatch.setattr(snapshots, "find_snapshot", forbidden)
    monkeypatch.setattr(snapshots, "compute_and_store", lambda *a, **k: [_make_output()])

    # Should not raise even though find_snapshot would blow up.
    result = snapshots.find_or_compute("25013", date(2026, 4, 15), bypass_cache=True)
    assert len(result) == 1


# ── retrospective router (commit 5) ─────────────────────────────────


def test_retrospective_endpoint_returns_expected_shape(monkeypatch):
    """Integration test over the HTTP layer: mock snapshots + pacing and
    exercise the full request → response path."""
    from fastapi.testclient import TestClient

    from backend import main
    from backend.routers import retrospective
    from backend.services import snapshots as snapshots_mod

    # Stub the auth middleware so tests don't need a Firebase token.
    # Simplest: patch the middleware's dispatch to pass requests through.
    from backend.middleware import auth as auth_mod

    async def passthrough(self, request, call_next):
        return await call_next(request)

    monkeypatch.setattr(auth_mod.FirebaseAuthMiddleware, "dispatch", passthrough)

    fake_rows = [
        {
            "id": "row-1",
            "project_code": "25013",
            "campaign_type": "persuasion",
            "evaluation_date": date(2026, 3, 1),
            "flight_day": 10,
            "flight_total_days": 30,
            "health_score": 81.0,
            "health_status": "STRONG",
            "pillars": {"distribution": {"score": 80, "status": "STRONG"}},
            "signals": [],
            "efficiency": {},
            "alerts": [],
            "platforms": ["meta"],
            "line_ids": ["l1"],
            "computed_at": None,
            "spec_version": "1.1",
            "engine_version": "sha-abc",
        }
    ]

    monkeypatch.setattr(snapshots_mod, "find_or_compute", lambda *a, **k: fake_rows)
    monkeypatch.setattr(snapshots_mod, "find_snapshot", lambda *a, **k: fake_rows)
    monkeypatch.setattr(
        retrospective, "run_pacing_for_project",
        lambda *a, **k: {"project_code": "25013", "lines_processed": 3, "alerts": 0},
    )
    monkeypatch.setattr(retrospective.settings, "engine_version", "sha-abc")

    client = TestClient(main.app)
    resp = client.get("/api/diagnostics/as-of/2026-03-01/project/25013")
    assert resp.status_code == 200, resp.text
    body = resp.json()

    assert body["project_code"] == "25013"
    assert body["as_of_date"] == "2026-03-01"
    assert body["engine_version"] == "sha-abc"
    assert body["cached"] is True
    assert len(body["diagnostics"]) == 1
    assert body["diagnostics"][0]["campaign_type"] == "persuasion"
    assert body["diagnostics"][0]["health_score"] == 81.0
    assert body["pacing"]["lines_processed"] == 3


def test_retrospective_endpoint_reports_cache_miss_when_computed(monkeypatch):
    """When find_or_compute had to compute fresh (no prior cached row), the
    response's `cached` flag should be False.

    Simulates: find_or_compute returns rows (just-computed), but
    find_snapshot (the probe) finds nothing because it ran before the write
    was visible — or more realistically, the returned rows came from the
    compute path. Test the probe-returns-empty case.
    """
    from fastapi.testclient import TestClient

    from backend import main
    from backend.routers import retrospective
    from backend.services import snapshots as snapshots_mod
    from backend.middleware import auth as auth_mod

    async def passthrough(self, request, call_next):
        return await call_next(request)

    monkeypatch.setattr(auth_mod.FirebaseAuthMiddleware, "dispatch", passthrough)

    fake_rows = [{
        "id": "row-1",
        "project_code": "25013",
        "campaign_type": "persuasion",
        "evaluation_date": date(2026, 3, 1),
        "flight_day": 10,
        "flight_total_days": 30,
        "health_score": 70.0,
        "health_status": "WATCH",
        "pillars": {},
        "signals": [],
        "efficiency": {},
        "alerts": [],
        "platforms": [],
        "line_ids": [],
        "computed_at": None,
        "spec_version": "1.1",
        "engine_version": "sha-abc",
    }]

    monkeypatch.setattr(snapshots_mod, "find_or_compute", lambda *a, **k: fake_rows)
    # probe returns empty → cached should be False.
    monkeypatch.setattr(snapshots_mod, "find_snapshot", lambda *a, **k: [])
    monkeypatch.setattr(
        retrospective, "run_pacing_for_project",
        lambda *a, **k: {"project_code": "25013", "lines_processed": 0, "alerts": 0},
    )
    monkeypatch.setattr(retrospective.settings, "engine_version", "sha-abc")

    client = TestClient(main.app)
    resp = client.get("/api/diagnostics/as-of/2026-03-01/project/25013")
    assert resp.status_code == 200
    assert resp.json()["cached"] is False


def test_retrospective_endpoint_rejects_bad_date():
    """FastAPI's date path-converter should 422 on malformed input before our
    handler runs, so we don't have to validate dates ourselves."""
    from fastapi.testclient import TestClient
    from backend import main

    client = TestClient(main.app)
    resp = client.get("/api/diagnostics/as-of/not-a-date/project/25013")
    assert resp.status_code == 422


def test_retrospective_endpoint_skip_writes_on_pacing(monkeypatch):
    """Regression guard: the endpoint MUST pass skip_writes=True to pacing —
    otherwise a retrospective request would pollute budget_tracking and fire
    Slack alerts about yesterday's state."""
    from fastapi.testclient import TestClient

    from backend import main
    from backend.routers import retrospective
    from backend.services import snapshots as snapshots_mod
    from backend.middleware import auth as auth_mod

    async def passthrough(self, request, call_next):
        return await call_next(request)

    monkeypatch.setattr(auth_mod.FirebaseAuthMiddleware, "dispatch", passthrough)

    captured_kwargs = {}

    def fake_pacing(project_code, as_of_date, **kwargs):
        captured_kwargs.update(kwargs)
        captured_kwargs["project_code"] = project_code
        captured_kwargs["as_of_date"] = as_of_date
        return {"project_code": project_code, "lines_processed": 0, "alerts": 0}

    monkeypatch.setattr(snapshots_mod, "find_or_compute", lambda *a, **k: [])
    monkeypatch.setattr(snapshots_mod, "find_snapshot", lambda *a, **k: [])
    monkeypatch.setattr(retrospective, "run_pacing_for_project", fake_pacing)

    client = TestClient(main.app)
    resp = client.get("/api/diagnostics/as-of/2026-03-01/project/25013")
    assert resp.status_code == 200
    assert captured_kwargs["skip_writes"] is True, (
        "Retrospective endpoint must pass skip_writes=True to pacing to avoid "
        "corrupting budget_tracking with reconstructed rows."
    )
    assert captured_kwargs["as_of_date"] == date(2026, 3, 1)


def test_pacing_default_skip_writes_false_still_writes():
    """Regression guard: live callers (skip_writes default=False) continue to
    write to budget_tracking. This is the current-pipeline behaviour — we'd
    break the dashboard if skip_writes defaulted to True.
    """
    from datetime import date as _date, timedelta
    from unittest.mock import MagicMock, patch

    from backend.services.pacing import run_pacing_for_project

    today = _date.today()
    flight_start = today - timedelta(days=5)
    flight_end = today + timedelta(days=15)

    line = {
        "line_id": "live-line-01",
        "line_code": "TEST",
        "platform_id": "meta",
        "channel_category": "Digital",
        "budget": 10000.0,
        "flight_start": flight_start.isoformat(),
        "flight_end": flight_end.isoformat(),
    }
    blocking_weeks = [{
        "line_id": "live-line-01",
        "week_start": flight_start.isoformat(),
        "is_active": True,
    }]

    with patch("backend.services.pacing.bq") as mock_bq, \
         patch("backend.services.pacing._write_budget_tracking") as mock_write_bt, \
         patch("backend.services.pacing._write_alerts"):

        mock_bq.table.return_value = "dummy_table"
        mock_bq.string_param.return_value = MagicMock()
        mock_bq.scalar_param.return_value = MagicMock()
        mock_bq.date_param.return_value = MagicMock()

        call_count = [0]

        def mock_run_query(sql, params=None):
            call_count[0] += 1
            if call_count[0] == 1:
                return [line]
            elif call_count[0] == 2:
                return blocking_weeks
            return []

        mock_bq.run_query.side_effect = mock_run_query

        # Live call: no skip_writes kwarg, defaults to False.
        run_pacing_for_project("TEST01", today)

        # Write MUST have been called — this is the live pipeline's contract.
        assert mock_write_bt.called, (
            "Live callers (skip_writes default=False) must still write to "
            "budget_tracking; the dashboard depends on this row."
        )
