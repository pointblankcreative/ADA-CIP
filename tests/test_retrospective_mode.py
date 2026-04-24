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
