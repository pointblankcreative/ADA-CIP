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
