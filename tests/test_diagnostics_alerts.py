"""Tests for diagnostic alert generation + dedup.

Covers the rules in docs/diagnostics/alert-rules.md:
  - Signal-level ACTION alerts fire per failing signal in a scored pillar
  - Guard-failed and non-ACTION signals do NOT fire
  - Health regression fires only on transition INTO ACTION
  - ACTION -> ACTION does NOT fire a regression alert
  - 24h dedup by (project_code, alert_type, severity)
  - Mixed campaigns produce per-campaign-type alerts with namespaced IDs

The signal math itself is covered elsewhere — these tests assemble
DiagnosticOutput objects directly and exercise the alert code paths.
"""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import pytest

from backend.services.diagnostics.models import (
    AlertSeverity,
    CampaignType,
    DiagnosticOutput,
    PillarScore,
    SignalResult,
    StatusBand,
)
from backend.services.diagnostics.shared.alerts import (
    build_regression_alert,
    populate_signal_alerts,
)


# ── helpers ─────────────────────────────────────────────────────────


def _signal(
    sid: str,
    score: float | None,
    status: StatusBand | None,
    guard_passed: bool = True,
    name: str | None = None,
    diagnostic: str = "",
) -> SignalResult:
    return SignalResult(
        id=sid,
        name=name or f"{sid} Signal",
        score=score,
        status=status,
        guard_passed=guard_passed,
        diagnostic=diagnostic,
    )


def _pillar(
    name: str,
    signals: list[SignalResult],
    score: float | None = 50.0,
    status: StatusBand | None = StatusBand.WATCH,
    weight: float = 0.5,
) -> PillarScore:
    return PillarScore(
        name=name,
        signals=signals,
        score=score,
        status=status,
        weight=weight,
    )


def _output(
    pillars: list[PillarScore],
    health_score: float | None = 50.0,
    health_status: StatusBand | None = StatusBand.WATCH,
    project_code: str = "26009",
    campaign_type: CampaignType = CampaignType.CONVERSION,
) -> DiagnosticOutput:
    return DiagnosticOutput(
        project_code=project_code,
        campaign_type=campaign_type,
        evaluation_date=date(2026, 4, 17),
        flight_day=10,
        flight_total_days=30,
        health_score=health_score,
        health_status=health_status,
        pillars=pillars,
    )


# ── Signal-level alert tests ────────────────────────────────────────


class TestSignalLevelAlerts:
    def test_action_signal_fires_one_alert(self):
        output = _output([
            _pillar("funnel", [
                _signal("F1", 22.0, StatusBand.ACTION,
                        diagnostic="Click-to-LP collapse"),
            ]),
        ])
        populate_signal_alerts(output)

        assert len(output.alerts) == 1
        alert = output.alerts[0]
        assert alert.type == "signal_f1"
        assert alert.severity == AlertSeverity.CRITICAL
        assert alert.signal_id == "F1"
        # Diagnostic text should be embedded in the message
        assert "Click-to-LP collapse" in alert.message
        # Flight context included
        assert "Flight day 10 of 30" in alert.message

    def test_strong_and_watch_signals_do_not_fire(self):
        output = _output([
            _pillar("funnel", [
                _signal("F1", 85.0, StatusBand.STRONG),
                _signal("F2", 55.0, StatusBand.WATCH),
                _signal("F3", 65.0, StatusBand.WATCH),
            ]),
        ])
        populate_signal_alerts(output)
        assert output.alerts == []

    def test_guard_failed_signal_does_not_fire_even_in_action_band(self):
        """A signal with guard_passed=False must never fire, even if its
        status somehow shows ACTION (defense-in-depth)."""
        output = _output([
            _pillar("funnel", [
                _signal("F1", 10.0, StatusBand.ACTION, guard_passed=False),
            ]),
        ])
        populate_signal_alerts(output)
        assert output.alerts == []

    def test_multiple_action_signals_each_fire(self):
        output = _output([
            _pillar("acquisition", [
                _signal("C1", 20.0, StatusBand.ACTION),
                _signal("C2", 30.0, StatusBand.ACTION),
                _signal("C3", 80.0, StatusBand.STRONG),
            ]),
            _pillar("funnel", [
                _signal("F1", 15.0, StatusBand.ACTION),
            ]),
        ])
        populate_signal_alerts(output)

        assert len(output.alerts) == 3
        types = {a.type for a in output.alerts}
        assert types == {"signal_c1", "signal_c2", "signal_f1"}

    def test_unscored_pillar_suppresses_its_signals(self):
        """Signals inside an unscored pillar (pillar.score is None) do not
        fire — belt-and-suspenders for the Quality-deferred pattern."""
        output = _output([
            _pillar("funnel",
                    [_signal("F1", 20.0, StatusBand.ACTION)],
                    score=None, status=None),
            _pillar("acquisition",
                    [_signal("C1", 25.0, StatusBand.ACTION)]),
        ])
        populate_signal_alerts(output)

        # Only C1 should fire; F1 is in an unscored pillar.
        assert len(output.alerts) == 1
        assert output.alerts[0].type == "signal_c1"


# ── Regression alert tests ──────────────────────────────────────────


class TestRegressionAlert:
    def test_transition_watch_to_action_fires(self):
        output = _output(
            pillars=[
                _pillar("funnel", [
                    _signal("F1", 20.0, StatusBand.ACTION,
                            diagnostic="CTR collapse"),
                    _signal("F4", 30.0, StatusBand.ACTION,
                            diagnostic="Form completion dropped"),
                ]),
            ],
            health_score=34.0,
            health_status=StatusBand.ACTION,
        )
        alert = build_regression_alert(
            output, prev_status=StatusBand.WATCH, prev_score=52.0
        )
        assert alert is not None
        assert alert.type == "health_regression"
        assert alert.severity == AlertSeverity.CRITICAL
        # Prior state referenced
        assert "52" in alert.message
        assert "WATCH" in alert.message
        # New state referenced
        assert "34" in alert.message
        assert "ACTION" in alert.message
        # Top failing signals included (top-2)
        assert "F1" in alert.message
        assert "F4" in alert.message

    def test_transition_strong_to_action_fires(self):
        output = _output(
            pillars=[_pillar("funnel", [
                _signal("F1", 10.0, StatusBand.ACTION, diagnostic="d"),
            ])],
            health_score=30.0,
            health_status=StatusBand.ACTION,
        )
        alert = build_regression_alert(
            output, prev_status=StatusBand.STRONG, prev_score=75.0
        )
        assert alert is not None

    def test_action_to_action_does_not_fire(self):
        output = _output(
            pillars=[_pillar("funnel", [
                _signal("F1", 10.0, StatusBand.ACTION, diagnostic="d"),
            ])],
            health_score=25.0,
            health_status=StatusBand.ACTION,
        )
        alert = build_regression_alert(
            output, prev_status=StatusBand.ACTION, prev_score=30.0
        )
        assert alert is None

    def test_first_ever_evaluation_does_not_fire(self):
        """Prior status None (no history) should not fire — first
        evaluation is not a regression. Per docs/diagnostics/alert-rules.md
        §"Does not fire when", a dedicated 'launch' alert is a later build.
        """
        output = _output(
            pillars=[_pillar("funnel", [
                _signal("F1", 10.0, StatusBand.ACTION, diagnostic="d"),
            ])],
            health_score=30.0,
            health_status=StatusBand.ACTION,
        )
        alert = build_regression_alert(output, prev_status=None, prev_score=None)
        assert alert is None

    def test_non_action_current_does_not_fire(self):
        """A WATCH or STRONG current score does not fire a regression alert
        regardless of prior state."""
        output = _output(
            pillars=[_pillar("funnel", [
                _signal("F1", 60.0, StatusBand.WATCH, diagnostic="d"),
            ])],
            health_score=55.0,
            health_status=StatusBand.WATCH,
        )
        assert build_regression_alert(
            output, prev_status=StatusBand.ACTION, prev_score=30.0
        ) is None

    def test_top_failing_signals_are_lowest_two(self):
        output = _output(
            pillars=[
                _pillar("acquisition", [
                    _signal("C1", 20.0, StatusBand.ACTION, diagnostic="a"),
                    _signal("C2", 35.0, StatusBand.ACTION, diagnostic="b"),
                    _signal("C3", 75.0, StatusBand.STRONG, diagnostic="c"),
                ]),
                _pillar("funnel", [
                    _signal("F1", 10.0, StatusBand.ACTION, diagnostic="d"),
                    _signal("F2", 45.0, StatusBand.WATCH, diagnostic="e"),
                ]),
            ],
            health_score=30.0,
            health_status=StatusBand.ACTION,
        )
        alert = build_regression_alert(
            output, prev_status=StatusBand.WATCH, prev_score=55.0
        )
        assert alert is not None
        # Lowest two scores: F1 (10), C1 (20). C2, F2, C3 must NOT be
        # in the top-2 list.
        msg = alert.message
        assert "F1" in msg
        assert "C1" in msg
        # Score 35 or 45 would signal C2/F2 snuck into the top-2.
        # Check the "Top failing signals:" block specifically:
        top_block = msg.split("Top failing signals:")[1].split("Flight day")[0]
        assert "C2" not in top_block
        assert "F2" not in top_block


# ── Engine integration: regression via prior-health query ──────────


class TestEngineRegressionIntegration:
    def test_populate_regression_alert_fires_on_transition(self):
        from backend.services.diagnostics import engine

        output = _output(
            pillars=[_pillar("funnel", [
                _signal("F1", 15.0, StatusBand.ACTION, diagnostic="d"),
            ])],
            health_score=25.0,
            health_status=StatusBand.ACTION,
        )

        with patch.object(
            engine, "_query_prior_health",
            return_value=(StatusBand.WATCH, 52.0),
        ):
            engine._populate_regression_alert(output)

        assert any(a.type == "health_regression" for a in output.alerts)

    def test_populate_regression_alert_silent_on_action_to_action(self):
        from backend.services.diagnostics import engine

        output = _output(
            pillars=[_pillar("funnel", [
                _signal("F1", 15.0, StatusBand.ACTION, diagnostic="d"),
            ])],
            health_score=25.0,
            health_status=StatusBand.ACTION,
        )

        with patch.object(
            engine, "_query_prior_health",
            return_value=(StatusBand.ACTION, 30.0),
        ):
            engine._populate_regression_alert(output)

        assert not any(a.type == "health_regression" for a in output.alerts)

    def test_populate_regression_swallows_query_failure(self):
        """If the prior-health query throws, we log and skip — a missing
        regression alert shouldn't blow up the pipeline."""
        from backend.services.diagnostics import engine

        output = _output(
            pillars=[_pillar("funnel", [
                _signal("F1", 15.0, StatusBand.ACTION, diagnostic="d"),
            ])],
            health_score=25.0,
            health_status=StatusBand.ACTION,
        )

        with patch.object(
            engine, "_query_prior_health",
            side_effect=RuntimeError("BQ unreachable"),
        ):
            # Must not raise
            engine._populate_regression_alert(output)

        assert not any(a.type == "health_regression" for a in output.alerts)

    def test_populate_regression_skipped_when_not_action(self):
        from backend.services.diagnostics import engine

        output = _output(
            pillars=[_pillar("funnel", [
                _signal("F1", 80.0, StatusBand.STRONG, diagnostic="d"),
            ])],
            health_score=80.0,
            health_status=StatusBand.STRONG,
        )

        # Should not even query prior health
        mock_q = MagicMock()
        with patch.object(engine, "_query_prior_health", mock_q):
            engine._populate_regression_alert(output)

        mock_q.assert_not_called()
        assert output.alerts == []


# ── Dedup tests ─────────────────────────────────────────────────────


class TestDedup:
    def _record(self, project_code="26009", alert_type="diagnostic_signal_f1",
                severity="critical") -> dict:
        return {
            "alert_id": f"diag-{project_code}-{alert_type}",
            "project_code": project_code,
            "alert_type": alert_type,
            "severity": severity,
            "title": "t",
            "message": "m",
            "metric_value": None,
            "threshold_value": None,
            "is_resolved": False,
            "created_at": "2026-04-17T10:00:00Z",
        }

    def test_no_existing_alerts_returns_all(self):
        from backend.services.diagnostics import engine

        records = [self._record(), self._record(alert_type="diagnostic_signal_c2")]
        with patch.object(engine.bq, "run_query", return_value=[]), \
             patch.object(engine.bq, "string_param", return_value=MagicMock()), \
             patch.object(engine.bq, "table", side_effect=lambda n: f"`dataset.{n}`"):
            result = engine._deduplicate_diagnostic_alerts(records)

        assert len(result) == 2

    def test_duplicate_in_last_24h_is_suppressed(self):
        from backend.services.diagnostics import engine

        records = [
            self._record(alert_type="diagnostic_signal_f1"),
            self._record(alert_type="diagnostic_signal_c2"),
        ]
        existing = [{
            "project_code": "26009",
            "alert_type": "diagnostic_signal_f1",
            "severity": "critical",
        }]
        with patch.object(engine.bq, "run_query", return_value=existing), \
             patch.object(engine.bq, "string_param", return_value=MagicMock()), \
             patch.object(engine.bq, "table", side_effect=lambda n: f"`dataset.{n}`"):
            result = engine._deduplicate_diagnostic_alerts(records)

        # Only c2 should survive — f1 was already fired in the last 24h.
        assert len(result) == 1
        assert result[0]["alert_type"] == "diagnostic_signal_c2"

    def test_dedup_key_includes_severity(self):
        """Same project+type but different severity are distinct alerts."""
        from backend.services.diagnostics import engine

        records = [self._record(severity="warning")]
        existing = [{
            "project_code": "26009",
            "alert_type": "diagnostic_signal_f1",
            "severity": "critical",  # different severity
        }]
        with patch.object(engine.bq, "run_query", return_value=existing), \
             patch.object(engine.bq, "string_param", return_value=MagicMock()), \
             patch.object(engine.bq, "table", side_effect=lambda n: f"`dataset.{n}`"):
            result = engine._deduplicate_diagnostic_alerts(records)

        # Warning-severity record should NOT match critical existing row.
        assert len(result) == 1

    def test_dedup_query_failure_falls_back_to_no_dedup(self):
        """If the dedup query errors (e.g. fresh table, schema gap), we
        insert everything rather than drop alerts on the floor."""
        from backend.services.diagnostics import engine

        records = [self._record(), self._record(alert_type="diagnostic_signal_c2")]
        with patch.object(engine.bq, "run_query",
                          side_effect=RuntimeError("bad schema")), \
             patch.object(engine.bq, "string_param", return_value=MagicMock()), \
             patch.object(engine.bq, "table", side_effect=lambda n: f"`dataset.{n}`"):
            result = engine._deduplicate_diagnostic_alerts(records)

        assert len(result) == 2

    def test_empty_records_short_circuits(self):
        from backend.services.diagnostics import engine
        assert engine._deduplicate_diagnostic_alerts([]) == []


# ── Title formatting ────────────────────────────────────────────────


class TestAlertTitleFormatting:
    def test_health_regression_title(self):
        from backend.services.diagnostics import engine
        from backend.services.diagnostics.models import DiagnosticAlert

        output = _output(
            pillars=[],
            health_score=34.0,
            health_status=StatusBand.ACTION,
            project_code="26009",
            campaign_type=CampaignType.CONVERSION,
        )
        alert = DiagnosticAlert(
            type="health_regression",
            severity=AlertSeverity.CRITICAL,
            message="",
        )
        title = engine._alert_title(output, alert)
        assert title == "26009 [conversion] \u00b7 Health dropped to ACTION (34)"

    def test_signal_title_uses_signal_name_and_score(self):
        from backend.services.diagnostics import engine
        from backend.services.diagnostics.models import DiagnosticAlert

        output = _output(
            pillars=[_pillar("funnel", [
                _signal("F1", 22.0, StatusBand.ACTION,
                        name="Click-to-Landing-Page"),
            ])],
            health_score=40.0,
            health_status=StatusBand.ACTION,
        )
        alert = DiagnosticAlert(
            type="signal_f1",
            severity=AlertSeverity.CRITICAL,
            message="",
            signal_id="F1",
        )
        title = engine._alert_title(output, alert)
        assert "F1" in title
        assert "Click-to-Landing-Page" in title
        assert "ACTION (22)" in title
