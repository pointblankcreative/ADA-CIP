"""Shared alert-generation helpers for diagnostic health modules.

Populates `DiagnosticOutput.alerts` per the rules in
`docs/diagnostics/alert-rules.md`. Both persuasion/health.py and
conversion/health.py call `populate_signal_alerts(output)` near the
end of their flow to emit per-signal ACTION-band alerts.

Health-regression (state-transition) alerts require a DB lookup of
the prior evaluation and are emitted later in the engine, not here
— see `engine._populate_regression_alert`.
"""

from __future__ import annotations

from backend.services.diagnostics.models import (
    AlertSeverity,
    DiagnosticAlert,
    DiagnosticOutput,
    SignalResult,
    StatusBand,
)


def populate_signal_alerts(output: DiagnosticOutput) -> None:
    """Append one DiagnosticAlert per signal in ACTION band.

    A signal fires when:
      - It's in a scored pillar (pillar.score is not None)
      - guard_passed is True (we measured it cleanly)
      - status == StatusBand.ACTION (score < 40)

    See docs/diagnostics/alert-rules.md §"Signal-level ACTION".
    """
    for pillar in output.pillars:
        # Skip unscored pillars. Quality (when deferred) will have
        # no pillar object at all, but belt-and-suspenders.
        if pillar.score is None:
            continue
        for signal in pillar.signals:
            if _is_action_level(signal):
                output.alerts.append(_build_signal_alert(signal, output))


def build_regression_alert(
    output: DiagnosticOutput,
    prev_status: StatusBand | None,
    prev_score: float | None,
) -> DiagnosticAlert | None:
    """Return a health-regression alert if the campaign just entered ACTION.

    Fires only when:
      - output.health_status == ACTION
      - A prior evaluation exists (prev_status is not None) AND
      - Prior evaluation was not already ACTION

    Suppressed cases:
      - ACTION → ACTION: dashboards carry standing state, no re-page.
      - No prior evaluation (prev_status is None): campaign's first run;
        a distinct "launch" alert is deferred to a later release.
        See docs/diagnostics/alert-rules.md §"Does not fire when".
    """
    if output.health_status != StatusBand.ACTION:
        return None
    if prev_status is None:
        return None
    if prev_status == StatusBand.ACTION:
        return None

    # Top 2 failing signals (lowest score, guard_passed, from scored pillars)
    failing = _top_failing_signals(output, n=2)

    lines = [
        f"Health score dropped from "
        f"{_fmt_score(prev_score)} ({_fmt_status(prev_status)}) to "
        f"{_fmt_score(output.health_score)} (ACTION)."
    ]
    if failing:
        lines.append("")
        lines.append("Top failing signals:")
        for s in failing:
            lines.append(
                f" \u2022 {s.id} {s.name} \u2014 "
                f"{_fmt_score(s.score)} (ACTION) \u2014 {s.diagnostic}"
            )
    lines.append("")
    lines.append(
        f"Flight day {output.flight_day} of {output.flight_total_days}. "
        f"Review on dashboard."
    )

    return DiagnosticAlert(
        type="health_regression",
        severity=AlertSeverity.CRITICAL,
        message="\n".join(lines),
        signal_id=None,
    )


# ── Internals ───────────────────────────────────────────────────────


def _is_action_level(signal: SignalResult) -> bool:
    return (
        signal.guard_passed
        and signal.status == StatusBand.ACTION
        and signal.score is not None
    )


def _build_signal_alert(
    signal: SignalResult, output: DiagnosticOutput
) -> DiagnosticAlert:
    body_lines = [
        f"{signal.name} ({signal.id}) scored "
        f"{_fmt_score(signal.score)} (ACTION).",
    ]
    if signal.diagnostic:
        body_lines.append("")
        body_lines.append(signal.diagnostic)
    body_lines.append("")
    body_lines.append(
        f"Flight day {output.flight_day} of {output.flight_total_days}."
    )

    # alert.type becomes alert_type = "diagnostic_signal_f1" etc.
    # Lowercased for consistency with other alert_type values in the
    # alerts table (e.g. "data_stale", "pacing_over").
    return DiagnosticAlert(
        type=f"signal_{signal.id.lower()}",
        severity=AlertSeverity.CRITICAL,
        message="\n".join(body_lines),
        signal_id=signal.id,
    )


def _top_failing_signals(
    output: DiagnosticOutput, n: int = 2
) -> list[SignalResult]:
    """Lowest-scoring signals from scored pillars where guard passed."""
    candidates: list[SignalResult] = []
    for pillar in output.pillars:
        if pillar.score is None:
            continue
        for signal in pillar.signals:
            if signal.guard_passed and signal.score is not None:
                candidates.append(signal)
    # Stable sort: lowest score first. Signals that guard-failed are
    # already excluded.
    candidates.sort(key=lambda s: s.score)  # type: ignore[arg-type,return-value]
    return candidates[:n]


def _fmt_score(score: float | None) -> str:
    if score is None:
        return "—"
    # health/score values are rounded to .1 upstream; format consistently
    if abs(score - round(score)) < 0.05:
        return f"{int(round(score))}"
    return f"{score:.1f}"


def _fmt_status(status: StatusBand | None) -> str:
    return status.value if status else "n/a"


__all__ = [
    "populate_signal_alerts",
    "build_regression_alert",
]
