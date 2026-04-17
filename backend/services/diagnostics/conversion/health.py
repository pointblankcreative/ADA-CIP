"""Conversion campaign health score rollup.

Assembles all pillar scores into the final health score with
flight-stage adjustment (early flight dampening, late flight urgency).

Scored pillars:
    Acquisition (C1-C3) — "Are we buying leads efficiently?"
    Funnel      (F1-F5) — "Does the path from ad to submit work?"

A Quality (Q1-Q3) pillar was originally scoped to answer "are the
leads we got worth anything?" but has been deferred indefinitely. Any
Quality score built from proxies (GA4 key_events, platform lead form
counts, etc.) would be dishonest without real CRM disposition data,
which PB's clients don't consistently expose. See
docs/diagnostics/quality-pillar-deferred.md for the full reasoning
and the data requirements that would unblock building it.

Quality's original 0.30 weight has been redistributed proportionally
between the two scored pillars:

Pillar weights (conversion):
    Acquisition: 0.43   (was 0.30, share of 0.70 active = 0.4286)
    Funnel:      0.57   (was 0.40, share of 0.70 active = 0.5714)
"""

from __future__ import annotations

from backend.services.diagnostics.models import (
    CampaignData,
    CampaignType,
    DiagnosticOutput,
    EfficiencyMetrics,
    status_band,
)
from backend.services.diagnostics.shared.alerts import populate_signal_alerts
from backend.services.diagnostics.shared.normalization import clamp, safe_div

from backend.services.diagnostics.conversion.acquisition import (
    compute_acquisition_pillar,
)
from backend.services.diagnostics.conversion.funnel import (
    compute_funnel_pillar,
)


def compute_conversion_health(data: CampaignData) -> DiagnosticOutput:
    """Full conversion diagnostic: pillars → health score → output.

    Runs Acquisition (C1-C3) and Funnel (F1-F5). A Quality pillar is
    not emitted — see module docstring for deferral reasoning.
    """
    # Compute pillars
    acquisition = compute_acquisition_pillar(data)
    funnel = compute_funnel_pillar(data)

    pillars = [acquisition, funnel]

    # Build output
    output = DiagnosticOutput(
        project_code=data.project_code,
        campaign_type=CampaignType.CONVERSION,
        evaluation_date=data.flight.evaluation_date,
        flight_day=data.flight.elapsed_days,
        flight_total_days=data.flight.total_days,
        pillars=pillars,
        platforms=[p.platform_id for p in data.platform_metrics],
        line_ids=[l.line_id for l in data.media_plan],
    )

    # Compute health score (weighted average of scored pillars —
    # unscorable pillars are excluded automatically)
    output.compute_health_score()

    # Apply flight-stage adjustment
    if output.health_score is not None:
        output.health_score = _flight_stage_adjust(
            output.health_score,
            data.flight.elapsed_fraction,
        )
        output.health_status = status_band(output.health_score)

    # Compute efficiency metrics
    output.efficiency = _compute_efficiency(data)

    # Populate signal-level ACTION alerts. Health-regression alerts
    # are added later in the engine after querying the prior
    # evaluation — see docs/diagnostics/alert-rules.md.
    populate_signal_alerts(output)

    return output


def _flight_stage_adjust(score: float, flight_progress: float) -> float:
    """Adjust health score based on flight stage.

    Same logic as persuasion — early dampening, late urgency.
    Conversion campaigns may be shorter, so the dampening window
    matters even more.
    """
    if flight_progress < 0.2:
        dampening = 1 - (flight_progress / 0.2)
        adjusted = score * (1 - dampening * 0.4) + 60 * (dampening * 0.4)
        return round(clamp(adjusted, 0, 100), 1)

    if flight_progress <= 0.8:
        return round(score, 1)

    # Late flight: push concerning scores lower
    urgency = (flight_progress - 0.8) / 0.2
    if score < 65:
        adjusted = score - (urgency * 10)
        return round(clamp(adjusted, 0, 100), 1)

    return round(score, 1)


def _compute_efficiency(data: CampaignData) -> EfficiencyMetrics:
    """Compute cost-efficiency metrics (Level 4 — context, not scored)."""
    spend = data.total_spend
    impressions = data.total_impressions
    clicks = data.total_clicks
    conversions = data.total_conversions
    completed_views = data.total_thruplay

    return EfficiencyMetrics(
        cpm=round(safe_div(spend, impressions / 1000, default=0), 2)
            if impressions > 0 else None,
        cpc=round(safe_div(spend, clicks), 2)
            if clicks > 0 else None,
        cpa=round(safe_div(spend, conversions), 2)
            if conversions > 0 else None,
        cpcv=round(safe_div(spend, completed_views), 2)
            if completed_views > 0 else None,
        pacing_pct=data.budget_pacing_pct,
    )
