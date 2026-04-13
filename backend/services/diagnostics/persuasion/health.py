"""Persuasion campaign health score rollup.

Assembles all pillar scores into the final health score with
flight-stage adjustment (early flight dampening, late flight urgency).

Pillar weights (persuasion):
    Distribution: 0.35
    Attention:    0.40
    Resonance:    0.25
"""

from __future__ import annotations

from backend.services.diagnostics.models import (
    CampaignData,
    CampaignType,
    DiagnosticOutput,
    EfficiencyMetrics,
    PillarScore,
    status_band,
)
from backend.services.diagnostics.shared.normalization import clamp, safe_div

# Placeholder pillar builders — Attention and Resonance are Phase 2.
# For now they return empty pillars so the engine can run with
# Distribution only.
from backend.services.diagnostics.persuasion.distribution import (
    compute_distribution_pillar,
)


def _placeholder_pillar(name: str, weight: float) -> PillarScore:
    """Return an empty pillar for signals not yet implemented."""
    return PillarScore(name=name, score=None, status=None, weight=weight)


def compute_persuasion_health(data: CampaignData) -> DiagnosticOutput:
    """Full persuasion diagnostic: pillars → health score → output.

    Phase 1: Only Distribution is active.
    Phase 2 will add Attention and Resonance.
    """
    # Compute pillars
    distribution = compute_distribution_pillar(data)
    attention = _placeholder_pillar("attention", weight=0.40)
    resonance = _placeholder_pillar("resonance", weight=0.25)

    pillars = [distribution, attention, resonance]

    # Build output
    output = DiagnosticOutput(
        project_code=data.project_code,
        campaign_type=CampaignType.PERSUASION,
        evaluation_date=data.flight.evaluation_date,
        flight_day=data.flight.elapsed_days,
        flight_total_days=data.flight.total_days,
        pillars=pillars,
        platforms=[p.platform_id for p in data.platform_metrics],
        line_ids=[l.line_id for l in data.media_plan],
    )

    # Compute health score (weighted average of scored pillars)
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

    return output


def _flight_stage_adjust(score: float, flight_progress: float) -> float:
    """Adjust health score based on flight stage.

    Early flight (first 20%): dampen toward 60 (neutral) — data is volatile.
    Mid flight (20-80%): no adjustment.
    Late flight (last 20%): amplify low scores (urgency).
    """
    if flight_progress < 0.2:
        # Pull toward 60 with diminishing dampening
        dampening = 1 - (flight_progress / 0.2)  # 1.0 at day 1, 0.0 at 20%
        adjusted = score * (1 - dampening * 0.4) + 60 * (dampening * 0.4)
        return round(clamp(adjusted, 0, 100), 1)

    if flight_progress <= 0.8:
        return round(score, 1)

    # Late flight: push concerning scores lower
    urgency = (flight_progress - 0.8) / 0.2  # 0.0 at 80%, 1.0 at end
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
