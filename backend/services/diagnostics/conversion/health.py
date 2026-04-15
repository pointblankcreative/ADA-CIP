"""Conversion campaign health score rollup.

Assembles all pillar scores into the final health score with
flight-stage adjustment (early flight dampening, late flight urgency).

Currently only the Acquisition pillar (C1-C3) is implemented.
Funnel (F1-F5) and Quality (Q1-Q3) pillars will be added in
subsequent phases and their weights will redistribute to active
pillars until then.

Pillar weights (conversion):
    Acquisition: 0.30
    Funnel:      0.40
    Quality:     0.30
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

from backend.services.diagnostics.conversion.acquisition import (
    compute_acquisition_pillar,
)


def compute_conversion_health(data: CampaignData) -> DiagnosticOutput:
    """Full conversion diagnostic: pillars → health score → output.

    Phase 1: Only the Acquisition pillar (C1-C3) is active.
    When Funnel and Quality pillars are added, they'll slot in and the
    health score will rebalance automatically via compute_health_score().
    """
    # Compute pillars
    acquisition = compute_acquisition_pillar(data)

    # Future pillars — placeholder PillarScores with no signals
    # so the output structure is consistent and ready for extension.
    funnel = PillarScore(name="funnel", weight=0.40)
    quality = PillarScore(name="quality", weight=0.30)

    pillars = [acquisition, funnel, quality]

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
