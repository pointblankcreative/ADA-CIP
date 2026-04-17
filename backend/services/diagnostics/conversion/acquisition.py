"""Acquisition Efficiency pillar signals (C1–C3) for conversion campaigns.

"What are we paying and is it sustainable?"

C1: CPA vs Friction-Adjusted Target — core conversion KPI
C2: Conversion Volume Trajectory — are leads flowing at budget-derived pace?
C3: CPA Trend (Deterioration Detection) — is CPA getting worse over time?

Data sources: fact_digital_daily (spend, conversions, clicks),
media_plan_lines (budget, ffs_score, audience_type).
"""

from __future__ import annotations

import math
import statistics
from typing import Any

from backend.services.diagnostics.models import (
    CampaignData,
    PillarScore,
    SignalResult,
    StatusBand,
    status_band,
)
from backend.services.diagnostics.shared.audience_temp import (
    get_temperature_multiplier,
)
from backend.services.diagnostics.shared.benchmarks import (
    ACQUISITION_SIGNAL_WEIGHTS,
)
from backend.services.diagnostics.shared.guards import (
    check_min_conversions,
    check_min_days,
    check_min_spend,
)
from backend.services.diagnostics.shared.normalization import (
    clamp,
    safe_div,
)


# ── Constants ──────────────────────────────────────────────────────

# C2: volume ratio thresholds
C2_VOLUME_CAP = 90       # max score from volume ratio alone
C2_TREND_MAX_BONUS = 10  # ±10 points from trend

# C3: CPA trend thresholds (daily % change)
C3_THRESHOLD_STABLE = 1.0
C3_THRESHOLD_EARLY = 3.0
C3_THRESHOLD_MODERATE = 6.0

C3_TREND_SCORES = {
    "STABLE_OR_IMPROVING": 90,
    "EARLY_DETERIORATION": 65,
    "MODERATE_DETERIORATION": 40,
    "RAPID_DETERIORATION": 15,
}

# Learning phase: dampen scores when data is too thin
LEARNING_PHASE_MIN_CONVERSIONS = 50
LEARNING_PHASE_MIN_DAYS = 10
LEARNING_PHASE_FLOOR = 55  # Floor C3 at WATCH level during learning

# Minimum days of data for trend signals (C2, C3)
MIN_DAYS_FOR_TREND = 3


# ── Diagnostic message templates ──────────────────────────────────

C1_MESSAGES = {
    StatusBand.STRONG: (
        "CPA at ${cpa:.2f} vs ${target:.2f} friction-adjusted target — "
        "beating target by {beat_pct}."
    ),
    StatusBand.WATCH: (
        "CPA at ${cpa:.2f} — {above_pct} above the ${target:.2f} "
        "friction-adjusted target. {context}"
    ),
    StatusBand.ACTION: (
        "CPA at ${cpa:.2f} — well above the ${target:.2f} target "
        "even after friction adjustment. {context}"
    ),
}

C2_MESSAGES = {
    StatusBand.STRONG: (
        "Averaging {daily_avg:.1f} leads/day against a friction-adjusted "
        "expectation of {expected:.1f}. {trend_msg}"
    ),
    StatusBand.WATCH: (
        "Volume at {daily_avg:.1f} leads/day vs {expected:.1f} expected "
        "from budget allocation. {trend_msg}"
    ),
    StatusBand.ACTION: (
        "Volume at {daily_avg:.1f} leads/day — significantly below the "
        "{expected:.1f}/day expectation. {trend_msg}"
    ),
}

C3_MESSAGES = {
    "STABLE_OR_IMPROVING": (
        "CPA trend is stable or improving ({change:+.1f}%/day over the "
        "last 7 days). No signs of audience saturation."
    ),
    "EARLY_DETERIORATION": (
        "CPA is increasing slightly ({change:+.1f}%/day). Early sign of "
        "audience fatigue — monitor over the next 3-5 days."
    ),
    "MODERATE_DETERIORATION": (
        "CPA is deteriorating ({change:+.1f}%/day). Likely audience "
        "saturation — consider expanding targeting or refreshing creative."
    ),
    "RAPID_DETERIORATION": (
        "CPA is spiking ({change:+.1f}%/day). Audience is saturated. "
        "Immediate action needed — expand targeting, introduce new "
        "creative, or reallocate budget."
    ),
}


# ── Signal implementations ─────────────────────────────────────────


def _guard_fail(signal_id: str, name: str, reason: str, message: str) -> SignalResult:
    """Helper to return a guard-failed SignalResult."""
    return SignalResult(
        id=signal_id,
        name=name,
        score=None,
        status=None,
        diagnostic=message,
        guard_passed=False,
        guard_reason=reason,
    )


def _determine_target_cpa(data: CampaignData) -> float | None:
    """Derive the CPA benchmark from media plan or FFS.

    Priority:
        1. Explicit target_cpa on media plan (not yet stored — future)
        2. FFS-derived: expected_cvr from friction model × audience temp
        3. None if insufficient data
    """
    # Get average FFS from media plan lines
    ffs_scores = [l.ffs_score for l in data.media_plan if l.ffs_score is not None]
    avg_ffs = statistics.mean(ffs_scores) if ffs_scores else None

    # Get dominant audience type (most budget)
    audience_budgets: dict[str | None, float] = {}
    for line in data.media_plan:
        at = line.audience_type.value if line.audience_type else None
        audience_budgets[at] = audience_budgets.get(at, 0) + line.planned_budget
    dominant_audience = max(audience_budgets, key=audience_budgets.get) if audience_budgets else None

    # Derive from FFS model
    if avg_ffs is not None:
        # Spec formula: expected_cvr = 25 * exp(-0.045 * ffs) / 100
        base_cvr = 25 * math.exp(-0.045 * avg_ffs) / 100
        adjusted_cvr = base_cvr * get_temperature_multiplier(dominant_audience)

        # Estimate CPC from actual data
        total_clicks = data.total_clicks
        total_spend = data.total_spend
        if total_clicks > 0 and total_spend > 0:
            estimated_cpc = total_spend / total_clicks
            if adjusted_cvr > 0:
                return estimated_cpc / adjusted_cvr

    # Fallback: derive from actual data if we have enough
    if data.total_conversions >= 5 and data.total_clicks > 0:
        actual_cvr = data.total_conversions / data.total_clicks
        estimated_cpc = data.total_spend / data.total_clicks
        # Use 120% of actual CPA as a generous target
        return (estimated_cpc / actual_cvr) * 1.2 if actual_cvr > 0 else None

    return None


def compute_c1_cpa_vs_target(data: CampaignData) -> SignalResult:
    """C1: CPA vs Friction-Adjusted Target.

    The core conversion KPI — what are we paying per lead, benchmarked
    against what we *should* be paying given form friction and audience warmth.
    """
    # Guard checks
    passed, reason = check_min_conversions(data)
    if not passed:
        return _guard_fail("C1", "CPA vs Target", reason,
                           f"Insufficient conversions — need at least {5}.")

    passed, reason = check_min_spend(data)
    if not passed:
        return _guard_fail("C1", "CPA vs Target", reason,
                           "Insufficient spend data.")

    # Compute actual CPA
    actual_cpa = data.total_spend / data.total_conversions

    # Determine target CPA
    target_cpa = _determine_target_cpa(data)
    if target_cpa is None or target_cpa <= 0:
        return _guard_fail("C1", "CPA vs Target", "no_target_cpa",
                           "Could not derive CPA target — no FFS data or insufficient click data.")

    # CPA efficiency ratio: >1.0 = beating target (good)
    cpa_efficiency = target_cpa / actual_cpa

    # Score using spec formula
    if cpa_efficiency >= 1.0:
        score = 75 + min((cpa_efficiency - 1.0) * 50, 25)
    elif cpa_efficiency >= 0.7:
        score = 50 + ((cpa_efficiency - 0.7) / 0.3) * 25
    elif cpa_efficiency >= 0.4:
        score = 25 + ((cpa_efficiency - 0.4) / 0.3) * 25
    else:
        score = max(cpa_efficiency / 0.4 * 25, 0)

    score = clamp(score, 0, 100)
    status = status_band(score)

    # Diagnostic message
    if status == StatusBand.STRONG:
        beat_pct = f"{(cpa_efficiency - 1) * 100:.0f}%"
        diagnostic = C1_MESSAGES[StatusBand.STRONG].format(
            cpa=actual_cpa, target=target_cpa, beat_pct=beat_pct,
        )
    elif status == StatusBand.WATCH:
        above_pct = f"{(1 - cpa_efficiency) * 100:.0f}%"
        ffs_scores = [l.ffs_score for l in data.media_plan if l.ffs_score is not None]
        avg_ffs = statistics.mean(ffs_scores) if ffs_scores else 0
        context = (
            f"Form friction (FFS {avg_ffs:.0f}) accounts for some of the gap."
            if avg_ffs > 30
            else "Review landing page and ad-to-LP alignment."
        )
        diagnostic = C1_MESSAGES[StatusBand.WATCH].format(
            cpa=actual_cpa, target=target_cpa, above_pct=above_pct,
            context=context,
        )
    else:
        context = "Review LP messaging, targeting breadth, and creative relevance."
        diagnostic = C1_MESSAGES[StatusBand.ACTION].format(
            cpa=actual_cpa, target=target_cpa, context=context,
        )

    return SignalResult(
        id="C1",
        name="CPA vs Target",
        score=round(score, 1),
        status=status,
        raw_value=round(cpa_efficiency, 3),
        benchmark=1.0,
        floor=0.4,
        diagnostic=diagnostic,
        guard_passed=True,
        inputs={
            "actual_cpa": round(actual_cpa, 2),
            "target_cpa": round(target_cpa, 2),
            "cpa_efficiency": round(cpa_efficiency, 3),
            "spend": round(data.total_spend, 2),
            "conversions": data.total_conversions,
            "total_clicks": data.total_clicks,
        },
    )


def compute_c2_volume_trajectory(data: CampaignData) -> SignalResult:
    """C2: Conversion Volume Trajectory.

    Derives expected daily volume from budget and friction-adjusted CPA,
    then measures whether actual volume is on track. Also monitors
    the volume trend — declining vs ramping.
    """
    # Guard: need enough data for rolling average
    passed, reason = check_min_days(data.flight, min_days=MIN_DAYS_FOR_TREND)
    if not passed:
        return _guard_fail("C2", "Volume Trajectory", reason,
                           f"Need at least {MIN_DAYS_FOR_TREND} days of data for volume trend.")

    passed, reason = check_min_conversions(data)
    if not passed:
        return _guard_fail("C2", "Volume Trajectory", reason,
                           "Insufficient conversions.")

    # Aggregate daily conversions (sum across platforms per day)
    daily_convs: dict[str, float] = {}
    for dm in data.daily_metrics:
        day_key = str(dm.date)
        daily_convs[day_key] = daily_convs.get(day_key, 0) + dm.conversions

    if len(daily_convs) < MIN_DAYS_FOR_TREND:
        return _guard_fail("C2", "Volume Trajectory", "insufficient_daily_data",
                           f"Need at least {MIN_DAYS_FOR_TREND} days with data.")

    # 7-day rolling average (or all days if < 7)
    sorted_days = sorted(daily_convs.values())
    recent = list(daily_convs.values())[-7:]
    rolling_avg = statistics.mean(recent) if recent else 0

    # Derive expected daily volume
    target_cpa = _determine_target_cpa(data)
    total_budget = data.planned_budget
    total_days = data.flight.total_days

    if target_cpa and target_cpa > 0 and total_days > 0:
        daily_budget = total_budget / total_days
        expected_daily = daily_budget / target_cpa
    else:
        # Fallback: use actual campaign-average as expectation
        total_days_with_data = len(daily_convs)
        expected_daily = data.total_conversions / total_days_with_data if total_days_with_data > 0 else 0

    if expected_daily <= 0:
        return _guard_fail("C2", "Volume Trajectory", "zero_expected_volume",
                           "Could not derive expected daily volume.")

    # Volume ratio
    volume_ratio = rolling_avg / expected_daily

    # Volume trend (linear regression slope over recent days)
    recent_values = list(daily_convs.values())[-7:]
    if len(recent_values) >= 3:
        mean_val = statistics.mean(recent_values)
        n = len(recent_values)
        x_mean = (n - 1) / 2
        numerator = sum((i - x_mean) * (v - mean_val) for i, v in enumerate(recent_values))
        denominator = sum((i - x_mean) ** 2 for i in range(n))
        slope = numerator / denominator if denominator > 0 else 0
        daily_volume_change = (slope / mean_val * 100) if mean_val > 0 else 0
    else:
        daily_volume_change = 0

    # Score: volume ratio dominates, trend provides bonus/penalty
    volume_score = clamp(volume_ratio * 75, 0, C2_VOLUME_CAP)
    trend_bonus = clamp(daily_volume_change * 2, -C2_TREND_MAX_BONUS, C2_TREND_MAX_BONUS)
    score = clamp(volume_score + trend_bonus, 0, 100)

    status = status_band(score)

    # Trend message
    if daily_volume_change > 2:
        trend_msg = f"Volume trending upward (+{daily_volume_change:.1f}%/day)."
    elif daily_volume_change < -5:
        trend_msg = f"Volume declining ({daily_volume_change:+.1f}%/day) — investigate."
    else:
        trend_msg = "Volume trend is stable."

    msg_template = C2_MESSAGES.get(status, C2_MESSAGES[StatusBand.WATCH])
    diagnostic = msg_template.format(
        daily_avg=rolling_avg,
        expected=expected_daily,
        trend_msg=trend_msg,
    )

    return SignalResult(
        id="C2",
        name="Volume Trajectory",
        score=round(score, 1),
        status=status,
        raw_value=round(volume_ratio, 3),
        benchmark=1.0,
        floor=0.5,
        diagnostic=diagnostic,
        guard_passed=True,
        inputs={
            "rolling_avg_daily": round(rolling_avg, 2),
            "expected_daily": round(expected_daily, 2),
            "volume_ratio": round(volume_ratio, 3),
            "daily_volume_change_pct": round(daily_volume_change, 2),
            "days_with_data": len(daily_convs),
        },
    )


def compute_c3_cpa_trend(data: CampaignData) -> SignalResult:
    """C3: CPA Trend (Deterioration Detection).

    Is CPA getting worse over time? A campaign that starts strong but
    sees CPA creep up signals audience saturation. This is the conversion
    equivalent of creative fatigue.
    """
    # Guard
    passed, reason = check_min_days(data.flight, min_days=MIN_DAYS_FOR_TREND)
    if not passed:
        return _guard_fail("C3", "CPA Trend", reason,
                           f"Need at least {MIN_DAYS_FOR_TREND} days for trend detection.")

    passed, reason = check_min_conversions(data)
    if not passed:
        return _guard_fail("C3", "CPA Trend", reason,
                           "Insufficient conversions.")

    # Build daily CPA series (aggregate spend/conversions per day)
    daily_spend: dict[str, float] = {}
    daily_conv: dict[str, float] = {}
    for dm in data.daily_metrics:
        day_key = str(dm.date)
        daily_spend[day_key] = daily_spend.get(day_key, 0) + dm.spend
        daily_conv[day_key] = daily_conv.get(day_key, 0) + dm.conversions

    # Only include days with conversions > 0 (CPA undefined otherwise)
    daily_cpa_values = []
    for day_key in sorted(daily_spend.keys()):
        conv = daily_conv.get(day_key, 0)
        spend = daily_spend.get(day_key, 0)
        if conv > 0 and spend > 0:
            daily_cpa_values.append(spend / conv)

    # Take last 7 days with valid CPA
    recent_cpa = daily_cpa_values[-7:]

    if len(recent_cpa) < MIN_DAYS_FOR_TREND:
        return _guard_fail("C3", "CPA Trend", "insufficient_cpa_data",
                           f"Need at least {MIN_DAYS_FOR_TREND} days with conversions for trend.")

    # Linear regression slope on recent CPA
    mean_cpa = statistics.mean(recent_cpa)
    n = len(recent_cpa)
    x_mean = (n - 1) / 2
    numerator = sum((i - x_mean) * (v - mean_cpa) for i, v in enumerate(recent_cpa))
    denominator = sum((i - x_mean) ** 2 for i in range(n))
    slope = numerator / denominator if denominator > 0 else 0
    daily_change_pct = (slope / mean_cpa * 100) if mean_cpa > 0 else 0

    # Classify trend
    if daily_change_pct < C3_THRESHOLD_STABLE:
        trend = "STABLE_OR_IMPROVING"
    elif daily_change_pct < C3_THRESHOLD_EARLY:
        trend = "EARLY_DETERIORATION"
    elif daily_change_pct < C3_THRESHOLD_MODERATE:
        trend = "MODERATE_DETERIORATION"
    else:
        trend = "RAPID_DETERIORATION"

    score = float(C3_TREND_SCORES[trend])

    # Learning phase guard: floor at WATCH level
    is_learning = (
        data.total_conversions < LEARNING_PHASE_MIN_CONVERSIONS
        and data.flight.elapsed_days < LEARNING_PHASE_MIN_DAYS
    )
    if is_learning:
        score = max(score, LEARNING_PHASE_FLOOR)

    status = status_band(score)
    diagnostic = C3_MESSAGES[trend].format(change=daily_change_pct)

    return SignalResult(
        id="C3",
        name="CPA Trend",
        score=round(score, 1),
        status=status,
        raw_value=round(daily_change_pct, 2),
        benchmark=0.0,   # Ideal: 0% change
        floor=C3_THRESHOLD_MODERATE,
        diagnostic=diagnostic,
        guard_passed=True,
        inputs={
            "daily_change_pct": round(daily_change_pct, 2),
            "trend": trend,
            "mean_cpa": round(mean_cpa, 2),
            "slope": round(slope, 4),
            "data_points": n,
            "is_learning_phase": is_learning,
        },
    )


# ── Pillar assembly ────────────────────────────────────────────────


def compute_acquisition_pillar(data: CampaignData) -> PillarScore:
    """Compute all acquisition signals and assemble the pillar score.

    Signal weights: C1=0.45, C2=0.35, C3=0.20
    """
    c1 = compute_c1_cpa_vs_target(data)
    c2 = compute_c2_volume_trajectory(data)
    c3 = compute_c3_cpa_trend(data)

    pillar = PillarScore(
        name="acquisition",
        signals=[c1, c2, c3],
        weight=0.43,  # Conversion pillar weight (Quality deferred; see benchmarks)
    )

    # Weighted average of active signals
    active = [s for s in pillar.signals if s.guard_passed and s.score is not None]
    if active:
        weights = ACQUISITION_SIGNAL_WEIGHTS
        weighted_sum = sum(
            s.score * weights.get(s.id, 0.33) for s in active
        )
        total_weight = sum(
            weights.get(s.id, 0.33) for s in active
        )
        pillar.score = round(weighted_sum / total_weight, 1) if total_weight > 0 else None
        pillar.status = status_band(pillar.score) if pillar.score is not None else None
    else:
        pillar.score = None
        pillar.status = None

    return pillar
