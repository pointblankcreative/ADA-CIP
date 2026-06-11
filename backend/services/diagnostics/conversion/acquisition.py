"""Acquisition Efficiency pillar signals (C1–C3) for conversion campaigns.

"What are we paying and is it sustainable?"

C1: CPA vs Friction-Adjusted Target — core conversion KPI
C2: Conversion Volume Trajectory — are leads flowing at budget-derived pace?
C3: CPA Trend (Deterioration Detection) — is CPA getting worse over time?

Calibration (2026-04-20):
  - Removed the 5-conversion gate. Signals now trigger as soon as the
    campaign has been active for a calendar day (12-hour proxy) with
    >$10 of spend. Under-performing campaigns (few conversions, high
    CPA) now surface an ACTION diagnostic rather than being hidden
    behind a guard-fail.
  - ``_determine_target_cpa`` no longer falls back to ``actual_cpa * 1.2``
    when FFS data is missing — that made the signal ungameable and
    silently covered for unfilled FFS wizards. The function now returns
    ``None`` in that case and the calling signal guard-fails with a
    message pointing at the FFS wizard.
  - Audience temperature is budget-weighted across the media plan
    instead of taken from the single dominant-audience line, so mixed
    warm/cold plans don't get a target inflated by the warmest tier.
  - C2 sorts daily-series by date key (not dict insertion order) and
    excludes the evaluation date from the rolling average — the
    evaluation day is almost always a partial day and was systematically
    dragging the average down.
  - C3 now fits a spend-weighted OLS on the last 7 CALENDAR days before
    the evaluation date rather than the last 7 days with valid CPA. The
    old "last 7 with CPA" window skipped over zero-conversion days, which
    masked the very audience-saturation pattern C3 is supposed to catch.

Data sources: fact_digital_daily (spend, conversions, clicks),
media_plan_lines (budget, ffs_score, audience_type).
"""

from __future__ import annotations

import math
import statistics
from datetime import date, timedelta
from typing import Any

from backend.services.diagnostics.models import (
    CampaignData,
    PillarScore,
    SignalResult,
    StatusBand,
    status_band,
)
from backend.services.diagnostics.shared.audience_temp import (
    DEFAULT_MULTIPLIER,
    get_temperature_multiplier,
)
from backend.services.diagnostics.shared.benchmarks import (
    ACQUISITION_SIGNAL_WEIGHTS,
    MIN_PILLAR_COVERAGE,
)
from backend.services.diagnostics.shared.guards import (
    check_min_days,
    check_min_spend,
)
from backend.services.diagnostics.shared.normalization import clamp


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

# Learning phase: dampen scores when data is too thin to trust
LEARNING_PHASE_MIN_CONVERSIONS = 50
LEARNING_PHASE_MIN_DAYS = 10
LEARNING_PHASE_FLOOR = 55  # Floor C3 at WATCH level during learning

# Minimum days of data for trend signals (C2, C3)
MIN_DAYS_FOR_TREND = 3

# C3: calendar window for CPA trend detection (days before evaluation)
C3_CALENDAR_WINDOW_DAYS = 7

# C3: fewer than this many days with valid CPA in the window → guard-fail
# (we can't fit a stable slope with 1-2 points). Not a conversion gate —
# 3 days × 1 conversion each still qualifies.
C3_MIN_CPA_DAYS_IN_WINDOW = 3

# C2: rolling window for comparing actual vs expected daily volume
C2_ROLLING_WINDOW_DAYS = 7


# ── Diagnostic message templates ──────────────────────────────────

# Voice rules (AI-115 plain-language pass): lead with what a conversion
# costs in plain dollars, then what to do. "Friction-adjusted target"
# becomes "what we'd expect for this form and audience" — same number,
# human words. Precise figures live in `inputs`.

C1_MESSAGES = {
    StatusBand.STRONG: (
        "Each conversion is costing ${cpa:.2f} against the ${target:.2f} "
        "we'd expect for this form and audience. Beating expectations "
        "by {beat_pct}."
    ),
    StatusBand.WATCH: (
        "Each conversion is costing ${cpa:.2f}, about {above_pct} more "
        "than the ${target:.2f} we'd expect for this form and audience. "
        "{context}"
    ),
    StatusBand.ACTION: (
        "Each conversion is costing ${cpa:.2f}, far above the "
        "${target:.2f} we'd expect for this form and audience. {context}"
    ),
}

# Used when the campaign is spending but has zero conversions — the
# standard CPA-ratio template doesn't apply because actual_cpa is
# undefined, so we describe the shortfall in dollars against target.
C1_ZERO_CONV_MESSAGE = (
    "${spend:.0f} spent and 0 conversions so far. For this form and "
    "audience a conversion should cost about ${target:.2f}. {context}"
)

C2_MESSAGES = {
    StatusBand.STRONG: (
        "Leads are flowing at {daily_avg:.1f} a day, right around the "
        "{expected:.1f} a day this budget should produce. {trend_msg}"
    ),
    StatusBand.WATCH: (
        "Leads are coming in at {daily_avg:.1f} a day; this budget "
        "should be producing about {expected:.1f}. {trend_msg}"
    ),
    StatusBand.ACTION: (
        "Leads are coming in at just {daily_avg:.1f} a day when this "
        "budget should be producing about {expected:.1f}. {trend_msg}"
    ),
}

C3_MESSAGES = {
    "STABLE_OR_IMPROVING": (
        "Cost per conversion is holding steady or improving "
        "({change:+.1f}%/day over the last week). No sign the audience "
        "is tapping out."
    ),
    "EARLY_DETERIORATION": (
        "Cost per conversion is creeping up ({change:+.1f}%/day). "
        "Usually the first sign the audience is getting tapped out. "
        "Watch it over the next few days."
    ),
    "MODERATE_DETERIORATION": (
        "Cost per conversion is climbing ({change:+.1f}%/day). The "
        "audience is probably saturating. Widen the targeting or "
        "refresh the creative before it compounds."
    ),
    "RAPID_DETERIORATION": (
        "Cost per conversion is spiking ({change:+.1f}%/day). This "
        "audience is tapped out. Act now: widen targeting, bring in new "
        "creative, or move the budget."
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


def _budget_weighted_audience_multiplier(
    data: CampaignData,
) -> tuple[float, dict[str, float]]:
    """Compute a budget-weighted audience-temperature multiplier.

    Returns ``(weighted_multiplier, budget_by_audience)``. A plan that is
    30% member-list / 70% prospecting resolves to
    ``0.3 * 1.0 + 0.7 * 0.30 = 0.51`` instead of the dominant-audience
    value (0.30). Mixed plans no longer get their target CPA inflated by
    the warmest tier's multiplier.

    Falls back to ``DEFAULT_MULTIPLIER`` when the plan has no budget.
    """
    audience_budgets: dict[str, float] = {}
    for line in data.media_plan:
        key = line.audience_type.value if line.audience_type else "unknown"
        audience_budgets[key] = audience_budgets.get(key, 0) + line.planned_budget

    total_budget = sum(audience_budgets.values())
    if total_budget <= 0:
        return DEFAULT_MULTIPLIER, audience_budgets

    weighted = (
        sum(
            get_temperature_multiplier(None if k == "unknown" else k) * b
            for k, b in audience_budgets.items()
        )
        / total_budget
    )
    return weighted, audience_budgets


def _determine_target_cpa(data: CampaignData) -> float | None:
    """Derive the CPA benchmark from the FFS model.

    Priority:
        1. FFS-derived: ``expected_cvr = 25 * exp(-0.045 * avg_ffs) / 100``,
           adjusted by the budget-weighted audience temperature, then
           divided into the observed CPC.
        2. ``None`` — calling signal must surface this as a guard-fail
           pointing the user at the FFS wizard.

    Removed (2026-04-20): ``actual_cpa * 1.2`` fallback. Benchmarking a
    signal against a slightly-worse version of its own actual made the
    signal impossible to fail — campaigns with terrible CPAs still
    scored "meeting target".
    """
    ffs_scores = [l.ffs_score for l in data.media_plan if l.ffs_score is not None]
    if not ffs_scores:
        return None

    avg_ffs = statistics.mean(ffs_scores)

    # Spec formula: expected_cvr = 25 * exp(-0.045 * ffs) / 100
    base_cvr = 25 * math.exp(-0.045 * avg_ffs) / 100

    # Budget-weighted audience temperature (was: dominant-audience)
    weighted_mult, _ = _budget_weighted_audience_multiplier(data)
    adjusted_cvr = base_cvr * weighted_mult

    # Estimate CPC from observed data
    if data.total_clicks <= 0 or data.total_spend <= 0 or adjusted_cvr <= 0:
        return None

    estimated_cpc = data.total_spend / data.total_clicks
    return estimated_cpc / adjusted_cvr


def compute_c1_cpa_vs_target(data: CampaignData) -> SignalResult:
    """C1: CPA vs Friction-Adjusted Target.

    The core conversion KPI — what are we paying per lead, benchmarked
    against what we *should* be paying given form friction and audience
    warmth.

    Activity gate (replaces the 5-conversion gate as of 2026-04-20): the
    signal turns on after ≥1 calendar day of flight with ≥$10 spend. Zero-
    conversion campaigns get an ACTION diagnostic that scales with how
    much budget has been burnt without a lead — they are no longer
    hidden from the user.
    """
    # 12-hour activity proxy — at least 1 calendar day + $10 spent.
    passed, reason = check_min_days(data.flight)
    if not passed:
        return _guard_fail(
            "C1", "CPA vs Target", reason,
            "Campaign just launched — metrics will start populating "
            "after 12 hours of delivery.",
        )

    passed, reason = check_min_spend(data)
    if not passed:
        return _guard_fail(
            "C1", "CPA vs Target", reason,
            "Campaign hasn't spent enough yet — waiting for delivery data.",
        )

    # Target CPA from FFS model (no actual-CPA fallback)
    target_cpa = _determine_target_cpa(data)
    if target_cpa is None or target_cpa <= 0:
        return _guard_fail(
            "C1", "CPA vs Target", "no_target_cpa",
            "Target CPA not yet derivable — complete the FFS wizard and "
            "confirm audience types in the media plan.",
        )

    # Zero-conversion path: surface as ACTION rather than guard-fail.
    # Score tapers from 25 (just started, spent ≤1 target CPA) down to 0
    # (spent ≥4× target with nothing to show).
    if data.total_conversions <= 0:
        spend_ratio = data.total_spend / target_cpa
        zero_conv_score = clamp(25 - (spend_ratio - 1) * 8, 0, 25)
        if spend_ratio >= 3:
            context = (
                "We've now spent several times what one conversion "
                "should cost. Check the landing page, the targeting, "
                "and the creative before more budget goes out."
            )
        elif spend_ratio >= 1:
            context = (
                "The campaign has now spent what one conversion should "
                "cost without getting one. Watch it closely."
            )
        else:
            context = "Early days; check again after a full day of delivery."
        diagnostic = C1_ZERO_CONV_MESSAGE.format(
            spend=data.total_spend, target=target_cpa, context=context,
        )
        return SignalResult(
            id="C1",
            name="CPA vs Target",
            score=round(zero_conv_score, 1),
            status=StatusBand.ACTION,
            raw_value=0.0,
            benchmark=1.0,
            floor=0.4,
            diagnostic=diagnostic,
            guard_passed=True,
            inputs={
                "actual_cpa": None,
                "target_cpa": round(target_cpa, 2),
                "cpa_efficiency": 0.0,
                "spend": round(data.total_spend, 2),
                "conversions": 0,
                "total_clicks": data.total_clicks,
                "spend_to_target_ratio": round(spend_ratio, 2),
                "zero_conversions": True,
            },
        )

    # Standard scoring path
    actual_cpa = data.total_spend / data.total_conversions
    cpa_efficiency = target_cpa / actual_cpa

    # Spec formula
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
            f"Some of that gap is the form's own friction "
            f"(FFS {avg_ffs:.0f}); the rest is worth investigating."
            if avg_ffs > 30
            else "Check the landing page and whether it matches what the ad promised."
        )
        diagnostic = C1_MESSAGES[StatusBand.WATCH].format(
            cpa=actual_cpa, target=target_cpa, above_pct=above_pct,
            context=context,
        )
    else:
        context = (
            "Look at the landing page message, how broad the targeting "
            "is, and whether the creative speaks to this audience."
        )
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
    then measures whether actual volume is on track. Also monitors the
    short-term trend — declining vs ramping.

    Activity gate replaces the old 5-conversion gate (2026-04-20).
    Campaigns with low volume now surface as ACTION rather than being
    hidden from the user.
    """
    # Need enough days for a 7-day rolling average + trend slope
    passed, reason = check_min_days(data.flight, min_days=MIN_DAYS_FOR_TREND)
    if not passed:
        return _guard_fail(
            "C2", "Volume Trajectory", reason,
            f"Need at least {MIN_DAYS_FOR_TREND} days of data for volume trend.",
        )

    passed, reason = check_min_spend(data)
    if not passed:
        return _guard_fail(
            "C2", "Volume Trajectory", reason,
            "Campaign hasn't spent enough yet — waiting for delivery data.",
        )

    # C2 needs a FFS-derived target CPA to compute expected volume.
    # If FFS is missing this signal can't say what volume to expect.
    target_cpa = _determine_target_cpa(data)
    if target_cpa is None or target_cpa <= 0:
        return _guard_fail(
            "C2", "Volume Trajectory", "no_target_cpa",
            "Target CPA not yet derivable — complete the FFS wizard to "
            "see volume trajectory against expectation.",
        )

    # Aggregate daily conversions, keyed by the actual date (not the
    # stringified date). Sort explicitly by date so we don't rely on
    # dict insertion order.
    daily_convs: dict[date, float] = {}
    for dm in data.daily_metrics:
        daily_convs[dm.date] = daily_convs.get(dm.date, 0) + dm.conversions

    # Exclude the evaluation date — it's almost always a partial day and
    # was dragging the rolling average down.
    evaluation = data.flight.evaluation_date
    sorted_days = sorted(
        ((d, v) for d, v in daily_convs.items() if d < evaluation),
        key=lambda item: item[0],
    )

    if len(sorted_days) < MIN_DAYS_FOR_TREND:
        return _guard_fail(
            "C2", "Volume Trajectory", "insufficient_daily_data",
            f"Need at least {MIN_DAYS_FOR_TREND} complete days of data "
            "before the current day to evaluate trajectory.",
        )

    # Rolling average of last N complete days
    recent_values = [v for _, v in sorted_days[-C2_ROLLING_WINDOW_DAYS:]]
    rolling_avg = statistics.mean(recent_values)

    # Derive expected daily volume from planned budget and target CPA
    total_budget = data.planned_budget
    total_days = data.flight.total_days
    daily_budget = total_budget / total_days if total_days > 0 else 0
    expected_daily = daily_budget / target_cpa if daily_budget > 0 else 0

    if expected_daily <= 0:
        return _guard_fail(
            "C2", "Volume Trajectory", "zero_expected_volume",
            "Planned budget is zero — cannot derive expected daily volume.",
        )

    volume_ratio = rolling_avg / expected_daily

    # Volume trend (OLS slope over the same window)
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

    # Score: volume ratio dominates, trend gives a ±bonus
    volume_score = clamp(volume_ratio * 75, 0, C2_VOLUME_CAP)
    trend_bonus = clamp(daily_volume_change * 2, -C2_TREND_MAX_BONUS, C2_TREND_MAX_BONUS)
    score = clamp(volume_score + trend_bonus, 0, 100)

    status = status_band(score)

    if daily_volume_change > 2:
        trend_msg = f"And the trend is up (+{daily_volume_change:.1f}%/day)."
    elif daily_volume_change < -5:
        trend_msg = (
            f"And the trend is down ({daily_volume_change:+.1f}%/day), "
            "which is worth investigating."
        )
    else:
        trend_msg = "The day-to-day trend is steady."

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
            "days_in_window": len(recent_values),
            "days_with_data": len(sorted_days),
            "evaluation_day_excluded": True,
        },
    )


def compute_c3_cpa_trend(data: CampaignData) -> SignalResult:
    """C3: CPA Trend (Deterioration Detection).

    Is CPA getting worse over time? A campaign that starts strong but
    sees CPA creep up signals audience saturation — the conversion
    equivalent of creative fatigue.

    Calibrated 2026-04-20:
      - Window is the last 7 CALENDAR days before ``evaluation_date``
        (not the last 7 days with conversions). Zero-conversion days
        no longer silently drop out of the denominator — they are now
        represented on the x-axis and the slope fit reflects real
        calendar time.
      - Spend-weighted OLS: a day with $1,000 of spend contributes more
        to the slope than a day with $50.
      - Activity gate instead of 5-conversion gate: fits any window with
        ≥3 days of valid CPA values, even if each day had a single
        conversion.
    """
    passed, reason = check_min_days(data.flight, min_days=MIN_DAYS_FOR_TREND)
    if not passed:
        return _guard_fail(
            "C3", "CPA Trend", reason,
            f"Need at least {MIN_DAYS_FOR_TREND} days for trend detection.",
        )

    passed, reason = check_min_spend(data)
    if not passed:
        return _guard_fail(
            "C3", "CPA Trend", reason,
            "Campaign hasn't spent enough yet — waiting for delivery data.",
        )

    # Build the calendar window: last N days strictly BEFORE evaluation_date
    evaluation = data.flight.evaluation_date
    window_dates = [
        evaluation - timedelta(days=i)
        for i in range(1, C3_CALENDAR_WINDOW_DAYS + 1)
    ]
    window_dates.sort()  # oldest → newest

    # Aggregate daily spend + conversions
    daily_spend: dict[date, float] = {}
    daily_conv: dict[date, float] = {}
    for dm in data.daily_metrics:
        daily_spend[dm.date] = daily_spend.get(dm.date, 0) + dm.spend
        daily_conv[dm.date] = daily_conv.get(dm.date, 0) + dm.conversions

    # Walk the calendar window. Positional index (i) is the x-axis for
    # OLS — this keeps zero-conversion days represented on the timeline
    # even when they can't contribute a CPA value.
    xs: list[int] = []
    ys: list[float] = []
    ws: list[float] = []
    for i, d in enumerate(window_dates):
        conv = daily_conv.get(d, 0)
        spend = daily_spend.get(d, 0)
        if conv > 0 and spend > 0:
            xs.append(i)
            ys.append(spend / conv)
            ws.append(spend)

    if len(ys) < C3_MIN_CPA_DAYS_IN_WINDOW:
        return _guard_fail(
            "C3", "CPA Trend", "insufficient_cpa_days",
            f"Need at least {C3_MIN_CPA_DAYS_IN_WINDOW} days with "
            f"conversions inside the last {C3_CALENDAR_WINDOW_DAYS} "
            "days to evaluate CPA trend.",
        )

    # Spend-weighted OLS:
    #   slope = Σ wᵢ (xᵢ - x̄_w)(yᵢ - ȳ_w) / Σ wᵢ (xᵢ - x̄_w)²
    total_w = sum(ws)
    x_mean_w = sum(w * x for w, x in zip(ws, xs)) / total_w
    y_mean_w = sum(w * y for w, y in zip(ws, ys)) / total_w
    numerator = sum(
        w * (x - x_mean_w) * (y - y_mean_w)
        for w, x, y in zip(ws, xs, ys)
    )
    denominator = sum(w * (x - x_mean_w) ** 2 for w, x in zip(ws, xs))
    slope = numerator / denominator if denominator > 0 else 0
    daily_change_pct = (slope / y_mean_w * 100) if y_mean_w > 0 else 0

    # Classify
    if daily_change_pct < C3_THRESHOLD_STABLE:
        trend = "STABLE_OR_IMPROVING"
    elif daily_change_pct < C3_THRESHOLD_EARLY:
        trend = "EARLY_DETERIORATION"
    elif daily_change_pct < C3_THRESHOLD_MODERATE:
        trend = "MODERATE_DETERIORATION"
    else:
        trend = "RAPID_DETERIORATION"

    score = float(C3_TREND_SCORES[trend])

    # Learning-phase floor: early campaigns have noisy CPA slopes, so we
    # cap the downside at WATCH until either the conversion count or the
    # flight length is meaningful. Independent of the conversion gate —
    # this protects the SCORE from false-RAPID_DETERIORATION readings on
    # thin data, not the signal's visibility.
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
            "weighted_mean_cpa": round(y_mean_w, 2),
            "slope": round(slope, 4),
            "data_points": len(ys),
            "window_days": C3_CALENDAR_WINDOW_DAYS,
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

    # Weighted average of active signals, gated on coverage (AI-040).
    pillar.apply_weighted_score(
        ACQUISITION_SIGNAL_WEIGHTS,
        min_coverage=MIN_PILLAR_COVERAGE,
        default_weight=0.33,
    )

    return pillar
