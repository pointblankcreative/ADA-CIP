"""Distribution pillar signals (D1–D4) for persuasion campaigns.

"Are we reaching the audience?"

D1: Reach Attainment — actual vs planned reach
D2: Frequency Adequacy — within effective frequency band
D3: Frequency Distribution Health — even spread vs concentration
D4: Incremental Reach by Platform — each platform adding unique audience

These are the proving-ground signals for Phase 1. They rely on
fact_digital_daily + fact_adset_daily + media_plan_lines.
"""

from __future__ import annotations

import statistics
from typing import Any

from backend.services.diagnostics.models import (
    CampaignData,
    PillarScore,
    SignalResult,
    StatusBand,
    status_band,
)
from backend.services.diagnostics.shared.benchmarks import (
    D1_BENCHMARK,
    D1_FLOOR,
    D3_BENCHMARK,
    D3_FLOOR,
    D4_BENCHMARK_RATIO,
    D4_FLOOR_RATIO,
    DISTRIBUTION_SIGNAL_WEIGHTS,
    DEFAULT_FREQ_BAND,
    get_freq_band,
    get_overlap_factor,
)
from backend.services.diagnostics.shared.guards import (
    check_has_media_plan,
    check_has_planned_impressions,
    check_has_reach_data,
    check_min_days,
    check_min_impressions,
    guard_distribution,
)
from backend.services.diagnostics.shared.normalization import (
    clamp,
    format_number,
    format_pct,
    normalize_linear,
    normalize_ratio,
    safe_div,
)


# ── Diagnostic message templates ────────────────────────────────────

D1_MESSAGES = {
    StatusBand.STRONG: (
        "Reach at {attainment_pct} of target with {remaining} days remaining "
        "— on track."
    ),
    StatusBand.WATCH: (
        "Reach at {attainment_pct} of target with {remaining} days remaining "
        "— may need budget reallocation to close the gap."
    ),
    StatusBand.ACTION: (
        "Reach at {attainment_pct} of target at day {elapsed} of {total} "
        "— delivery issue. Check audience sizing, bid strategy, and "
        "platform-level pacing."
    ),
}

D2_MESSAGES = {
    "under": (
        "Average frequency at {freq:.1f} across {days} days — below the "
        "effective floor of {min_freq} for {format}. Not enough exposures "
        "for message absorption."
    ),
    "optimal": (
        "Average frequency at {freq:.1f} across {days} days — within the "
        "optimal range for {format}. Message repetition is effective without "
        "fatigue risk."
    ),
    "high": (
        "Frequency on {platform} has reached {freq:.1f} at {days} days — "
        "approaching saturation for {format}. Consider audience expansion "
        "or creative refresh within 3-5 days."
    ),
    "over": (
        "Frequency at {freq:.1f} — well past the effective ceiling of "
        "{max_freq} for {format}. Budget is being wasted on over-exposed "
        "users. Recommend immediate audience expansion or budget "
        "reallocation."
    ),
}

D3_MESSAGES = {
    StatusBand.STRONG: (
        "Frequency distribution is healthy — impressions are spreading "
        "evenly across the target audience with consistent delivery "
        "across platforms."
    ),
    StatusBand.WATCH: (
        "{top_platform} frequency is {top_ratio:.1f}x the campaign average "
        "— this platform may be over-serving to a narrow audience segment. "
        "Review targeting overlap."
    ),
    StatusBand.ACTION: (
        "Frequency variance across platforms is extreme (CV: {cv:.2f}). "
        "{high_plat} at freq {high_freq:.1f} while {low_plat} at "
        "{low_freq:.1f}. Rebalance platform budgets."
    ),
}

D4_MESSAGES = {
    StatusBand.STRONG: (
        "All platforms contributing proportional reach. "
        "{best_platform} most efficient at {best_ratio:.1f}x reach-to-spend ratio."
    ),
    StatusBand.WATCH: (
        "{worst_platform} spending {spend_share_pct} of budget but "
        "contributing only {reach_share_pct} of reach — audience overlap "
        "with other platforms is likely."
    ),
    StatusBand.ACTION: (
        "{worst_platform} reach efficiency at {worst_ratio:.1f}x — "
        "significantly underperforming vs spend share. Consider "
        "consolidating budget to more efficient platforms."
    ),
}


# ── Signal implementations ──────────────────────────────────────────


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


def compute_d1_reach_attainment(data: CampaignData) -> SignalResult:
    """D1: Reach Attainment — are we reaching the planned audience?

    Uses MAX reach per platform (Phase 0 finding: not SUM).
    Adjusts for cross-platform overlap estimate.
    """
    # Guard checks
    passed, reason = check_min_days(data.flight, min_days=1)
    if not passed:
        return _guard_fail("D1", "Reach Attainment", reason,
                           "Insufficient data — less than 1 day elapsed.")

    passed, reason = check_has_media_plan(data)
    if not passed:
        return _guard_fail("D1", "Reach Attainment", reason,
                           "No media plan data available.")

    passed, reason = check_has_planned_impressions(data)
    if not passed:
        return _guard_fail("D1", "Reach Attainment", reason,
                           "Media plan has no planned impressions.")

    # Compute planned reach pro-rated to elapsed time
    # planned_reach = planned_impressions / target_frequency
    avg_freq_cap = _avg_frequency_cap(data)
    planned_reach = safe_div(data.planned_impressions, avg_freq_cap, 0)
    pro_rated_reach = planned_reach * data.flight.elapsed_fraction

    if pro_rated_reach <= 0:
        return _guard_fail("D1", "Reach Attainment", "zero_planned_reach",
                           "Pro-rated planned reach is zero — check media plan.")

    # Actual reach: MAX per platform (not SUM), then sum with overlap adjustment
    platform_reaches = [p.reach for p in data.platform_metrics if p.reach > 0]
    if not platform_reaches:
        # Fall back to impressions/frequency proxy
        for p in data.platform_metrics:
            if p.impressions > 0 and p.frequency > 0:
                platform_reaches.append(int(p.impressions / p.frequency))

    if not platform_reaches:
        return _guard_fail("D1", "Reach Attainment", "no_reach_data",
                           "No reach data available from any platform.")

    raw_total_reach = sum(platform_reaches)
    overlap = get_overlap_factor(len(platform_reaches))
    actual_reach = int(raw_total_reach * (1 - overlap))

    # Attainment ratio
    attainment = actual_reach / pro_rated_reach

    # Normalize: floor=0.5, benchmark=1.0
    score = normalize_linear(attainment, D1_FLOOR, D1_BENCHMARK)
    status = status_band(score)

    # Diagnostic message
    msg_template = D1_MESSAGES.get(status, D1_MESSAGES[StatusBand.WATCH])
    diagnostic = msg_template.format(
        attainment_pct=format_pct(attainment),
        remaining=data.flight.remaining_days,
        elapsed=data.flight.elapsed_days,
        total=data.flight.total_days,
    )

    return SignalResult(
        id="D1",
        name="Reach Attainment",
        score=round(score, 1),
        status=status,
        raw_value=round(attainment, 3),
        benchmark=D1_BENCHMARK,
        floor=D1_FLOOR,
        diagnostic=diagnostic,
        guard_passed=True,
        inputs={
            "actual_reach": actual_reach,
            "pro_rated_reach": round(pro_rated_reach),
            "planned_reach": round(planned_reach),
            "elapsed_fraction": round(data.flight.elapsed_fraction, 3),
            "overlap_factor": overlap,
            "platform_reaches": {
                p.platform_id: p.reach
                for p in data.platform_metrics if p.reach > 0
            },
        },
    )


def compute_d2_frequency_adequacy(data: CampaignData) -> SignalResult:
    """D2: Frequency Adequacy — within effective frequency band?

    Score based on where the weighted average frequency falls
    relative to the band for the dominant creative format.
    """
    passed, reason = guard_distribution(data)
    if not passed:
        return _guard_fail("D2", "Frequency Adequacy", reason,
                           f"Insufficient data — {reason}.")

    # Get frequency data across platforms
    freqs_with_weight = []
    for p in data.platform_metrics:
        if p.frequency > 0 and p.impressions > 0:
            freqs_with_weight.append((p.frequency, p.impressions))

    if not freqs_with_weight:
        return _guard_fail("D2", "Frequency Adequacy", "no_frequency_data",
                           "No frequency data available from any platform.")

    # Impression-weighted average frequency
    total_imp = sum(w for _, w in freqs_with_weight)
    avg_freq = sum(f * w for f, w in freqs_with_weight) / total_imp

    # Get the frequency band — use default for now (format detection is Phase 2)
    band = DEFAULT_FREQ_BAND
    creative_format = "video_medium"  # Default assumption

    # Score using the band-based formula from spec
    score = _score_frequency(avg_freq, band)
    status = status_band(score)

    # Find highest-frequency platform for messaging
    max_plat = max(data.platform_metrics, key=lambda p: p.frequency)

    # Diagnostic message
    if avg_freq < band["min"]:
        msg_key = "under"
    elif avg_freq <= band["optimal"]:
        msg_key = "optimal"
    elif avg_freq <= band["max"]:
        msg_key = "high"
    else:
        msg_key = "over"

    diagnostic = D2_MESSAGES[msg_key].format(
        freq=avg_freq,
        days=data.flight.elapsed_days,
        min_freq=band["min"],
        max_freq=band["max"],
        format=creative_format.replace("_", " "),
        platform=max_plat.platform_id,
    )

    return SignalResult(
        id="D2",
        name="Frequency Adequacy",
        score=round(score, 1),
        status=status,
        raw_value=round(avg_freq, 2),
        benchmark=float(band["optimal"]),
        floor=float(band["min"]),
        diagnostic=diagnostic,
        guard_passed=True,
        inputs={
            "avg_frequency": round(avg_freq, 2),
            "band": band,
            "creative_format": creative_format,
            "platform_frequencies": {
                p.platform_id: round(p.frequency, 2)
                for p in data.platform_metrics if p.frequency > 0
            },
        },
    )


def compute_d3_frequency_distribution(data: CampaignData) -> SignalResult:
    """D3: Frequency Distribution Health — even or concentrated?

    Combines frequency efficiency (reach × freq ≈ impressions?) with
    inter-platform frequency variance.
    """
    passed, reason = guard_distribution(data)
    if not passed:
        return _guard_fail("D3", "Frequency Distribution", reason,
                           f"Insufficient data — {reason}.")

    platforms_with_freq = [
        p for p in data.platform_metrics
        if p.frequency > 0 and p.reach > 0 and p.impressions > 0
    ]

    if len(platforms_with_freq) < 1:
        return _guard_fail("D3", "Frequency Distribution", "no_freq_reach_data",
                           "No platforms with both frequency and reach data.")

    # Frequency efficiency: expected vs actual impressions
    total_expected = sum(p.reach * p.frequency for p in platforms_with_freq)
    total_actual = sum(p.impressions for p in platforms_with_freq)
    freq_efficiency = safe_div(total_expected, total_actual, 0)
    freq_efficiency = min(freq_efficiency, 1.0)  # Cap at 1.0

    # Inter-platform frequency CV (only meaningful with 2+ platforms)
    if len(platforms_with_freq) >= 2:
        freqs = [p.frequency for p in platforms_with_freq]
        mean_freq = statistics.mean(freqs)
        if mean_freq > 0:
            cv = statistics.stdev(freqs) / mean_freq
        else:
            cv = 0
        cv_score = 1 - min(cv, 1.0)
    else:
        cv = 0
        cv_score = 1.0

    # Combined score: 60% efficiency + 40% CV
    combined = freq_efficiency * 0.6 + cv_score * 0.4

    score = normalize_linear(combined, D3_FLOOR, D3_BENCHMARK)
    status = status_band(score)

    # Messaging
    if status == StatusBand.ACTION and len(platforms_with_freq) >= 2:
        sorted_plats = sorted(platforms_with_freq, key=lambda p: p.frequency)
        diagnostic = D3_MESSAGES[StatusBand.ACTION].format(
            cv=cv,
            high_plat=sorted_plats[-1].platform_id,
            high_freq=sorted_plats[-1].frequency,
            low_plat=sorted_plats[0].platform_id,
            low_freq=sorted_plats[0].frequency,
        )
    elif status == StatusBand.WATCH and len(platforms_with_freq) >= 2:
        sorted_plats = sorted(platforms_with_freq, key=lambda p: p.frequency, reverse=True)
        avg_f = statistics.mean(p.frequency for p in platforms_with_freq)
        diagnostic = D3_MESSAGES[StatusBand.WATCH].format(
            top_platform=sorted_plats[0].platform_id,
            top_ratio=sorted_plats[0].frequency / avg_f if avg_f > 0 else 0,
        )
    else:
        diagnostic = D3_MESSAGES[StatusBand.STRONG]

    return SignalResult(
        id="D3",
        name="Frequency Distribution",
        score=round(score, 1),
        status=status,
        raw_value=round(combined, 3),
        benchmark=D3_BENCHMARK,
        floor=D3_FLOOR,
        diagnostic=diagnostic,
        guard_passed=True,
        inputs={
            "freq_efficiency": round(freq_efficiency, 3),
            "cv": round(cv, 3),
            "cv_score": round(cv_score, 3),
            "combined": round(combined, 3),
            "platforms": {
                p.platform_id: {
                    "reach": p.reach,
                    "frequency": round(p.frequency, 2),
                    "impressions": p.impressions,
                }
                for p in platforms_with_freq
            },
        },
    )


def compute_d4_incremental_reach(data: CampaignData) -> SignalResult:
    """D4: Incremental Reach by Platform — each adding unique audience?

    Compares each platform's reach share vs spend share. A platform
    spending 30% of budget should deliver ~30% of reach.
    """
    passed, reason = guard_distribution(data)
    if not passed:
        return _guard_fail("D4", "Incremental Reach", reason,
                           f"Insufficient data — {reason}.")

    platforms_with_data = [
        p for p in data.platform_metrics
        if p.reach > 0 and p.spend > 0
    ]

    if len(platforms_with_data) < 2:
        return _guard_fail("D4", "Incremental Reach", "single_platform",
                           "Incremental reach requires at least 2 platforms.")

    total_reach = sum(p.reach for p in platforms_with_data)
    total_spend = sum(p.spend for p in platforms_with_data)

    # Compute reach efficiency for each platform
    efficiencies = {}
    for p in platforms_with_data:
        reach_share = safe_div(p.reach, total_reach, 0)
        spend_share = safe_div(p.spend, total_spend, 0)
        efficiency = safe_div(reach_share, spend_share, 0)
        efficiencies[p.platform_id] = {
            "reach_share": round(reach_share, 3),
            "spend_share": round(spend_share, 3),
            "efficiency": round(efficiency, 3),
        }

    # Score = mean efficiency across platforms (capped at 1.5 to avoid
    # one outlier dominating)
    eff_values = [min(e["efficiency"], 1.5) for e in efficiencies.values()]
    mean_eff = statistics.mean(eff_values)

    # Normalize: floor=0.3 ratio, benchmark=1.0
    score = normalize_linear(mean_eff, D4_FLOOR_RATIO, D4_BENCHMARK_RATIO)
    status = status_band(score)

    # Messaging — highlight best and worst platforms
    sorted_eff = sorted(efficiencies.items(), key=lambda x: x[1]["efficiency"])
    worst_plat, worst_data = sorted_eff[0]
    best_plat, best_data = sorted_eff[-1]

    msg_template = D4_MESSAGES.get(status, D4_MESSAGES[StatusBand.WATCH])
    diagnostic = msg_template.format(
        worst_platform=worst_plat,
        best_platform=best_plat,
        worst_ratio=worst_data["efficiency"],
        best_ratio=best_data["efficiency"],
        spend_share_pct=format_pct(worst_data["spend_share"]),
        reach_share_pct=format_pct(worst_data["reach_share"]),
    )

    return SignalResult(
        id="D4",
        name="Incremental Reach",
        score=round(score, 1),
        status=status,
        raw_value=round(mean_eff, 3),
        benchmark=D4_BENCHMARK_RATIO,
        floor=D4_FLOOR_RATIO,
        diagnostic=diagnostic,
        guard_passed=True,
        inputs={
            "mean_efficiency": round(mean_eff, 3),
            "platform_efficiencies": efficiencies,
        },
    )


# ── Pillar assembly ─────────────────────────────────────────────────


def compute_distribution_pillar(data: CampaignData) -> PillarScore:
    """Compute all distribution signals and assemble the pillar score.

    Signal weights: D1=0.40, D2=0.30, D3=0.15, D4=0.15
    """
    d1 = compute_d1_reach_attainment(data)
    d2 = compute_d2_frequency_adequacy(data)
    d3 = compute_d3_frequency_distribution(data)
    d4 = compute_d4_incremental_reach(data)

    pillar = PillarScore(
        name="distribution",
        signals=[d1, d2, d3, d4],
        weight=0.35,  # Persuasion pillar weight
    )

    # Weighted average of active signals (using Distribution-specific weights)
    active = [s for s in pillar.signals if s.guard_passed and s.score is not None]
    if active:
        weights = DISTRIBUTION_SIGNAL_WEIGHTS
        weighted_sum = sum(
            s.score * weights.get(s.id, 0.25) for s in active
        )
        total_weight = sum(
            weights.get(s.id, 0.25) for s in active
        )
        pillar.score = round(weighted_sum / total_weight, 1) if total_weight > 0 else None
        pillar.status = status_band(pillar.score) if pillar.score is not None else None
    else:
        pillar.score = None
        pillar.status = None

    return pillar


# ── Internal helpers ────────────────────────────────────────────────


def _avg_frequency_cap(data: CampaignData) -> float:
    """Get the average frequency cap from media plan lines.
    Falls back to 5 (reasonable default for persuasion) if not set.
    """
    caps = [l.frequency_cap for l in data.media_plan if l.frequency_cap > 0]
    if caps:
        return statistics.mean(caps)
    return 5.0


def _score_frequency(freq: float, band: dict[str, int]) -> float:
    """Score frequency against the effective band.

    Implements the spec's band-based scoring:
        < min: 0-50 (under-frequency)
        min to optimal: 50-85
        optimal to max: 85→75 (slight decline, diminishing returns)
        > max: 75→0 (wasted spend)
    """
    if freq < band["min"]:
        return clamp((freq / band["min"]) * 50, 0, 100)

    if freq <= band["optimal"]:
        range_size = band["optimal"] - band["min"]
        if range_size <= 0:
            return 85.0
        progress = (freq - band["min"]) / range_size
        return 50 + progress * 35  # 50 → 85

    if freq <= band["max"]:
        range_size = band["max"] - band["optimal"]
        if range_size <= 0:
            return 75.0
        progress = (freq - band["optimal"]) / range_size
        return 85 - progress * 10  # 85 → 75

    # Over max — degrades toward 0
    overshoot = (freq - band["max"]) / band["max"]
    return max(75 - overshoot * 100, 0)
