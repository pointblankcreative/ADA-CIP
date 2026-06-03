"""Distribution pillar signals (D1–D5) for persuasion campaigns.

"Are we reaching the audience?"

D1: Reach Attainment — actual vs planned reach
D2: Frequency Adequacy — within effective frequency band
D3: Frequency Distribution Health — even spread vs concentration
D4: Incremental Reach by Platform — each platform adding unique audience
D5: Delivery Cadence — smoothness + gap-day penalty across the flight

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
    D1_EARLY_FLIGHT_DAYS,
    D1_FLOOR,
    D1_PLANNED_REACH_PLAUSIBILITY_RATIO,
    D3_BENCHMARK,
    D3_FLOOR,
    D4_BENCHMARK_RATIO,
    D4_FLOOR_RATIO,
    D5_CV_CEILING,
    D5_CV_TARGET,
    D5_GAP_CEILING,
    D5_GAP_TARGET,
    D5_GAP_WEIGHT,
    D5_MIN_DAILY_ROWS,
    D5_MIN_FLIGHT_ELAPSED_FRACTION,
    D5_SMOOTHNESS_WEIGHT,
    DISTRIBUTION_SIGNAL_WEIGHTS,
    EFFECTIVE_FREQ_FLOOR,
    MIN_PILLAR_COVERAGE,
    get_freq_band,
    get_overlap_factor,
    infer_creative_format,
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
    normalize_inverse,
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
        "{platform} frequency at {freq:.1f} after {days} days — below the "
        "effective floor of {min_freq} for {format}. Not enough exposures "
        "for message absorption."
    ),
    "optimal": (
        "Weighted frequency at {freq:.1f} after {days} days — platforms "
        "are within their effective bands. Message repetition is effective "
        "without fatigue risk."
    ),
    "high": (
        "Frequency on {platform} has reached {freq:.1f} at {days} days — "
        "approaching saturation for {format}. Consider audience expansion "
        "or creative refresh within 3-5 days."
    ),
    "over": (
        "{platform} frequency at {freq:.1f} — well past the effective "
        "ceiling of {max_freq} for {format}. Budget is being wasted on "
        "over-exposed users. Recommend immediate audience expansion or "
        "budget reallocation."
    ),
}

D3_MESSAGES = {
    StatusBand.STRONG: (
        "Frequency distribution is healthy — platforms are delivering at "
        "similar positions within their effective bands (band-normalized "
        "CV {cv:.2f})."
    ),
    StatusBand.WATCH: (
        "Platforms are diverging in how well they track their effective "
        "bands — {high_plat} at {high_pos:.1f}x of its band-optimal while "
        "{low_plat} at {low_pos:.1f}x. Review audience sizing and pacing "
        "on the outlier."
    ),
    StatusBand.ACTION: (
        "Extreme variance in band-normalized delivery (CV {cv:.2f}). "
        "{high_plat} is at {high_pos:.1f}x of its band-optimal while "
        "{low_plat} is at {low_pos:.1f}x. Rebalance platform budgets or "
        "pause the over-saturated platform."
    ),
}

D4_MESSAGES = {
    StatusBand.STRONG: (
        "Platforms are contributing unique reach in line with their "
        "spend share. {best_platform} most efficient "
        "(reach {best_reach_share_pct} vs spend {best_spend_share_pct})."
    ),
    StatusBand.WATCH: (
        "{worst_platform} spending {worst_spend_share_pct} of budget but "
        "contributing only {worst_reach_share_pct} of reach — audience "
        "overlap with other platforms is likely."
    ),
    StatusBand.ACTION: (
        "{worst_platform} spending {worst_spend_share_pct} of budget but "
        "contributing only {worst_reach_share_pct} of reach. Significantly "
        "underperforming — consider consolidating budget to more efficient "
        "platforms."
    ),
}

# Formats where lower reach-per-dollar is typical — surface in the
# diagnostic as a planner-facing caveat, not as a score adjustment.
D4_LOW_REACH_FORMATS = {"video_medium", "dooh", "audio_long", "video_long"}


D5_MESSAGES = {
    StatusBand.STRONG: (
        "Delivery is pacing smoothly — {platforms_scored} platform(s) "
        "within healthy cadence (worst platform {worst_plat} at "
        "CV={worst_cv:.2f}, {worst_gap_pct} gap days)."
    ),
    StatusBand.WATCH: (
        "{worst_plat} is showing uneven delivery "
        "(CV={worst_cv:.2f}, {worst_gap_pct} gap days within its active "
        "window). Check for mid-flight pauses, sudden budget shifts, or "
        "pacing settings."
    ),
    StatusBand.ACTION: (
        "{worst_plat} is delivering in bursts or has significant dark "
        "days (CV={worst_cv:.2f}, {worst_gap_pct} gap days). "
        "Confirm the platform is live and pacing is set to Standard — "
        "this level of variance will distort frequency and attention."
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

    Planned reach sources (per line, with fallback chain):
        1. MediaPlanLine.planned_reach if populated AND plausible
           (≤ planned_impressions × plausibility ratio — otherwise
            the value is likely platform potential reach, not effective
            reach).
        2. Otherwise derive: planned_impressions / target_effective_freq
           where target is the line's frequency_cap if set, else
           EFFECTIVE_FREQ_FLOOR (3.0 — Spenkuch & Toniatti effective-
           reach floor).

    Early-flight floor: D1 cannot score below WATCH (40) for the first
    D1_EARLY_FLIGHT_DAYS days of a flight to prevent platform-learning
    noise from triggering false ACTION states.
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

    # Compute planned reach (per-line, with data quality tracking)
    planned_reach, plan_meta = _compute_planned_reach_components(data)
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

    # Early-flight WATCH floor — suppress ACTION states in days 0-2
    early_flight_floor_applied = False
    if data.flight.elapsed_days < D1_EARLY_FLIGHT_DAYS and score < 40:
        score = 40.0
        early_flight_floor_applied = True

    status = status_band(score)

    # Base diagnostic message
    msg_template = D1_MESSAGES.get(status, D1_MESSAGES[StatusBand.WATCH])
    diagnostic = msg_template.format(
        attainment_pct=format_pct(attainment),
        remaining=data.flight.remaining_days,
        elapsed=data.flight.elapsed_days,
        total=data.flight.total_days,
    )

    # Data-quality notes (appended so planners can audit / fix inputs)
    notes: list[str] = []
    if early_flight_floor_applied:
        notes.append(
            f"Score held at WATCH floor — first {D1_EARLY_FLIGHT_DAYS} days "
            "of a flight are too noisy to score ACTION."
        )
    if plan_meta["lines_flagged_potential_reach"] > 0:
        n = plan_meta["lines_flagged_potential_reach"]
        notes.append(
            f"{n} line{'s' if n != 1 else ''} had planned_reach inconsistent "
            "with planned_impressions (likely platform potential reach) — "
            "using derived effective reach instead."
        )
    if plan_meta["lines_derived_with_default_freq"] > 0:
        n = plan_meta["lines_derived_with_default_freq"]
        notes.append(
            f"{n} line{'s' if n != 1 else ''} missing frequency target — "
            f"assumed effective frequency of {EFFECTIVE_FREQ_FLOOR:.0f}."
        )

    if notes:
        diagnostic += " " + " ".join(f"Note: {n}" for n in notes)

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
            "early_flight_floor_applied": early_flight_floor_applied,
            "plan_quality": plan_meta,
            "platform_reaches": {
                p.platform_id: p.reach
                for p in data.platform_metrics if p.reach > 0
            },
        },
    )


def compute_d2_frequency_adequacy(data: CampaignData) -> SignalResult:
    """D2: Frequency Adequacy — within the effective frequency band?

    Scores each platform against its own creative-format band, then
    impression-weights the platform scores for the campaign-level score.
    This prevents mixed-format campaigns (e.g. DOOH + social video) from
    being evaluated against a single wrong band.

    Format inference uses `infer_creative_format(platform_id,
    channel_category)` — platform_id is primary, channel_category from
    the matching media plan line is consulted for conflict detection.
    """
    passed, reason = guard_distribution(data)
    if not passed:
        return _guard_fail("D2", "Frequency Adequacy", reason,
                           f"Insufficient data — {reason}.")

    # Build platform_id → channel_category lookup from media plan lines.
    # First populated value wins; planners may have multiple lines per
    # platform but channel_category should be consistent.
    channel_cat_by_platform: dict[str, str | None] = {}
    for line in data.media_plan:
        if line.platform_id and line.platform_id not in channel_cat_by_platform:
            channel_cat_by_platform[line.platform_id] = line.channel_category

    # Score each platform against its inferred band
    platform_breakdown: list[dict[str, Any]] = []
    conflict_notes: list[str] = []
    for p in data.platform_metrics:
        if p.frequency <= 0 or p.impressions <= 0:
            continue
        cc = channel_cat_by_platform.get(p.platform_id)
        fmt, conflict = infer_creative_format(p.platform_id, cc)
        if conflict:
            conflict_notes.append(conflict)
        band = get_freq_band(fmt)
        platform_score = _score_frequency(p.frequency, band)
        platform_breakdown.append({
            "platform_id": p.platform_id,
            "frequency": p.frequency,
            "impressions": p.impressions,
            "creative_format": fmt,
            "band": band,
            "score": platform_score,
        })

    if not platform_breakdown:
        return _guard_fail("D2", "Frequency Adequacy", "no_frequency_data",
                           "No frequency data available from any platform.")

    # Impression-weighted aggregation
    total_imp = sum(pb["impressions"] for pb in platform_breakdown)
    score = sum(pb["score"] * pb["impressions"] for pb in platform_breakdown) / total_imp
    avg_freq = sum(pb["frequency"] * pb["impressions"] for pb in platform_breakdown) / total_imp
    status = status_band(score)

    # Pick the platform driving the worst score for the diagnostic message.
    # (Ties broken by highest impression share so the largest delivery
    # issue surfaces first.)
    worst = min(
        platform_breakdown,
        key=lambda pb: (pb["score"], -pb["impressions"]),
    )
    worst_freq = worst["frequency"]
    worst_band = worst["band"]
    worst_format = worst["creative_format"]
    worst_platform = worst["platform_id"]

    # Message selection: when the campaign is STRONG overall, use the
    # "optimal" template with the weighted-average frequency. Otherwise
    # anchor the message on the worst platform's position in its band.
    if status == StatusBand.STRONG:
        msg_key = "optimal"
        diagnostic = D2_MESSAGES["optimal"].format(
            freq=avg_freq,
            days=data.flight.elapsed_days,
            format=worst_format.replace("_", " "),
        )
    else:
        if worst_freq < worst_band["min"]:
            msg_key = "under"
        elif worst_freq <= worst_band["optimal"]:
            # Worst platform is fine but overall status is WATCH/ACTION —
            # surface whichever adjacent band drove the miss.
            msg_key = "high" if score < 50 else "under"
        elif worst_freq <= worst_band["max"]:
            msg_key = "high"
        else:
            msg_key = "over"

        diagnostic = D2_MESSAGES[msg_key].format(
            freq=worst_freq,
            days=data.flight.elapsed_days,
            min_freq=worst_band["min"],
            max_freq=worst_band["max"],
            format=worst_format.replace("_", " "),
            platform=worst_platform,
        )

    # Multi-platform context suffix — useful when one platform drags the
    # score but most of the delivery is healthy.
    if len(platform_breakdown) > 1 and msg_key != "optimal":
        in_band = sum(
            1 for pb in platform_breakdown
            if pb["band"]["min"] <= pb["frequency"] <= pb["band"]["max"]
        )
        if in_band > 0 and in_band < len(platform_breakdown):
            diagnostic += (
                f" ({in_band} of {len(platform_breakdown)} platforms are "
                "within their effective band.)"
            )

    if conflict_notes:
        uniq = list(dict.fromkeys(conflict_notes))
        diagnostic += " Note: " + " ".join(uniq)

    return SignalResult(
        id="D2",
        name="Frequency Adequacy",
        score=round(score, 1),
        status=status,
        raw_value=round(avg_freq, 2),
        benchmark=float(worst_band["optimal"]),
        floor=float(worst_band["min"]),
        diagnostic=diagnostic,
        guard_passed=True,
        inputs={
            "avg_frequency": round(avg_freq, 2),
            "weighted_score": round(score, 1),
            "worst_platform": worst_platform,
            "platforms": [
                {
                    "platform_id": pb["platform_id"],
                    "frequency": round(pb["frequency"], 2),
                    "impressions": pb["impressions"],
                    "creative_format": pb["creative_format"],
                    "band": pb["band"],
                    "score": round(pb["score"], 1),
                }
                for pb in platform_breakdown
            ],
            "conflict_notes": list(dict.fromkeys(conflict_notes)),
        },
    )


def compute_d3_frequency_distribution(data: CampaignData) -> SignalResult:
    """D3: Frequency Distribution Health — concentration vs even delivery.

    Measures inter-platform delivery concentration using a
    band-normalized CV. Each platform's frequency is normalized against
    its own creative-format band optimal (so DOOH at freq=8 and Meta at
    freq=4 both register as "at optimal") — then CV is computed across
    those normalized positions.

    Scoring:
        cv_score = 1 - min(CV, 1.0)
        score    = normalize_linear(cv_score, floor=0.50, benchmark=0.85)

    A CV ≤ 0.15 (tight cluster at similar band positions) → STRONG.
    A CV ≥ 0.50 (one platform heavily concentrated) → ACTION.

    Guard-fails on single-platform campaigns — "distribution across
    platforms" is an empty concept with one platform.

    Note: D3 does NOT capture temporal frequency cadence (daily/weekly/
    monthly trending). That requires daily reach data which is not
    currently fetched by the engine. A separate "D5: Delivery Cadence"
    signal is in the backlog for that.
    """
    passed, reason = guard_distribution(data)
    if not passed:
        return _guard_fail("D3", "Frequency Distribution", reason,
                           f"Insufficient data — {reason}.")

    # Build platform_id → channel_category lookup for format inference
    channel_cat_by_platform: dict[str, str | None] = {}
    for line in data.media_plan:
        if line.platform_id and line.platform_id not in channel_cat_by_platform:
            channel_cat_by_platform[line.platform_id] = line.channel_category

    # Collect band-normalized positions for each platform with frequency
    platforms_with_freq = [
        p for p in data.platform_metrics
        if p.frequency > 0 and p.impressions > 0
    ]

    if len(platforms_with_freq) < 2:
        return _guard_fail(
            "D3", "Frequency Distribution", "single_platform",
            "Frequency distribution requires at least 2 platforms.",
        )

    platform_positions: list[dict[str, Any]] = []
    for p in platforms_with_freq:
        cc = channel_cat_by_platform.get(p.platform_id)
        fmt, _ = infer_creative_format(p.platform_id, cc)
        band = get_freq_band(fmt)
        optimal = band["optimal"]
        # band_position = 1.0 means "at optimal". Values > 1 → past optimal,
        # < 1 → under-delivering against this format's effective band.
        band_position = safe_div(p.frequency, optimal, 0.0)
        platform_positions.append({
            "platform_id": p.platform_id,
            "frequency": p.frequency,
            "impressions": p.impressions,
            "creative_format": fmt,
            "band_optimal": optimal,
            "band_position": band_position,
        })

    positions = [pp["band_position"] for pp in platform_positions]
    mean_pos = statistics.mean(positions)
    if mean_pos > 0:
        cv = statistics.stdev(positions) / mean_pos
    else:
        cv = 0.0
    cv_score = 1 - min(cv, 1.0)

    score = normalize_linear(cv_score, D3_FLOOR, D3_BENCHMARK)
    status = status_band(score)

    # Sorted platforms by band position for messaging
    sorted_pps = sorted(platform_positions, key=lambda pp: pp["band_position"])
    low = sorted_pps[0]
    high = sorted_pps[-1]

    if status == StatusBand.STRONG:
        diagnostic = D3_MESSAGES[StatusBand.STRONG].format(cv=cv)
    elif status == StatusBand.WATCH:
        diagnostic = D3_MESSAGES[StatusBand.WATCH].format(
            high_plat=high["platform_id"],
            high_pos=high["band_position"],
            low_plat=low["platform_id"],
            low_pos=low["band_position"],
        )
    else:
        diagnostic = D3_MESSAGES[StatusBand.ACTION].format(
            cv=cv,
            high_plat=high["platform_id"],
            high_pos=high["band_position"],
            low_plat=low["platform_id"],
            low_pos=low["band_position"],
        )

    return SignalResult(
        id="D3",
        name="Frequency Distribution",
        score=round(score, 1),
        status=status,
        raw_value=round(cv, 3),
        benchmark=D3_BENCHMARK,
        floor=D3_FLOOR,
        diagnostic=diagnostic,
        guard_passed=True,
        inputs={
            "cv": round(cv, 3),
            "cv_score": round(cv_score, 3),
            "mean_band_position": round(mean_pos, 3),
            "platforms": [
                {
                    "platform_id": pp["platform_id"],
                    "frequency": round(pp["frequency"], 2),
                    "impressions": pp["impressions"],
                    "creative_format": pp["creative_format"],
                    "band_optimal": pp["band_optimal"],
                    "band_position": round(pp["band_position"], 3),
                }
                for pp in platform_positions
            ],
        },
    )


def compute_d4_incremental_reach(data: CampaignData) -> SignalResult:
    """D4: Incremental Reach by Platform — each adding unique audience?

    Scoring logic (redesigned 2026-04-20):
        For each platform compute `deviation = reach_share - spend_share`.
        Negative deviation = under-delivering for its budget share. The
        overall score is driven by spend-weighted underperformance:

            underperformance = Σ max(-deviation_i, 0) × spend_share_i

        A platform eating 70% of budget but delivering 40% of reach
        contributes 0.30 × 0.70 = 0.21 to underperformance — and that
        single-platform drag dominates the score. Tiny outliers are
        appropriately dampened by their small spend share.

        Score via normalize_inverse:
            target=0.05  (5% spend-weighted underperformance → STRONG)
            ceiling=0.30 (30% → ACTION)

    Overlap: cross-platform audience overlap is reported in `inputs` via
    `get_overlap_factor(n_platforms)` so the UI can convey effective
    unique reach, but the deviation math operates on raw reach shares.
    A note is appended to the diagnostic when overlap is high (4+
    platforms) to remind the planner that apparent over-delivery may
    partly reflect double-counting.

    Format: platforms in D4_LOW_REACH_FORMATS (CTV, DOOH, longer-form
    audio/video) are flagged in the diagnostic when they're the worst
    performer — reach-per-dollar is structurally lower for those formats
    and the "underperformance" may be expected, not a problem.
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

    # Build channel_category lookup for format inference
    channel_cat_by_platform: dict[str, str | None] = {}
    for line in data.media_plan:
        if line.platform_id and line.platform_id not in channel_cat_by_platform:
            channel_cat_by_platform[line.platform_id] = line.channel_category

    total_reach_raw = sum(p.reach for p in platforms_with_data)
    total_spend = sum(p.spend for p in platforms_with_data)
    overlap = get_overlap_factor(len(platforms_with_data))
    effective_unique_reach = int(total_reach_raw * (1 - overlap))

    # Per-platform shares and deviations
    breakdown: list[dict[str, Any]] = []
    underperformance = 0.0
    for p in platforms_with_data:
        reach_share = safe_div(p.reach, total_reach_raw, 0.0)
        spend_share = safe_div(p.spend, total_spend, 0.0)
        deviation = reach_share - spend_share  # negative = under-delivering
        penalty = max(-deviation, 0.0) * spend_share
        underperformance += penalty

        fmt, _ = infer_creative_format(
            p.platform_id, channel_cat_by_platform.get(p.platform_id),
        )
        breakdown.append({
            "platform_id": p.platform_id,
            "reach": p.reach,
            "spend": p.spend,
            "reach_share": reach_share,
            "spend_share": spend_share,
            "deviation": deviation,
            "penalty": penalty,
            "creative_format": fmt,
        })

    score = normalize_inverse(
        underperformance,
        target=0.05,
        ceiling=0.30,
    )
    status = status_band(score)

    # Pick worst/best by spend-weighted penalty (worst) and deviation (best)
    worst = max(breakdown, key=lambda b: b["penalty"])
    best = max(breakdown, key=lambda b: b["deviation"])

    if status == StatusBand.STRONG:
        diagnostic = D4_MESSAGES[StatusBand.STRONG].format(
            best_platform=best["platform_id"],
            best_reach_share_pct=format_pct(best["reach_share"]),
            best_spend_share_pct=format_pct(best["spend_share"]),
        )
    else:
        msg_template = D4_MESSAGES.get(status, D4_MESSAGES[StatusBand.WATCH])
        diagnostic = msg_template.format(
            worst_platform=worst["platform_id"],
            worst_reach_share_pct=format_pct(worst["reach_share"]),
            worst_spend_share_pct=format_pct(worst["spend_share"]),
        )

        # Format-specific caveat on low-reach formats
        if worst["creative_format"] in D4_LOW_REACH_FORMATS:
            diagnostic += (
                f" Note: {worst['platform_id']} runs "
                f"{worst['creative_format'].replace('_', ' ')} — this "
                "format typically delivers lower reach-per-dollar than "
                "social/display, so some under-delivery may be expected."
            )

    # High-overlap caveat for 4+ platform campaigns
    if len(platforms_with_data) >= 4:
        diagnostic += (
            f" Note: with {len(platforms_with_data)} platforms, estimated "
            f"{format_pct(overlap)} cross-platform audience overlap — "
            "reach shares may overstate individual-platform uniqueness."
        )

    return SignalResult(
        id="D4",
        name="Incremental Reach",
        score=round(score, 1),
        status=status,
        raw_value=round(underperformance, 3),
        benchmark=0.05,
        floor=0.30,
        diagnostic=diagnostic,
        guard_passed=True,
        inputs={
            "underperformance": round(underperformance, 3),
            "effective_unique_reach": effective_unique_reach,
            "overlap_factor": overlap,
            "worst_platform": worst["platform_id"],
            "best_platform": best["platform_id"],
            "platforms": [
                {
                    "platform_id": b["platform_id"],
                    "reach": b["reach"],
                    "spend": round(b["spend"], 2),
                    "reach_share": round(b["reach_share"], 3),
                    "spend_share": round(b["spend_share"], 3),
                    "deviation": round(b["deviation"], 3),
                    "penalty": round(b["penalty"], 4),
                    "creative_format": b["creative_format"],
                }
                for b in breakdown
            ],
        },
    )


# ── Signal D5: Delivery Cadence ────────────────────────────────────


def compute_d5_delivery_cadence(data: CampaignData) -> SignalResult:
    """D5: Delivery Cadence — smoothness + gap-day penalty.

    Measures whether daily impressions are flowing evenly across the
    flight. Operates on per-platform daily impressions and takes the
    worst-scoring platform as the signal score — a single bursting or
    dark platform is the problem to surface, even when campaign totals
    look smooth.

    Scoring (per platform):
        cv         = stdev(daily_impressions) / mean(daily_impressions)
        cv_score   = normalize_inverse(cv, target=0.30, ceiling=1.00)
        gap_rate   = gap_days / active_window_days
        gap_score  = normalize_inverse(gap_rate, target=0.0, ceiling=0.25)
        plat_score = 0.60 * cv_score + 0.40 * gap_score

    "Active window" for a platform runs from its first non-zero
    impression day through the evaluation date. Gap days are days in
    that window with zero impressions. Staggered launches don't get
    penalized for pre-launch zeros; platforms that went dark mid-flight
    do.

    Guards:
      - Flight must be ≥20% elapsed (early flight is too noisy to score
        cadence — a front-loaded run on day 3 of 30 isn't actually a
        problem yet).
      - At least one platform needs ≥7 daily rows in its active window;
        otherwise there's not enough data to compute a meaningful CV.
    """
    # Flight-level guard: too early to measure cadence
    if data.flight.elapsed_fraction < D5_MIN_FLIGHT_ELAPSED_FRACTION:
        return _guard_fail(
            "D5", "Delivery Cadence", "early_flight",
            f"Too early to score cadence — flight is "
            f"{format_pct(data.flight.elapsed_fraction)} elapsed "
            f"(need ≥{format_pct(D5_MIN_FLIGHT_ELAPSED_FRACTION)}).",
        )

    if not data.daily_metrics:
        return _guard_fail(
            "D5", "Delivery Cadence", "no_daily_data",
            "No daily metrics available — cannot measure cadence.",
        )

    # Group daily impressions by platform
    by_platform: dict[str, dict] = {}
    for d in data.daily_metrics:
        entry = by_platform.setdefault(
            d.platform_id, {"rows": []}
        )
        entry["rows"].append({
            "date": d.date,
            "impressions": d.impressions,
        })

    platform_results: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []

    for platform_id, entry in by_platform.items():
        # Sort by date for window detection
        rows = sorted(entry["rows"], key=lambda r: r["date"])

        # Find active window: first non-zero impression day → eval_date
        first_active = next(
            (r["date"] for r in rows if r["impressions"] > 0), None
        )
        if first_active is None:
            skipped.append({
                "platform_id": platform_id,
                "reason": "no_impressions",
            })
            continue

        # Filter to active window
        in_window = [r for r in rows if r["date"] >= first_active]
        # Deduplicate by date (a platform can have multiple rows per day
        # across adsets — the engine already aggregates, but be defensive)
        by_date: dict = {}
        for r in in_window:
            by_date[r["date"]] = by_date.get(r["date"], 0) + r["impressions"]

        window_days = len(by_date)
        if window_days < D5_MIN_DAILY_ROWS:
            skipped.append({
                "platform_id": platform_id,
                "reason": "insufficient_data",
                "window_days": window_days,
            })
            continue

        impressions_series = list(by_date.values())
        gap_days = sum(1 for v in impressions_series if v == 0)
        gap_rate = gap_days / window_days

        mean_imp = statistics.mean(impressions_series)
        if mean_imp <= 0:
            skipped.append({
                "platform_id": platform_id,
                "reason": "zero_mean",
            })
            continue

        # Need at least 2 points for stdev; guaranteed by min 7 rows above
        cv = statistics.stdev(impressions_series) / mean_imp

        cv_score = normalize_inverse(cv, D5_CV_TARGET, D5_CV_CEILING)
        gap_score = normalize_inverse(gap_rate, D5_GAP_TARGET, D5_GAP_CEILING)
        platform_score = (
            D5_SMOOTHNESS_WEIGHT * cv_score
            + D5_GAP_WEIGHT * gap_score
        )

        platform_results.append({
            "platform_id": platform_id,
            "window_days": window_days,
            "first_active": first_active,
            "cv": cv,
            "cv_score": cv_score,
            "gap_days": gap_days,
            "gap_rate": gap_rate,
            "gap_score": gap_score,
            "score": platform_score,
            "mean_daily_impressions": int(mean_imp),
        })

    if not platform_results:
        return _guard_fail(
            "D5", "Delivery Cadence", "insufficient_data",
            f"No platform has ≥{D5_MIN_DAILY_ROWS} days of delivery data "
            "within its active window yet.",
        )

    # Worst-platform drives the score
    worst = min(platform_results, key=lambda p: p["score"])
    score = worst["score"]
    status = status_band(score)

    msg_template = D5_MESSAGES.get(status, D5_MESSAGES[StatusBand.WATCH])
    diagnostic = msg_template.format(
        platforms_scored=len(platform_results),
        worst_plat=worst["platform_id"],
        worst_cv=worst["cv"],
        worst_gap_pct=format_pct(worst["gap_rate"]),
    )

    if skipped:
        # Surface skipped platforms so operators can see coverage gaps
        skipped_ids = ", ".join(s["platform_id"] for s in skipped)
        diagnostic += (
            f" Note: {len(skipped)} platform(s) not yet scored ({skipped_ids})."
        )

    return SignalResult(
        id="D5",
        name="Delivery Cadence",
        score=round(score, 1),
        status=status,
        raw_value=round(worst["cv"], 3),
        benchmark=D5_CV_TARGET,
        floor=D5_CV_CEILING,
        diagnostic=diagnostic,
        guard_passed=True,
        inputs={
            "worst_platform": worst["platform_id"],
            "worst_cv": round(worst["cv"], 3),
            "worst_gap_rate": round(worst["gap_rate"], 3),
            "worst_gap_days": worst["gap_days"],
            "platforms": [
                {
                    "platform_id": p["platform_id"],
                    "window_days": p["window_days"],
                    "first_active": p["first_active"].isoformat(),
                    "cv": round(p["cv"], 3),
                    "cv_score": round(p["cv_score"], 1),
                    "gap_days": p["gap_days"],
                    "gap_rate": round(p["gap_rate"], 3),
                    "gap_score": round(p["gap_score"], 1),
                    "score": round(p["score"], 1),
                    "mean_daily_impressions": p["mean_daily_impressions"],
                }
                for p in platform_results
            ],
            "skipped_platforms": skipped,
            "composite_weights": {
                "smoothness": D5_SMOOTHNESS_WEIGHT,
                "gap": D5_GAP_WEIGHT,
            },
        },
    )


# ── Pillar assembly ─────────────────────────────────────────────────


def compute_distribution_pillar(data: CampaignData) -> PillarScore:
    """Compute all distribution signals and assemble the pillar score.

    Signal weights: D1=0.35, D2=0.25, D3=0.125, D4=0.125, D5=0.15
    """
    d1 = compute_d1_reach_attainment(data)
    d2 = compute_d2_frequency_adequacy(data)
    d3 = compute_d3_frequency_distribution(data)
    d4 = compute_d4_incremental_reach(data)
    d5 = compute_d5_delivery_cadence(data)

    pillar = PillarScore(
        name="distribution",
        signals=[d1, d2, d3, d4, d5],
        weight=0.35,  # Persuasion pillar weight
    )

    # Weighted average of active signals (Distribution-specific weights),
    # gated on coverage (AI-040).
    pillar.apply_weighted_score(
        DISTRIBUTION_SIGNAL_WEIGHTS,
        min_coverage=MIN_PILLAR_COVERAGE,
        default_weight=0.25,
    )

    return pillar


# ── Internal helpers ────────────────────────────────────────────────


def _compute_planned_reach_components(data: CampaignData) -> tuple[float, dict]:
    """Compute total planned reach per-line with data quality tracking.

    Per-line logic:
        1. If MediaPlanLine.planned_reach is populated AND plausible
           (planned_reach ≤ planned_impressions × PLAUSIBILITY_RATIO),
           use it directly — trusts planner-entered effective reach.
        2. Else if planned_impressions > 0, derive using:
               planned_reach = planned_impressions / target_effective_freq
           where target = line.frequency_cap if set, else
           EFFECTIVE_FREQ_FLOOR (3.0).
        3. Else skip the line (no impressions to work from).

    Plausibility rationale: planned_reach > planned_impressions × 0.5
    implies an average frequency below 2.0, which is below the
    effective-reach floor. That pattern almost always means the planner
    pasted the platform's reported "potential reach" (addressable
    audience size) into the wrong field.

    Returns:
        (total_planned_reach, meta_dict) — meta is consumed by the D1
        diagnostic builder to surface data-quality notes.
    """
    total = 0.0
    meta = {
        "lines_using_planned_reach_field": 0,
        "lines_derived_with_freq_cap": 0,
        "lines_derived_with_default_freq": 0,
        "lines_flagged_potential_reach": 0,
        "lines_skipped_no_impressions": 0,
    }

    for line in data.media_plan:
        imps = line.planned_impressions or 0
        if imps <= 0:
            meta["lines_skipped_no_impressions"] += 1
            continue

        pr = line.planned_reach or 0
        if pr > 0:
            if pr <= imps * D1_PLANNED_REACH_PLAUSIBILITY_RATIO:
                total += pr
                meta["lines_using_planned_reach_field"] += 1
                continue
            # Implausible — probably platform potential reach. Fall
            # through to derivation below.
            meta["lines_flagged_potential_reach"] += 1

        cap = line.frequency_cap or 0
        if cap > 0:
            target_freq = cap
            meta["lines_derived_with_freq_cap"] += 1
        else:
            target_freq = EFFECTIVE_FREQ_FLOOR
            meta["lines_derived_with_default_freq"] += 1

        total += imps / target_freq

    return total, meta


def _score_frequency(freq: float, band: dict[str, int]) -> float:
    """Score frequency against the effective band.

    Curve:
        < min:          linear 0 → 50 over [0, min]       (under-frequency)
        min → optimal:  linear 50 → 85                    (approaching effective)
        optimal → max:  linear 85 → 60                    (diminishing returns)
        max → 1.5×max:  linear 60 → 0                     (wasted spend / fatigue)
        > 1.5×max:      0                                  (severe waste)

    The drop from 85 → 60 across the optimal→max range is intentional:
    a frequency that has passed the optimal value but sits below the
    fatigue ceiling is still useful delivery, but no longer STRONG —
    WATCH is the right status to earn planner attention. Past the
    ceiling, the curve decays linearly to zero over a half-band so
    extreme over-frequency drives a clear ACTION state.
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
            return 60.0
        progress = (freq - band["optimal"]) / range_size
        return 85 - progress * 25  # 85 → 60

    # Over max — linear decay 60 → 0 over [max, 1.5 × max]
    decay_range = band["max"] * 0.5
    if decay_range <= 0:
        return 0.0
    overshoot = freq - band["max"]
    progress = min(overshoot / decay_range, 1.0)
    return max(60 - progress * 60, 0.0)
