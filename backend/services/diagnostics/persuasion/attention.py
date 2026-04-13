"""Attention pillar signals (A1–A5) for persuasion campaigns.

"Are they absorbing the message?"

A1: Video Completion Quality — drop-off curve vs length-adjusted benchmarks
A2: Audio Completion Quality — StackAdapt audio quartiles
A3: Viewability — measured viewable impressions vs IAB standard
A4: Focused View / Time-Based Attention — platform-appropriate attention threshold
A5: Creative Fatigue Index — rolling trend of attention metric per creative

Phase 2 build. A1, A3, A4, and A5 have partial data coverage from the
current fact_digital_daily transformation; A2 guard-fails until the
StackAdapt audio columns are transformed. Each signal guard-fails
honestly when data is insufficient — partial coverage is preferable to
misleading scores (Phase 0 ethos: under-promise, over-deliver).
"""

from __future__ import annotations

import statistics
from collections import defaultdict
from typing import Any

from backend.services.diagnostics.models import (
    CampaignData,
    DailyMetrics,
    PillarScore,
    PlatformMetrics,
    SignalResult,
    StatusBand,
    status_band,
)
from backend.services.diagnostics.shared.benchmarks import (
    A1_QUARTILE_WEIGHTS,
    A3_BENCHMARK,
    A3_FLOOR,
    A5_FATIGUE_SCORES,
    A5_FATIGUE_THRESHOLDS,
    ATTENTION_SIGNAL_WEIGHTS,
    VIDEO_LENGTH_BENCHMARKS,
    get_a4_benchmark,
)
from backend.services.diagnostics.shared.guards import (
    MIN_DAYS_FOR_FATIGUE,
    MIN_VIEWABILITY_MEASURED,
    MIN_VIDEO_STARTS,
    check_has_quartile_data,
    check_has_viewability_data,
    check_min_days,
    check_min_days_for_fatigue,
    check_min_impressions,
    check_min_video_starts,
    guard_attention,
)
from backend.services.diagnostics.shared.normalization import (
    clamp,
    format_pct,
    normalize_linear,
    safe_div,
)


# ── Diagnostic message templates ────────────────────────────────────

A1_DIAGNOSES = {
    "HEALTHY": "Retention curve is within normal parameters for the assumed {length} format.",
    "HOOK_MISS": (
        "Only {q25_pct} of video starts reach Q1 — the opening isn't stopping the "
        "scroll. Assess the first 2–3 seconds of the creative."
    ),
    "HOOK_FAILURE": (
        "{drop:.0%} drop-off between Q1 and Q2 — audience got past the hook but "
        "disengaged during the message body. Strengthen the bridge between hook "
        "and core argument."
    ),
    "MESSAGE_FATIGUE": (
        "{drop:.0%} drop-off between Q2 and Q3 — mid-section is losing audience. "
        "Consider tightening the message or adding visual variety."
    ),
    "CTA_WEAKNESS": (
        "{drop:.0%} drop-off between Q3 and Q4 — audience absorbed the message but "
        "disengaged at the close. Strengthen the CTA or end card."
    ),
}

A3_MESSAGES = {
    StatusBand.STRONG: (
        "Viewability at {rate} across measured impressions (vs {benchmark}% "
        "IAB standard). Placements are in-view and attention-capable."
    ),
    StatusBand.WATCH: (
        "Viewability at {rate} — below the {benchmark}% IAB standard. "
        "Review placement quality; some inventory may be below-the-fold."
    ),
    StatusBand.ACTION: (
        "Viewability at {rate} — significantly below the {benchmark}% IAB "
        "standard. Placements may be in low-attention inventory. Review "
        "the site list with the trading team."
    ),
}

A4_MESSAGES = {
    StatusBand.STRONG: (
        "Focused-view rate averaging {rate} across platforms — audience is holding "
        "attention past platform-specific thresholds."
    ),
    StatusBand.WATCH: (
        "Focused-view rate at {rate} — on the edge of platform benchmarks. "
        "{worst_platform} is underperforming at {worst_rate}."
    ),
    StatusBand.ACTION: (
        "Focused-view rate at {rate} — well below platform benchmarks. "
        "{worst_platform} at {worst_rate} vs {worst_benchmark} benchmark. "
        "Scroll-past problem, not a targeting problem — assess the creative."
    ),
}

A5_MESSAGES = {
    "NONE": "Attention metric stable over {days} days — no fatigue signal detected.",
    "EARLY": (
        "Attention metric declining {slope:.1f}% per day over {days} days — "
        "early fatigue. Have a creative refresh ready within the week."
    ),
    "MODERATE": (
        "Attention metric declining {slope:.1f}% per day over {days} days — "
        "moderate fatigue. Plan a creative refresh; continued spend on a "
        "fatigued creative is inefficient."
    ),
    "SEVERE": (
        "Attention metric down {slope:.1f}% per day over {days} days — severe "
        "fatigue. Immediate creative assessment required."
    ),
}


# ── Internal helpers ────────────────────────────────────────────────


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


def _platform_video_starts(p: PlatformMetrics) -> int:
    """Estimate video starts for a platform.

    Facebook/Meta has an explicit 3-second autoplay filter (video_views_3s).
    Other platforms don't expose a unified start column in our transformation
    yet, so use Q25 (25% completion) as a conservative proxy — this
    under-counts starts slightly, which tilts A1 scores more optimistic
    (acceptable in the "under-promise" direction since we're protecting
    against false ACTION calls).
    """
    if p.video_views_3s > 0:
        return p.video_views_3s
    return p.video_q25


def _has_video_activity(p: PlatformMetrics) -> bool:
    """Platform has any video quartile activity worth scoring."""
    return (p.video_q25 + p.video_q50 + p.video_q75 + p.video_q100) > 0


def _default_video_length() -> str:
    """Default video length bucket when creative metadata isn't yet captured.

    30s is the most common PB persuasion format. Once ADAC-15 (creative
    duration metadata) lands, this should use per-line length from the
    media plan.
    """
    return "30s"


def _daily_thruplay_rate(day: list[DailyMetrics]) -> float | None:
    """Attention proxy for a given day: thruplay / starts (or thruplay / impressions).

    Returns None if the day has no video activity.
    """
    thruplay = sum(d.thruplay for d in day)
    starts = sum(d.video_views_3s for d in day)
    imps = sum(d.impressions for d in day)

    if thruplay <= 0:
        # No thruplay signal — fall back to 3s-views-per-impression as the
        # only attention proxy we have. If neither is present, day is dark.
        if starts > 0 and imps > 0:
            return starts / imps
        return None

    if starts > 0:
        return thruplay / starts
    if imps > 0:
        return thruplay / imps
    return None


def _linear_slope(values: list[float]) -> float:
    """Ordinary-least-squares slope of a short series (x = 0..n-1).

    Returns 0 for series shorter than 2.
    """
    n = len(values)
    if n < 2:
        return 0.0
    mean_x = (n - 1) / 2
    mean_y = sum(values) / n
    num = sum((i - mean_x) * (v - mean_y) for i, v in enumerate(values))
    den = sum((i - mean_x) ** 2 for i in range(n))
    if den == 0:
        return 0.0
    return num / den


def _classify_fatigue(daily_change_pct: float) -> str:
    """Map daily % change to a fatigue band per spec thresholds."""
    if daily_change_pct > A5_FATIGUE_THRESHOLDS["NONE"]:
        return "NONE"
    if daily_change_pct > A5_FATIGUE_THRESHOLDS["EARLY"]:
        return "EARLY"
    if daily_change_pct > A5_FATIGUE_THRESHOLDS["MODERATE"]:
        return "MODERATE"
    return "SEVERE"


# ── Signal A1: Video Completion Quality ────────────────────────────


def compute_a1_video_completion(data: CampaignData) -> SignalResult:
    """A1: Video Completion Quality.

    Aggregates quartile data across platforms with video activity, compares
    each quartile retention rate against a length-adjusted benchmark, and
    scores a weighted average. Also classifies drop-off shape (HOOK_MISS,
    HOOK_FAILURE, MESSAGE_FATIGUE, CTA_WEAKNESS, HEALTHY) and surfaces it
    in the diagnostic message.
    """
    passed, reason = guard_attention(data)
    if not passed:
        return _guard_fail("A1", "Video Completion Quality", reason,
                           f"Insufficient data — {reason}.")

    passed, reason = check_has_quartile_data(data)
    if not passed:
        return _guard_fail("A1", "Video Completion Quality", reason,
                           "No video quartile data on any platform.")

    passed, reason = check_min_video_starts(data)
    if not passed:
        # Fallback to Q25 as a start proxy — but only if it clears the threshold
        q25_total = sum(p.video_q25 for p in data.platform_metrics)
        if q25_total < MIN_VIDEO_STARTS:
            return _guard_fail("A1", "Video Completion Quality", reason,
                               "Not enough video starts to produce stable "
                               "completion rates yet.")

    # Aggregate starts + quartiles across video platforms
    video_platforms = [p for p in data.platform_metrics if _has_video_activity(p)]
    total_starts = sum(_platform_video_starts(p) for p in video_platforms)
    total_q25 = sum(p.video_q25 for p in video_platforms)
    total_q50 = sum(p.video_q50 for p in video_platforms)
    total_q75 = sum(p.video_q75 for p in video_platforms)
    total_q100 = sum(p.video_q100 for p in video_platforms)

    if total_starts <= 0:
        return _guard_fail("A1", "Video Completion Quality", "no_starts",
                           "No video starts recorded — cannot compute completion rates.")

    # Retention rates (capped at 1.0 — some platforms report q25 > starts
    # because of autoplay de-dup differences; we treat those as full retention)
    q25_rate = min(total_q25 / total_starts, 1.0)
    q50_rate = min(total_q50 / total_starts, 1.0)
    q75_rate = min(total_q75 / total_starts, 1.0)
    q100_rate = min(total_q100 / total_starts, 1.0)

    # Length-adjusted benchmark — 30s default until creative metadata lands
    length = _default_video_length()
    bench = VIDEO_LENGTH_BENCHMARKS[length]
    w = A1_QUARTILE_WEIGHTS

    # Compare each retention rate to its benchmark, weight, and scale to 75
    benchmark_score = (
        (q25_rate / bench["q25"])   * w["q25"] +
        (q50_rate / bench["q50"])   * w["q50"] +
        (q75_rate / bench["q75"])   * w["q75"] +
        (q100_rate / bench["q100"]) * w["q100"]
    ) * 75

    score = clamp(benchmark_score, 0, 100)
    status = status_band(score)

    # Drop-off diagnostics — only meaningful if we have non-zero Q1
    diagnosis = "HEALTHY"
    drop_value = 0.0
    if q25_rate < 0.50:
        diagnosis = "HOOK_MISS"
    else:
        drop_25_to_50 = 1 - safe_div(total_q50, total_q25, 1)
        drop_50_to_75 = 1 - safe_div(total_q75, total_q50, 1)
        drop_75_to_100 = 1 - safe_div(total_q100, total_q75, 1)
        if drop_25_to_50 > 0.40:
            diagnosis = "HOOK_FAILURE"
            drop_value = drop_25_to_50
        elif drop_50_to_75 > 0.35:
            diagnosis = "MESSAGE_FATIGUE"
            drop_value = drop_50_to_75
        elif drop_75_to_100 > 0.40:
            diagnosis = "CTA_WEAKNESS"
            drop_value = drop_75_to_100

    template = A1_DIAGNOSES.get(diagnosis, A1_DIAGNOSES["HEALTHY"])
    diagnostic = template.format(
        length=length,
        q25_pct=format_pct(q25_rate),
        drop=drop_value,
    )

    return SignalResult(
        id="A1",
        name="Video Completion Quality",
        score=round(score, 1),
        status=status,
        raw_value=round(q100_rate, 3),
        benchmark=bench["q100"],
        floor=0.0,
        diagnostic=diagnostic,
        guard_passed=True,
        inputs={
            "length_assumed": length,
            "diagnosis": diagnosis,
            "starts": total_starts,
            "q25": total_q25,
            "q50": total_q50,
            "q75": total_q75,
            "q100": total_q100,
            "q25_rate": round(q25_rate, 3),
            "q50_rate": round(q50_rate, 3),
            "q75_rate": round(q75_rate, 3),
            "q100_rate": round(q100_rate, 3),
            "benchmark": bench,
            "platforms": [p.platform_id for p in video_platforms],
        },
    )


# ── Signal A2: Audio Completion Quality ────────────────────────────


def compute_a2_audio_completion(data: CampaignData) -> SignalResult:
    """A2: Audio Completion Quality — StackAdapt audio quartiles.

    GUARD-FAILED in Phase 2 — StackAdapt audio quartile columns
    (Audio_started, Audio_completed_25/50/75/95) are not yet in the
    transformation layer. The signal shell is here so the engine and
    pillar builder can incorporate it once data is wired in.
    """
    return _guard_fail(
        "A2", "Audio Completion Quality", "no_audio_data_in_transformation",
        "Audio completion metrics are not yet wired into the transformation "
        "layer (StackAdapt audio columns pending). Signal will activate once "
        "audio quartile data is available in fact_digital_daily.",
    )


# ── Signal A3: Viewability ─────────────────────────────────────────


def compute_a3_viewability(data: CampaignData) -> SignalResult:
    """A3: Viewability — measured viewable impressions vs IAB 70% standard.

    Uses the viewability_measured / viewability_viewed columns on
    fact_digital_daily. Platforms that don't report viewability (most
    video/feed platforms) are excluded from the measurement base.
    """
    passed, reason = guard_attention(data)
    if not passed:
        return _guard_fail("A3", "Viewability", reason,
                           f"Insufficient data — {reason}.")

    passed, reason = check_has_viewability_data(data)
    if not passed:
        return _guard_fail("A3", "Viewability", reason,
                           "No platforms with measured viewability data — common "
                           "for video/feed-heavy campaigns. Not a cause for concern.")

    plats_with_view = [
        p for p in data.platform_metrics
        if p.viewability_measured > 0
    ]
    total_measured = sum(p.viewability_measured for p in plats_with_view)
    total_viewed = sum(p.viewability_viewed for p in plats_with_view)

    viewability_rate = safe_div(total_viewed, total_measured, 0)
    viewability_pct = viewability_rate * 100

    score = normalize_linear(viewability_pct, A3_FLOOR, A3_BENCHMARK)
    status = status_band(score)

    template = A3_MESSAGES.get(status, A3_MESSAGES[StatusBand.WATCH])
    diagnostic = template.format(
        rate=format_pct(viewability_rate),
        benchmark=A3_BENCHMARK,
    )

    return SignalResult(
        id="A3",
        name="Viewability",
        score=round(score, 1),
        status=status,
        raw_value=round(viewability_rate, 3),
        benchmark=float(A3_BENCHMARK),
        floor=float(A3_FLOOR),
        diagnostic=diagnostic,
        guard_passed=True,
        inputs={
            "viewability_rate": round(viewability_rate, 3),
            "measured_impressions": total_measured,
            "viewed_impressions": total_viewed,
            "platforms": {
                p.platform_id: {
                    "measured": p.viewability_measured,
                    "viewed": p.viewability_viewed,
                    "rate": round(safe_div(p.viewability_viewed, p.viewability_measured, 0), 3),
                }
                for p in plats_with_view
            },
        },
    )


# ── Signal A4: Focused View / Time-Based Attention ─────────────────


def compute_a4_focused_view(data: CampaignData) -> SignalResult:
    """A4: Focused View — platform-appropriate meaningful attention rate.

    Per platform, compute the "meaningful view rate":
        Facebook/Meta: thruplay / impressions (ThruPlay = 15s+)
        Others:        video_views_3s / impressions (3s floor as proxy)

    Score each platform against its A4 benchmark, then impression-weight
    to a single pillar score. Platforms without video activity are
    excluded from the measurement base.
    """
    passed, reason = guard_attention(data)
    if not passed:
        return _guard_fail("A4", "Focused View", reason,
                           f"Insufficient data — {reason}.")

    platform_rows: list[dict[str, Any]] = []
    for p in data.platform_metrics:
        if p.impressions <= 0:
            continue

        pid_lower = (p.platform_id or "").lower()
        is_meta = "facebook" in pid_lower or "meta" in pid_lower

        if is_meta:
            meaningful = p.thruplay
            metric_label = "ThruPlay (15s+)"
        else:
            meaningful = p.video_views_3s
            metric_label = "3s+ views"

        if meaningful <= 0:
            continue

        rate = meaningful / p.impressions
        bench_cfg = get_a4_benchmark(p.platform_id)
        bench = bench_cfg["benchmark"]
        floor = bench_cfg["floor"]
        plat_score = normalize_linear(rate, floor, bench)

        platform_rows.append({
            "platform_id": p.platform_id,
            "rate": rate,
            "benchmark": bench,
            "floor": floor,
            "score": plat_score,
            "impressions": p.impressions,
            "meaningful_views": meaningful,
            "metric_label": metric_label,
        })

    if not platform_rows:
        return _guard_fail("A4", "Focused View", "no_attention_data",
                           "No platforms reported video views or ThruPlay — "
                           "signal will activate once video creative is running.")

    # Impression-weighted aggregate score
    total_imps = sum(r["impressions"] for r in platform_rows)
    weighted_score = sum(r["score"] * r["impressions"] for r in platform_rows) / total_imps
    weighted_rate = sum(r["rate"] * r["impressions"] for r in platform_rows) / total_imps

    score = round(weighted_score, 1)
    status = status_band(score)

    worst = min(platform_rows, key=lambda r: r["score"])

    template = A4_MESSAGES.get(status, A4_MESSAGES[StatusBand.WATCH])
    diagnostic = template.format(
        rate=format_pct(weighted_rate),
        worst_platform=worst["platform_id"],
        worst_rate=format_pct(worst["rate"]),
        worst_benchmark=format_pct(worst["benchmark"]),
    )

    return SignalResult(
        id="A4",
        name="Focused View",
        score=score,
        status=status,
        raw_value=round(weighted_rate, 3),
        benchmark=None,  # Platform-specific — no single benchmark
        floor=None,
        diagnostic=diagnostic,
        guard_passed=True,
        inputs={
            "weighted_rate": round(weighted_rate, 3),
            "platforms": {
                r["platform_id"]: {
                    "rate": round(r["rate"], 3),
                    "benchmark": r["benchmark"],
                    "floor": r["floor"],
                    "score": round(r["score"], 1),
                    "metric": r["metric_label"],
                    "impressions": r["impressions"],
                    "meaningful_views": r["meaningful_views"],
                }
                for r in platform_rows
            },
        },
    )


# ── Signal A5: Creative Fatigue Index ──────────────────────────────


def compute_a5_creative_fatigue(data: CampaignData) -> SignalResult:
    """A5: Creative Fatigue Index — trend of attention metric over time.

    Phase 2 scope: uses platform-level daily trend (not per-creative).
    Per-creative fatigue requires creative_variant_id on fact_digital_daily
    which isn't in the transformation yet — flagged as a follow-up.

    Method:
        1. Collapse daily_metrics to one value per date (sum across platforms)
        2. Compute a ThruPlay-rate proxy per day
        3. Take the last 7 days, fit a linear slope
        4. Normalise slope / mean to % change per day → classify → score
    """
    passed, reason = check_min_days_for_fatigue(data.flight)
    if not passed:
        return _guard_fail("A5", "Creative Fatigue", reason,
                           f"Fatigue trend needs at least "
                           f"{MIN_DAYS_FOR_FATIGUE} days of delivery — "
                           f"signal will activate at day {MIN_DAYS_FOR_FATIGUE}.")

    if not data.daily_metrics:
        return _guard_fail("A5", "Creative Fatigue", "no_daily_data",
                           "No daily metrics available — cannot compute trend.")

    # Group daily metrics by date
    by_date: dict[Any, list[DailyMetrics]] = defaultdict(list)
    for d in data.daily_metrics:
        by_date[d.date].append(d)

    # Compute daily attention rate, keep only days that have a value
    series: list[tuple[Any, float]] = []
    for dt in sorted(by_date.keys()):
        rate = _daily_thruplay_rate(by_date[dt])
        if rate is not None:
            series.append((dt, rate))

    if len(series) < MIN_DAYS_FOR_FATIGUE:
        return _guard_fail("A5", "Creative Fatigue", "insufficient_daily_series",
                           f"Only {len(series)} days of attention data available — "
                           f"need at least {MIN_DAYS_FOR_FATIGUE} for a stable trend.")

    window = series[-MIN_DAYS_FOR_FATIGUE:]
    values = [v for _, v in window]
    mean_v = statistics.mean(values)

    if mean_v <= 0:
        return _guard_fail("A5", "Creative Fatigue", "zero_attention_baseline",
                           "Attention metric is zero across the window — "
                           "cannot compute a meaningful trend.")

    slope = _linear_slope(values)
    # Express slope as % change per day relative to window mean
    daily_change_pct = (slope / mean_v) * 100

    fatigue = _classify_fatigue(daily_change_pct)
    score = float(A5_FATIGUE_SCORES[fatigue])

    # Grace period for young flights — don't over-punish fresh creative
    if data.flight.elapsed_days < 14:
        score = max(score, 65.0)

    status = status_band(score)

    template = A5_MESSAGES[fatigue]
    diagnostic = template.format(
        days=len(window),
        slope=daily_change_pct,
    )

    return SignalResult(
        id="A5",
        name="Creative Fatigue",
        score=round(score, 1),
        status=status,
        raw_value=round(daily_change_pct, 2),
        benchmark=A5_FATIGUE_THRESHOLDS["NONE"],
        floor=A5_FATIGUE_THRESHOLDS["MODERATE"],
        diagnostic=diagnostic,
        guard_passed=True,
        inputs={
            "fatigue_band": fatigue,
            "daily_change_pct": round(daily_change_pct, 3),
            "window_days": len(window),
            "window_mean_rate": round(mean_v, 4),
            "slope_raw": round(slope, 6),
            "window": [
                {"date": str(dt), "rate": round(v, 4)}
                for dt, v in window
            ],
            "note": (
                "Platform-level trend proxy — per-creative fatigue requires "
                "creative_variant_id in fact_digital_daily (pending)."
            ),
        },
    )


# ── Pillar assembly ─────────────────────────────────────────────────


def compute_attention_pillar(data: CampaignData) -> PillarScore:
    """Compute all Attention signals and assemble the pillar score.

    Weights (video-default): A1=0.35, A2=0.20, A3=0.15, A4=0.30, A5=0.20.
    Guard-failed signals are excluded and their weight redistributes pro
    rata across active signals (same pattern as Distribution).
    """
    a1 = compute_a1_video_completion(data)
    a2 = compute_a2_audio_completion(data)
    a3 = compute_a3_viewability(data)
    a4 = compute_a4_focused_view(data)
    a5 = compute_a5_creative_fatigue(data)

    pillar = PillarScore(
        name="attention",
        signals=[a1, a2, a3, a4, a5],
        weight=0.40,  # Persuasion pillar weight
    )

    active = [s for s in pillar.signals if s.guard_passed and s.score is not None]
    if active:
        weights = ATTENTION_SIGNAL_WEIGHTS
        weighted_sum = sum(
            s.score * weights.get(s.id, 0.20) for s in active
        )
        total_weight = sum(
            weights.get(s.id, 0.20) for s in active
        )
        pillar.score = round(weighted_sum / total_weight, 1) if total_weight > 0 else None
        pillar.status = status_band(pillar.score) if pillar.score is not None else None
    else:
        pillar.score = None
        pillar.status = None

    return pillar
