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
    A3_COVERAGE_NOTE_THRESHOLD,
    A3_FLOOR,
    A5_FATIGUE_SCORES,
    A5_FATIGUE_THRESHOLDS,
    A5_MIN_DAY_IMP_FRACTION,
    ATTENTION_SIGNAL_WEIGHTS,
    VIDEO_LENGTH_BENCHMARKS,
    get_a4_benchmark,
    infer_creative_format,
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
    "HEALTHY": (
        "Retention curve is within normal parameters across video platforms "
        "(assumed {length} format)."
    ),
    "HOOK_MISS": (
        "{platform} ({length}): only {q25_pct} of video starts reach Q1 — "
        "the opening isn't stopping the scroll. Assess the first 2–3 seconds "
        "of the creative."
    ),
    "HOOK_FAILURE": (
        "{platform} ({length}): {drop:.0%} drop-off between Q1 and Q2 — "
        "audience got past the hook but disengaged during the message body. "
        "Strengthen the bridge between hook and core argument."
    ),
    "MESSAGE_FATIGUE": (
        "{platform} ({length}): {drop:.0%} drop-off between Q2 and Q3 — "
        "mid-section is losing audience. Consider tightening the message or "
        "adding visual variety."
    ),
    "CTA_WEAKNESS": (
        "{platform} ({length}): {drop:.0%} drop-off between Q3 and Q4 — "
        "audience absorbed the message but disengaged at the close. "
        "Strengthen the CTA or end card."
    ),
}

# Minimum gap (observed - expected) that qualifies as a "drop-off shape"
# abnormality. 15pp keeps the classification meaningful across length
# buckets — for 30s the expected q50→q75 drop is ~29%, so HOOK_FAILURE
# fires at ~44%+; for 15s the expected drop is ~21%, so it fires at ~36%+.
A1_SHAPE_ABNORMAL_DELTA = 0.15

# HOOK_MISS fires when q25_rate is less than this fraction of the
# length-specific q25 benchmark. 0.7 matches the normalize_linear shape
# (75 at benchmark, 0 at floor) — a platform landing <70% of its
# benchmark Q1 retention is performing meaningfully below healthy.
A1_HOOK_MISS_RATIO = 0.7

# Format → length bucket mapping for VIDEO_LENGTH_BENCHMARKS lookup.
# Short formats (6-15s creative) use 15s benchmarks — gentler expected
# drop-offs, harder scores, which keeps 6s creatives from under-scoring.
# Medium = 30s (PB's default persuasion video). Long = 60s (representative
# of 31s+ creative; 90s creative would benefit from further calibration
# once we carry duration metadata per-line).
A1_FORMAT_TO_LENGTH = {
    "video_short":  "15s",
    "video_medium": "30s",
    "video_long":   "60s",
}


def _a1_length_for_format(creative_format: str) -> str:
    """Map inferred creative format to a VIDEO_LENGTH_BENCHMARKS bucket."""
    return A1_FORMAT_TO_LENGTH.get(creative_format, "30s")


def _a1_expected_drops(bench: dict[str, float]) -> dict[str, float]:
    """Expected quartile-to-quartile drop-off ratios for a length bucket."""
    return {
        "q25_to_q50": 1.0 - bench["q50"] / bench["q25"],
        "q50_to_q75": 1.0 - bench["q75"] / bench["q50"],
        "q75_to_q100": 1.0 - bench["q100"] / bench["q75"],
    }


def _a1_classify_shape(
    q25_rate: float,
    q50_rate: float,
    q75_rate: float,
    q100_rate: float,
    bench: dict[str, float],
) -> tuple[str, float]:
    """Length-aware drop-off shape classification.

    Returns (diagnosis_key, drop_value). drop_value is the observed drop
    that triggered the classification (0 for HEALTHY / HOOK_MISS).
    """
    # HOOK_MISS: Q1 retention falls meaningfully below the length-specific
    # benchmark. Uses A1_HOOK_MISS_RATIO × bench["q25"] as the trigger.
    if q25_rate < A1_HOOK_MISS_RATIO * bench["q25"]:
        return "HOOK_MISS", 0.0

    expected = _a1_expected_drops(bench)
    observed = {
        "q25_to_q50": 1.0 - safe_div(q50_rate, q25_rate, 1.0),
        "q50_to_q75": 1.0 - safe_div(q75_rate, q50_rate, 1.0),
        "q75_to_q100": 1.0 - safe_div(q100_rate, q75_rate, 1.0),
    }

    # Evaluate each transition in order; first abnormal drop wins so the
    # earliest-in-the-funnel issue surfaces to the planner.
    for key, diagnosis in (
        ("q25_to_q50", "HOOK_FAILURE"),
        ("q50_to_q75", "MESSAGE_FATIGUE"),
        ("q75_to_q100", "CTA_WEAKNESS"),
    ):
        if observed[key] - expected[key] >= A1_SHAPE_ABNORMAL_DELTA:
            return diagnosis, observed[key]

    return "HEALTHY", 0.0

A3_MESSAGES = {
    StatusBand.STRONG: (
        "Viewability at {rate} across measured impressions. Placements are "
        "in-view and attention-capable."
    ),
    StatusBand.WATCH: (
        "Viewability at {rate} — below the {benchmark}% target. {worst_suffix}"
        "Review placement quality; some inventory may be below-the-fold."
    ),
    StatusBand.ACTION: (
        "Viewability at {rate} — well below the {benchmark}% target. "
        "{worst_suffix}Placements likely in low-attention inventory. Review "
        "the site list with the trading team."
    ),
}

A4_MESSAGES = {
    StatusBand.STRONG: (
        "Focused-view rate averaging {rate} across video platforms — "
        "audience is holding attention past platform-specific thresholds."
    ),
    StatusBand.WATCH: (
        "Focused-view rate at {rate} — on the edge of platform benchmarks. "
        "{worst_platform} at {worst_rate} ({worst_metric}, benchmark "
        "{worst_benchmark})."
    ),
    StatusBand.ACTION: (
        "Focused-view rate at {rate} — well below platform benchmarks. "
        "{worst_platform} at {worst_rate} {worst_metric} vs "
        "{worst_benchmark} benchmark. Scroll-past problem, not a targeting "
        "problem — assess the creative."
    ),
}

A5_MESSAGES = {
    "NONE": (
        "Attention metric stable across platforms over {days} days — no "
        "fatigue signal detected.{worst_suffix}"
    ),
    "EARLY": (
        "Attention metric declining {slope:.1f}%/day over {days} days{worst_suffix} — "
        "early fatigue. Have a creative refresh ready within the week."
    ),
    "MODERATE": (
        "Attention metric declining {slope:.1f}%/day over {days} days{worst_suffix} — "
        "moderate fatigue. Plan a creative refresh; continued spend on a "
        "fatigued creative is inefficient."
    ),
    "SEVERE": (
        "Attention metric down {slope:.1f}%/day over {days} days{worst_suffix} — "
        "severe fatigue. Immediate creative assessment required."
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


def _platform_video_starts(p: PlatformMetrics) -> tuple[int, bool]:
    """Return (starts, has_true_starts) for a platform.

    Facebook/Meta has an explicit 3-second autoplay filter (video_views_3s)
    which we treat as a true starts measurement. Other platforms don't
    expose a unified start column in our transformation yet — when that's
    the case we fall back to Q25 as a denominator proxy, but the caller
    must drop Q1 from scoring (it would evaluate to 1.0 by construction
    and bias scores high).
    """
    if p.video_views_3s > 0:
        return p.video_views_3s, True
    return p.video_q25, False


def _has_video_activity(p: PlatformMetrics) -> bool:
    """Platform has any video quartile activity worth scoring."""
    return (p.video_q25 + p.video_q50 + p.video_q75 + p.video_q100) > 0


def _daily_attention_rate(day: list[DailyMetrics]) -> tuple[float | None, int, str | None]:
    """Per-platform/day attention proxy: thruplay ÷ impressions.

    Returns (rate, impressions, metric_used).
    metric_used is "thruplay" when thruplay>0; "3s-view" when falling back
    to 3s-views ÷ impressions (platforms without thruplay data); or None
    when neither is available.

    Thruplay ÷ impressions (not thruplay ÷ starts) is the true
    attention-per-impression rate. If audiences disengage, starts drop
    first and thruplay drops proportionally — a completion rate
    (thruplay/starts) can stay flat while actual attention collapses.
    """
    thruplay = sum(d.thruplay for d in day)
    starts = sum(d.video_views_3s for d in day)
    imps = sum(d.impressions for d in day)

    if imps <= 0:
        return None, 0, None

    if thruplay > 0:
        return thruplay / imps, imps, "thruplay"
    if starts > 0:
        return starts / imps, imps, "3s-view"
    return None, imps, None


# Retained for backwards-compatibility in case external callers reference
# the old aggregated helper. Uses the new per-platform formulation summed
# across platforms. Prefer _daily_attention_rate for new code.
def _daily_thruplay_rate(day: list[DailyMetrics]) -> float | None:  # noqa: D401
    rate, _, _ = _daily_attention_rate(day)
    return rate


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

    Per-platform scoring: each video platform is scored against its own
    length-adjusted benchmark (inferred via platform_id + channel_category),
    then impression-weighted to a single A1 score. Drop-off shape is
    classified per-platform; the worst-scoring platform's diagnosis
    anchors the diagnostic message.

    Platforms without a true starts signal (non-Meta platforms missing
    video_views_3s) fall back to Q25-as-starts, but Q1 retention is
    dropped from scoring for those platforms — Q1 would evaluate to 1.0
    by construction and bias scores optimistic. Q2/Q3/Q4 weights are
    redistributed proportionally so the platform still gets a full score.
    """
    passed, reason = guard_attention(data)
    if not passed:
        return _guard_fail("A1", "Video Completion Quality", reason,
                           f"Insufficient data — {reason}.")

    passed, reason = check_has_quartile_data(data)
    if not passed:
        return _guard_fail("A1", "Video Completion Quality", reason,
                           "No video quartile data on any platform.")

    # Build channel_category lookup for format inference (same pattern
    # as D2/D4). First populated value per platform wins.
    channel_cat_by_platform: dict[str, str | None] = {}
    for line in data.media_plan:
        if line.platform_id and line.platform_id not in channel_cat_by_platform:
            channel_cat_by_platform[line.platform_id] = line.channel_category

    video_platforms = [p for p in data.platform_metrics if _has_video_activity(p)]

    # Weights for the Q25-absent case (Q1 isn't measurable with Q25-as-starts).
    # Proportionally redistribute A1_QUARTILE_WEIGHTS["q25"] across the
    # remaining three quartiles.
    q25_weight = A1_QUARTILE_WEIGHTS["q25"]
    remaining_total = (
        A1_QUARTILE_WEIGHTS["q50"]
        + A1_QUARTILE_WEIGHTS["q75"]
        + A1_QUARTILE_WEIGHTS["q100"]
    )
    rw_weights = {
        "q50":  A1_QUARTILE_WEIGHTS["q50"]  + q25_weight * A1_QUARTILE_WEIGHTS["q50"]  / remaining_total,
        "q75":  A1_QUARTILE_WEIGHTS["q75"]  + q25_weight * A1_QUARTILE_WEIGHTS["q75"]  / remaining_total,
        "q100": A1_QUARTILE_WEIGHTS["q100"] + q25_weight * A1_QUARTILE_WEIGHTS["q100"] / remaining_total,
    }

    platform_rows: list[dict[str, Any]] = []
    for p in video_platforms:
        starts, has_true_starts = _platform_video_starts(p)
        if starts <= 0:
            continue

        cc = channel_cat_by_platform.get(p.platform_id)
        creative_format, _ = infer_creative_format(p.platform_id, cc)
        length = _a1_length_for_format(creative_format)
        bench = VIDEO_LENGTH_BENCHMARKS[length]

        # Retention rates — capped at 1.0 for platforms that report more
        # Q25s than starts (autoplay de-dup quirks).
        q25_rate = min(p.video_q25 / starts, 1.0)
        q50_rate = min(p.video_q50 / starts, 1.0)
        q75_rate = min(p.video_q75 / starts, 1.0)
        q100_rate = min(p.video_q100 / starts, 1.0)

        # Score against this platform's length benchmark.
        if has_true_starts:
            w = A1_QUARTILE_WEIGHTS
            benchmark_score = (
                (q25_rate  / bench["q25"])  * w["q25"] +
                (q50_rate  / bench["q50"])  * w["q50"] +
                (q75_rate  / bench["q75"])  * w["q75"] +
                (q100_rate / bench["q100"]) * w["q100"]
            ) * 75
        else:
            # Q1 is a construction artifact when starts == Q25 — drop it.
            benchmark_score = (
                (q50_rate  / bench["q50"])  * rw_weights["q50"] +
                (q75_rate  / bench["q75"])  * rw_weights["q75"] +
                (q100_rate / bench["q100"]) * rw_weights["q100"]
            ) * 75

        plat_score = clamp(benchmark_score, 0, 100)

        # Length-aware shape classification. Skip HOOK_MISS/HOOK_FAILURE
        # when we don't have true starts — q25_rate is unreliable there.
        if has_true_starts:
            diagnosis, drop_value = _a1_classify_shape(
                q25_rate, q50_rate, q75_rate, q100_rate, bench,
            )
        else:
            # Evaluate only the Q2→Q3 and Q3→Q4 transitions; Q1→Q2
            # requires a trustworthy Q1 rate we don't have.
            expected = _a1_expected_drops(bench)
            observed_50_75 = 1.0 - safe_div(q75_rate, q50_rate, 1.0)
            observed_75_100 = 1.0 - safe_div(q100_rate, q75_rate, 1.0)
            if observed_50_75 - expected["q50_to_q75"] >= A1_SHAPE_ABNORMAL_DELTA:
                diagnosis, drop_value = "MESSAGE_FATIGUE", observed_50_75
            elif observed_75_100 - expected["q75_to_q100"] >= A1_SHAPE_ABNORMAL_DELTA:
                diagnosis, drop_value = "CTA_WEAKNESS", observed_75_100
            else:
                diagnosis, drop_value = "HEALTHY", 0.0

        platform_rows.append({
            "platform_id": p.platform_id,
            "creative_format": creative_format,
            "length": length,
            "has_true_starts": has_true_starts,
            "starts": starts,
            "impressions": p.impressions,
            "q25": p.video_q25,
            "q50": p.video_q50,
            "q75": p.video_q75,
            "q100": p.video_q100,
            "q25_rate": q25_rate,
            "q50_rate": q50_rate,
            "q75_rate": q75_rate,
            "q100_rate": q100_rate,
            "benchmark": bench,
            "score": plat_score,
            "diagnosis": diagnosis,
            "drop_value": drop_value,
        })

    if not platform_rows:
        return _guard_fail("A1", "Video Completion Quality", "no_starts",
                           "No video starts recorded on any platform — "
                           "cannot compute completion rates.")

    # Aggregate starts threshold — still honor MIN_VIDEO_STARTS but across
    # the per-platform data we just built.
    total_starts = sum(r["starts"] for r in platform_rows)
    if total_starts < MIN_VIDEO_STARTS:
        return _guard_fail("A1", "Video Completion Quality", "below_min_starts",
                           "Not enough video starts to produce stable "
                           "completion rates yet.")

    # Impression-weighted aggregation
    total_imps = sum(r["impressions"] for r in platform_rows)
    if total_imps <= 0:
        return _guard_fail("A1", "Video Completion Quality", "no_impressions",
                           "No impressions on any video platform — cannot "
                           "aggregate completion scores.")

    score = sum(r["score"] * r["impressions"] for r in platform_rows) / total_imps
    weighted_q100_rate = sum(
        r["q100_rate"] * r["impressions"] for r in platform_rows
    ) / total_imps
    score = round(score, 1)
    status = status_band(score)

    # Anchor the diagnostic on the worst diagnosis (non-HEALTHY wins
    # even if another healthy platform has a lower score), breaking ties
    # by lowest per-platform score.
    unhealthy = [r for r in platform_rows if r["diagnosis"] != "HEALTHY"]
    if unhealthy:
        worst = min(unhealthy, key=lambda r: r["score"])
    else:
        worst = min(platform_rows, key=lambda r: r["score"])

    template = A1_DIAGNOSES.get(worst["diagnosis"], A1_DIAGNOSES["HEALTHY"])
    diagnostic = template.format(
        platform=worst["platform_id"],
        length=worst["length"],
        q25_pct=format_pct(worst["q25_rate"]),
        drop=worst["drop_value"],
    )

    # Multi-platform context suffix — helpful when one platform is
    # dragging but others are healthy. Keep concise.
    if len(platform_rows) > 1 and worst["diagnosis"] != "HEALTHY":
        healthy_count = sum(1 for r in platform_rows if r["diagnosis"] == "HEALTHY")
        if healthy_count > 0:
            diagnostic += (
                f" ({healthy_count} of {len(platform_rows)} video platforms "
                "are tracking their length benchmarks.)"
            )

    # Note when any platform relied on the Q25-as-starts proxy.
    proxy_plats = [r["platform_id"] for r in platform_rows if not r["has_true_starts"]]
    if proxy_plats:
        diagnostic += (
            f" Note: {', '.join(proxy_plats)} had no native video-start "
            "column — Q1 retention is excluded from scoring on those "
            "platforms until the transformation layer carries a true "
            "starts signal."
        )

    # Report the dominant platform's benchmark in raw_value/benchmark so
    # the UI has a single reference point — but the real story is in the
    # per-platform inputs.
    dominant = max(platform_rows, key=lambda r: r["impressions"])

    return SignalResult(
        id="A1",
        name="Video Completion Quality",
        score=score,
        status=status,
        raw_value=round(weighted_q100_rate, 3),
        benchmark=dominant["benchmark"]["q100"],
        floor=0.0,
        diagnostic=diagnostic,
        guard_passed=True,
        inputs={
            "weighted_q100_rate": round(weighted_q100_rate, 3),
            "total_starts": total_starts,
            # `diagnosis` is kept as a top-level alias for the overall-signal
            # diagnostic (always == worst-platform's classification) so
            # existing frontend consumers keep working. Per-platform
            # diagnoses are in `platforms[platform_id].diagnosis`.
            "diagnosis": worst["diagnosis"],
            "worst_platform": worst["platform_id"],
            "worst_diagnosis": worst["diagnosis"],
            "dominant_platform": dominant["platform_id"],
            "dominant_length": dominant["length"],
            "platforms": {
                r["platform_id"]: {
                    "creative_format": r["creative_format"],
                    "length": r["length"],
                    "has_true_starts": r["has_true_starts"],
                    "starts": r["starts"],
                    "impressions": r["impressions"],
                    "q25": r["q25"], "q50": r["q50"],
                    "q75": r["q75"], "q100": r["q100"],
                    "q25_rate": round(r["q25_rate"], 3),
                    "q50_rate": round(r["q50_rate"], 3),
                    "q75_rate": round(r["q75_rate"], 3),
                    "q100_rate": round(r["q100_rate"], 3),
                    "benchmark": r["benchmark"],
                    "score": round(r["score"], 1),
                    "diagnosis": r["diagnosis"],
                }
                for r in platform_rows
            },
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
    """A3: Viewability — viewable impressions vs MRC/IAB standard.

    Scored on aggregate `viewable / measured` across all platforms that
    report viewability. The benchmark (A3_BENCHMARK=80) is calibrated to
    "good actual performance" rather than the IAB 70% legal-minimum; a
    campaign scraping just past IAB compliance lands in WATCH.

    Platforms that don't report viewability (most video/feed platforms)
    are excluded from the measurement base and flagged in the diagnostic.
    Measurement coverage (measured / total impressions) is surfaced in
    inputs and noted when it falls below A3_COVERAGE_NOTE_THRESHOLD.
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
    plats_without_view = [
        p for p in data.platform_metrics
        if p.viewability_measured <= 0 and p.impressions > 0
    ]
    total_measured = sum(p.viewability_measured for p in plats_with_view)
    total_viewed = sum(p.viewability_viewed for p in plats_with_view)
    total_impressions_all = sum(p.impressions for p in data.platform_metrics)
    measurement_coverage = safe_div(total_measured, total_impressions_all, 0.0)

    viewability_rate = safe_div(total_viewed, total_measured, 0)
    viewability_pct = viewability_rate * 100

    score = normalize_linear(viewability_pct, A3_FLOOR, A3_BENCHMARK)
    status = status_band(score)

    # Per-platform viewability rates + score context
    per_platform_rows = [
        {
            "platform_id": p.platform_id,
            "measured": p.viewability_measured,
            "viewed": p.viewability_viewed,
            "rate": safe_div(p.viewability_viewed, p.viewability_measured, 0.0),
        }
        for p in plats_with_view
    ]

    # Worst-platform anchor — only meaningful when there's variation
    # between platforms (single-platform A3 → no variance to anchor on).
    worst_suffix = ""
    worst_row = None
    if status != StatusBand.STRONG and len(per_platform_rows) > 1:
        worst_row = min(per_platform_rows, key=lambda r: r["rate"])
        best_row = max(per_platform_rows, key=lambda r: r["rate"])
        if worst_row["rate"] < best_row["rate"]:
            worst_suffix = (
                f"{worst_row['platform_id']} at "
                f"{format_pct(worst_row['rate'])} is the drag. "
            )

    template = A3_MESSAGES.get(status, A3_MESSAGES[StatusBand.WATCH])
    diagnostic = template.format(
        rate=format_pct(viewability_rate),
        benchmark=A3_BENCHMARK,
        worst_suffix=worst_suffix,
    )

    # Coverage note — when most of the campaign's impressions aren't being
    # measured, the score represents a small slice; flag so planners
    # don't over-interpret the result.
    coverage_note_applied = False
    if measurement_coverage < A3_COVERAGE_NOTE_THRESHOLD and total_impressions_all > 0:
        diagnostic += (
            f" Note: viewability was measured on only "
            f"{format_pct(measurement_coverage)} of total impressions — "
            "the score reflects a limited inventory sample."
        )
        coverage_note_applied = True

    # Unreported-platform list — helps planners see WHY coverage is low.
    # Keep concise; only include platforms that ran meaningful volume.
    unreported = [
        p.platform_id for p in plats_without_view if p.impressions > 10_000
    ]
    if unreported:
        diagnostic += (
            f" Viewability not reported by: {', '.join(unreported)}."
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
            "total_impressions": total_impressions_all,
            "measurement_coverage": round(measurement_coverage, 3),
            "coverage_note_applied": coverage_note_applied,
            "worst_platform": worst_row["platform_id"] if worst_row else None,
            "unreported_platforms": unreported,
            "platforms": {
                r["platform_id"]: {
                    "measured": r["measured"],
                    "viewed": r["viewed"],
                    "rate": round(r["rate"], 3),
                }
                for r in per_platform_rows
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
    excluded_no_metric: list[str] = []  # Platforms like display with None benchmark
    for p in data.platform_metrics:
        if p.impressions <= 0:
            continue

        bench_cfg, used_default = get_a4_benchmark(p.platform_id)
        if bench_cfg is None:
            # Platform has no meaningful focused-view metric (e.g. display).
            # Record it for transparency but exclude from scoring.
            excluded_no_metric.append(p.platform_id)
            continue

        pid_lower = (p.platform_id or "").lower()
        is_meta = "facebook" in pid_lower or "meta" in pid_lower

        if is_meta:
            meaningful = p.thruplay
            metric_label = "ThruPlay (15s+)"
        else:
            meaningful = p.video_views_3s
            metric_label = "3s-view"

        if meaningful <= 0:
            continue

        rate = meaningful / p.impressions
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
            "used_default_benchmark": used_default,
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
    if status == StatusBand.STRONG:
        diagnostic = template.format(rate=format_pct(weighted_rate))
    else:
        diagnostic = template.format(
            rate=format_pct(weighted_rate),
            worst_platform=worst["platform_id"],
            worst_rate=format_pct(worst["rate"]),
            worst_metric=worst["metric_label"],
            worst_benchmark=format_pct(worst["benchmark"]),
        )

    # Default-benchmark transparency — flag any platform scored against
    # the fallback benchmark so planners know the score isn't
    # platform-calibrated.
    default_plats = [r["platform_id"] for r in platform_rows if r["used_default_benchmark"]]
    if default_plats:
        diagnostic += (
            f" Note: {', '.join(default_plats)} scored against default "
            "benchmark — no platform-specific calibration yet."
        )

    # Surface display (or other no-metric) exclusions so planners aren't
    # confused about why a running line doesn't contribute to A4.
    if excluded_no_metric:
        diagnostic += (
            f" Excluded from A4: {', '.join(excluded_no_metric)} "
            "(no meaningful focused-view metric for this inventory)."
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
            "worst_platform": worst["platform_id"],
            "platforms_scored_against_default": default_plats,
            "excluded_no_metric": excluded_no_metric,
            "platforms": {
                r["platform_id"]: {
                    "rate": round(r["rate"], 3),
                    "benchmark": r["benchmark"],
                    "floor": r["floor"],
                    "score": round(r["score"], 1),
                    "metric": r["metric_label"],
                    "impressions": r["impressions"],
                    "meaningful_views": r["meaningful_views"],
                    "used_default_benchmark": r["used_default_benchmark"],
                }
                for r in platform_rows
            },
        },
    )


# ── Signal A5: Creative Fatigue Index ──────────────────────────────


def compute_a5_creative_fatigue(data: CampaignData) -> SignalResult:
    """A5: Creative Fatigue Index — per-platform trend of attention metric.

    Phase 2 scope: platform-level trend (not per-creative). Per-creative
    fatigue requires creative_variant_id on fact_digital_daily which isn't
    in the transformation yet — flagged as a follow-up.

    Method (calibrated 2026-04-20):
        1. Group daily_metrics by (platform_id, date)
        2. For each platform, compute thruplay ÷ impressions per day
        3. Drop days below A5_MIN_DAY_IMP_FRACTION of that platform's
           window-mean impressions (filters reporting gaps / dark days)
        4. Fit OLS slope on the last MIN_DAYS_FOR_FATIGUE qualifying days
        5. Express slope as % change per day relative to window mean
        6. Classify per-platform; impression-weight into overall score
        7. Anchor diagnostic on the worst platform when the overall band
           is EARLY/MODERATE/SEVERE (matches A1/A3/A4 pattern)
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

    # Group daily metrics by (platform_id, date). A DailyMetrics row can
    # carry multiple line_ids for the same platform/date, so we aggregate.
    by_platform_date: dict[str, dict[Any, list[DailyMetrics]]] = defaultdict(
        lambda: defaultdict(list)
    )
    for d in data.daily_metrics:
        by_platform_date[d.platform_id][d.date].append(d)

    # Per-platform fatigue computation
    platform_results: dict[str, dict[str, Any]] = {}
    skipped: list[dict[str, Any]] = []

    for platform_id, by_date in by_platform_date.items():
        # Build the full series for this platform
        raw_series: list[tuple[Any, float, int, str]] = []  # (date, rate, imps, metric)
        for dt in sorted(by_date.keys()):
            rate, imps, metric = _daily_attention_rate(by_date[dt])
            if rate is not None and imps > 0:
                raw_series.append((dt, rate, imps, metric))

        if len(raw_series) < MIN_DAYS_FOR_FATIGUE:
            skipped.append({
                "platform_id": platform_id,
                "reason": "insufficient_days",
                "days_available": len(raw_series),
            })
            continue

        # Apply volume floor using the platform's window-mean impressions.
        # This filters dark days / reporting gaps that would otherwise
        # dominate a low-volume OLS fit.
        mean_imps = statistics.mean(s[2] for s in raw_series)
        imp_floor = mean_imps * A5_MIN_DAY_IMP_FRACTION
        filtered = [s for s in raw_series if s[2] >= imp_floor]

        if len(filtered) < MIN_DAYS_FOR_FATIGUE:
            skipped.append({
                "platform_id": platform_id,
                "reason": "insufficient_days_after_volume_filter",
                "days_above_floor": len(filtered),
                "imp_floor": round(imp_floor, 0),
            })
            continue

        window = filtered[-MIN_DAYS_FOR_FATIGUE:]
        values = [v for _, v, _, _ in window]
        window_imps = sum(i for _, _, i, _ in window)
        mean_v = statistics.mean(values)

        if mean_v <= 0:
            skipped.append({
                "platform_id": platform_id,
                "reason": "zero_attention_baseline",
            })
            continue

        slope = _linear_slope(values)
        daily_change_pct = (slope / mean_v) * 100
        fatigue = _classify_fatigue(daily_change_pct)
        p_score = float(A5_FATIGUE_SCORES[fatigue])

        # Detect mid-window metric switches (e.g. thruplay starts reporting
        # mid-flight) — would introduce a step change that looks like slope.
        metrics_in_window = {m for _, _, _, m in window}
        metric_switched = len(metrics_in_window) > 1

        platform_results[platform_id] = {
            "fatigue_band": fatigue,
            "daily_change_pct": round(daily_change_pct, 3),
            "window_mean_rate": round(mean_v, 4),
            "window_impressions": window_imps,
            "slope_raw": round(slope, 6),
            "score": round(p_score, 1),
            "metric": next(iter(metrics_in_window)) if not metric_switched else "mixed",
            "metric_switched": metric_switched,
            "window_days": len(window),
        }

    if not platform_results:
        # No platform had enough clean days to fit a trend
        return _guard_fail("A5", "Creative Fatigue", "insufficient_daily_series",
                           f"No platform has {MIN_DAYS_FOR_FATIGUE}+ days of "
                           f"attention data above the volume floor — trend "
                           f"cannot be computed yet.")

    # Impression-weighted roll-up
    total_imps = sum(r["window_impressions"] for r in platform_results.values())
    if total_imps <= 0:
        return _guard_fail("A5", "Creative Fatigue", "zero_window_impressions",
                           "No impressions in the trend window — cannot compute.")

    weighted_score = sum(
        r["score"] * r["window_impressions"] for r in platform_results.values()
    ) / total_imps
    weighted_change = sum(
        r["daily_change_pct"] * r["window_impressions"]
        for r in platform_results.values()
    ) / total_imps

    overall_band = _classify_fatigue(weighted_change)
    score = float(weighted_score)

    # Grace period for young flights — don't over-punish fresh creative
    if data.flight.elapsed_days < 14:
        score = max(score, 65.0)

    status = status_band(score)

    # Anchor diagnostic on the worst-fatiguing platform when overall is
    # EARLY/MODERATE/SEVERE. For NONE, add the worst platform's band only
    # if someone is trending (context, not alarm).
    band_order = {"NONE": 0, "EARLY": 1, "MODERATE": 2, "SEVERE": 3}
    worst_pid, worst = max(
        platform_results.items(),
        key=lambda kv: (band_order[kv[1]["fatigue_band"]], -kv[1]["daily_change_pct"]),
    )
    if overall_band == "NONE":
        worst_suffix = ""
    else:
        worst_suffix = (
            f"; {worst_pid} worst at {worst['daily_change_pct']:.1f}%/day "
            f"({worst['fatigue_band']})"
        )

    template = A5_MESSAGES[overall_band]
    diagnostic = template.format(
        days=MIN_DAYS_FOR_FATIGUE,
        slope=weighted_change,
        worst_suffix=worst_suffix,
    )

    # Surface skipped platforms + metric-switch caveats for transparency
    notes: list[str] = []
    if skipped:
        note_bits = []
        for s in skipped:
            note_bits.append(f"{s['platform_id']} ({s['reason']})")
        notes.append("Platforms excluded from trend: " + ", ".join(note_bits) + ".")
    switched = [pid for pid, r in platform_results.items() if r["metric_switched"]]
    if switched:
        notes.append(
            "Metric switched mid-window on: " + ", ".join(switched) +
            " — trend may reflect reporting changes, not audience behaviour."
        )
    if notes:
        diagnostic = diagnostic + " " + " ".join(notes)

    return SignalResult(
        id="A5",
        name="Creative Fatigue",
        score=round(score, 1),
        status=status,
        raw_value=round(weighted_change, 2),
        benchmark=A5_FATIGUE_THRESHOLDS["NONE"],
        floor=A5_FATIGUE_THRESHOLDS["MODERATE"],
        diagnostic=diagnostic,
        guard_passed=True,
        inputs={
            "fatigue_band": overall_band,
            "daily_change_pct": round(weighted_change, 3),
            "window_days": MIN_DAYS_FOR_FATIGUE,
            "platforms": platform_results,
            "skipped_platforms": skipped,
            "worst_platform": worst_pid,
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
