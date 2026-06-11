"""Score normalization utilities.

All signals produce raw metric values that need to be mapped to a 0-100
score. This module provides the normalization functions used across all
signal computations.

Scoring philosophy (from spec):
    - 0-100 scale, continuous
    - floor = minimum acceptable value (score 0 below this)
    - benchmark = expected/good performance (score ~75 at benchmark)
    - Scores above benchmark continue to climb but with diminishing returns
    - STRONG >= 70, WATCH >= 40, ACTION < 40
"""

from __future__ import annotations

from backend.services.diagnostics.models import StatusBand, status_band


def clamp(value: float, lo: float = 0.0, hi: float = 100.0) -> float:
    """Clamp a value to [lo, hi]."""
    return max(lo, min(hi, value))


def normalize_linear(
    value: float,
    floor: float,
    benchmark: float,
    *,
    score_at_benchmark: float = 75.0,
    max_score: float = 100.0,
) -> float:
    """Linear normalization: floor→0, benchmark→score_at_benchmark.

    Values above benchmark continue linearly up to max_score, but the
    slope flattens (75→100 range maps to benchmark→2×benchmark overshoot).
    Values below floor clamp to 0.

    Args:
        value: The raw metric value.
        floor: Below this → score 0.
        benchmark: At this → score_at_benchmark (default 75).
        score_at_benchmark: Score assigned at the benchmark value.
        max_score: Hard ceiling.

    Returns:
        Score in [0, max_score].
    """
    if benchmark <= floor:
        # Degenerate case — avoid division by zero
        return max_score if value >= benchmark else 0.0

    if value <= floor:
        return 0.0

    if value <= benchmark:
        # Linear from floor→0 to benchmark→score_at_benchmark
        proportion = (value - floor) / (benchmark - floor)
        return clamp(proportion * score_at_benchmark, 0.0, max_score)

    # Above benchmark: slower climb toward 100
    overshoot = value - benchmark
    headroom = max_score - score_at_benchmark  # typically 25 points
    # Full headroom at 2× the floor-to-benchmark range above benchmark
    overshoot_range = benchmark - floor
    if overshoot_range <= 0:
        return max_score
    bonus = (overshoot / overshoot_range) * headroom
    return clamp(score_at_benchmark + bonus, 0.0, max_score)


def normalize_inverse(
    value: float,
    target: float,
    ceiling: float,
    *,
    score_at_target: float = 75.0,
    max_score: float = 100.0,
) -> float:
    """Inverse normalization for metrics where lower is better.

    Used for CPA, frequency overshoot, etc.
    At target → score_at_target, at ceiling → 0, below target → climbs to max.

    Args:
        value: The raw metric (lower = better).
        target: Ideal value → score_at_target.
        ceiling: Worst acceptable → score 0.
        score_at_target: Score at the target value.
        max_score: Hard ceiling score.

    Returns:
        Score in [0, max_score].
    """
    if ceiling <= target:
        return max_score if value <= target else 0.0

    if value >= ceiling:
        return 0.0

    if value >= target:
        # Linear decline from target→score_at_target to ceiling→0
        proportion = (ceiling - value) / (ceiling - target)
        return clamp(proportion * score_at_target, 0.0, max_score)

    # Below target (better than ideal): climb toward 100
    undershoot = target - value
    headroom = max_score - score_at_target
    undershoot_range = ceiling - target
    if undershoot_range <= 0:
        return max_score
    bonus = (undershoot / undershoot_range) * headroom
    return clamp(score_at_target + bonus, 0.0, max_score)


def normalize_ratio(
    actual: float,
    expected: float,
    *,
    floor_ratio: float = 0.5,
    benchmark_ratio: float = 1.0,
    score_at_benchmark: float = 75.0,
    max_score: float = 100.0,
) -> float:
    """Normalize an actual/expected ratio.

    Convenience wrapper around normalize_linear that works on ratios.
    Floor ratio (e.g. 0.5 = 50% of expected) → score 0.
    Benchmark ratio (e.g. 1.0 = on target) → score 75.

    Args:
        actual: Numerator.
        expected: Denominator (what was planned/expected).
        floor_ratio: Ratio below which score is 0.
        benchmark_ratio: Ratio that maps to score_at_benchmark.

    Returns:
        Score in [0, max_score].
    """
    if expected <= 0:
        return 0.0

    ratio = actual / expected
    return normalize_linear(
        ratio,
        floor=floor_ratio,
        benchmark=benchmark_ratio,
        score_at_benchmark=score_at_benchmark,
        max_score=max_score,
    )


def format_pct(value: float, decimals: int = 1) -> str:
    """Format a ratio as a percentage string, e.g. 0.872 → '87.2%'."""
    return f"{value * 100:.{decimals}f}%"


def format_number(value: float | int) -> str:
    """Format a number with commas, e.g. 39098 → '39,098'."""
    if isinstance(value, float) and value == int(value):
        value = int(value)
    return f"{value:,}"


def safe_div(numerator: float, denominator: float, default: float = 0.0) -> float:
    """Safe division — returns default when denominator is 0."""
    if denominator == 0:
        return default
    return numerator / denominator


# ── Platform display labels (AI-115 plain-language pass) ────────────
#
# Diagnostic messages used to interpolate raw platform_id values
# ("google_ads", "stackadapt") straight into user-facing sentences.
# This map mirrors the frontend's `platformLabel` in lib/utils.ts —
# keep the two in sync when a new platform lands. Unknown ids fall
# back to underscore-stripped Title Case so new platforms degrade
# gracefully instead of leaking snake_case.
#
# Use at MESSAGE-FORMAT TIME ONLY — `inputs` payloads keep raw ids so
# the evidence layer and any programmatic consumers stay stable.

PLATFORM_LABELS: dict[str, str] = {
    "meta": "Meta",
    "facebook": "Meta",
    "google_ads": "Google Ads",
    "stackadapt": "StackAdapt",
    "linkedin": "LinkedIn",
    "tiktok": "TikTok",
    "snapchat": "Snapchat",
    "perion": "Perion/DOOH",
    "reddit": "Reddit",
    "pinterest": "Pinterest",
}


def platform_label(platform_id: str | None) -> str:
    """Human display label for a platform id, for diagnostic copy."""
    if not platform_id:
        return "unknown platform"
    return PLATFORM_LABELS.get(
        platform_id.lower(),
        platform_id.replace("_", " ").title(),
    )
