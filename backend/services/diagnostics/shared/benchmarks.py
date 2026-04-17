"""Platform-specific benchmarks for diagnostic signals.

Benchmark sources:
    - CIP's existing benchmarks table (political advertising baselines)
    - Spenkuch & Toniatti (2018) — frequency diminishing returns
    - PNAS 2023 — persuasion messaging exposure research
    - Platform-specific internal benchmarks (StackAdapt, Meta)
    - Phase 0 validation against campaign 25042

These are initial values — designed to be calibrated over time
against actual campaign outcomes via historical backfill.

Conservative by design: benchmarks are intentionally set to the
"good but achievable" level, not aspirational. Under-promise,
over-deliver.
"""

from __future__ import annotations


# ── D1: Reach Attainment ────────────────────────────────────────────

D1_BENCHMARK = 1.0      # 100% of planned reach at this point in flight
D1_FLOOR = 0.5           # 50% — critically behind

# Cross-platform overlap estimates (conservative)
OVERLAP_FACTORS = {
    1: 0.0,              # Single platform — no overlap
    2: 0.15,             # 2 platforms — 15% estimated overlap
    3: 0.20,             # 3 platforms — 20%
    4: 0.30,             # 4+ platforms — 30%
}

def get_overlap_factor(n_platforms: int) -> float:
    """Estimated cross-platform audience overlap."""
    if n_platforms <= 1:
        return 0.0
    if n_platforms in OVERLAP_FACTORS:
        return OVERLAP_FACTORS[n_platforms]
    return 0.35  # 5+ platforms


# ── D2: Frequency Adequacy ──────────────────────────────────────────

# Effective frequency bands by creative format
# {format: {"min": floor, "optimal": sweet spot, "max": fatigue ceiling}}
FREQ_BANDS = {
    "video_short":  {"min": 3, "optimal": 5, "max": 8},     # 6-15s
    "video_medium": {"min": 2, "optimal": 4, "max": 7},     # 16-30s
    "video_long":   {"min": 2, "optimal": 3, "max": 5},     # 31s+
    "static":       {"min": 4, "optimal": 6, "max": 10},
    "audio_short":  {"min": 3, "optimal": 5, "max": 8},     # <30s
    "audio_long":   {"min": 2, "optimal": 4, "max": 6},     # 30s+
    "dooh":         {"min": 3, "optimal": 8, "max": 15},
}

# Default band when format is unknown
DEFAULT_FREQ_BAND = {"min": 3, "optimal": 5, "max": 8}


def get_freq_band(creative_format: str | None) -> dict[str, int]:
    """Look up the frequency band for a creative format."""
    if not creative_format:
        return DEFAULT_FREQ_BAND
    return FREQ_BANDS.get(creative_format, DEFAULT_FREQ_BAND)


# ── D3: Frequency Distribution Health ───────────────────────────────

D3_BENCHMARK = 0.85      # 85% frequency efficiency
D3_FLOOR = 0.50


# ── D4: Incremental Reach by Platform ───────────────────────────────

# No fixed benchmark — D4 compares each platform's reach share
# vs spend share. A ratio of 1.0 = proportional reach.
D4_BENCHMARK_RATIO = 1.0
D4_FLOOR_RATIO = 0.3


# ── A1: Video Completion Quality ────────────────────────────────────

# Length-adjusted quartile benchmarks (what % of starters should reach each point)
VIDEO_LENGTH_BENCHMARKS = {
    "6s":  {"q25": 0.85, "q50": 0.75, "q75": 0.65, "q100": 0.55},
    "15s": {"q25": 0.70, "q50": 0.55, "q75": 0.42, "q100": 0.30},
    "30s": {"q25": 0.60, "q50": 0.42, "q75": 0.30, "q100": 0.20},
    "60s": {"q25": 0.50, "q50": 0.35, "q75": 0.22, "q100": 0.12},
    "90s": {"q25": 0.45, "q50": 0.28, "q75": 0.15, "q100": 0.08},
}

# Quartile weights for A1 scoring
A1_QUARTILE_WEIGHTS = {
    "q25": 0.10,
    "q50": 0.20,
    "q75": 0.35,
    "q100": 0.35,
}


# ── A2: Audio Completion Quality ────────────────────────────────────

AUDIO_LENGTH_BENCHMARKS = {
    "15s": {"q25": 0.90, "q50": 0.85, "q75": 0.80, "q95": 0.75},
    "30s": {"q25": 0.85, "q50": 0.78, "q75": 0.70, "q95": 0.65},
    "60s": {"q25": 0.80, "q50": 0.70, "q75": 0.60, "q95": 0.50},
}


# ── A3: Viewability ─────────────────────────────────────────────────

A3_BENCHMARK = 70        # 70% viewability
A3_FLOOR = 40


# ── A4: Focused View / ThruPlay Rate ───────────────────────────────

# Platform-specific benchmarks (Phase 0 validated)
A4_BENCHMARKS = {
    "meta":               {"benchmark": 0.08, "floor": 0.02},
    "facebook":           {"benchmark": 0.08, "floor": 0.02},
    "tiktok":             {"benchmark": 0.25, "floor": 0.08},
    "snapchat":           {"benchmark": 0.60, "floor": 0.25},
    "youtube":            {"benchmark": 0.30, "floor": 0.10},
    "google_ads":         {"benchmark": 0.30, "floor": 0.10},
    "linkedin":           {"benchmark": 0.30, "floor": 0.10},
    "reddit":             {"benchmark": 0.15, "floor": 0.05},
    "pinterest":          {"benchmark": 0.20, "floor": 0.06},
    "stackadapt_ctv":     {"benchmark": 0.90, "floor": 0.70},
    "stackadapt_display": {"benchmark": 0.15, "floor": 0.05},
    "stackadapt":         {"benchmark": 0.15, "floor": 0.05},
}


def get_a4_benchmark(platform_id: str) -> dict[str, float]:
    """Look up A4 focused view benchmark for a platform."""
    key = platform_id.lower().replace(" ", "_")
    return A4_BENCHMARKS.get(key, {"benchmark": 0.20, "floor": 0.06})


# ── R1: Engagement Quality Ratio ────────────────────────────────────

R1_BENCHMARK = 0.55      # 55% of engagements being high-value
R1_FLOOR = 0.20


# ── R2: Earned Amplification ────────────────────────────────────────

R2_BENCHMARK = 0.05      # 5% earned-to-paid ratio
R2_FLOOR = 0.0


# ── R3: Landing Page Depth ──────────────────────────────────────────

R3_BENCHMARK = 55         # 55% engaged session rate with healthy scroll
R3_FLOOR = 20


# ── Persuasion Pillar Weights ───────────────────────────────────────

PERSUASION_PILLAR_WEIGHTS = {
    "distribution": 0.35,
    "attention": 0.40,
    "resonance": 0.25,
}

# Signal weights within Distribution pillar
DISTRIBUTION_SIGNAL_WEIGHTS = {
    "D1": 0.40,
    "D2": 0.30,
    "D3": 0.15,
    "D4": 0.15,
}

# Signal weights within Attention pillar.
#
# The spec defines format-conditional weights (video+audio+static, video-only,
# audio-only, etc.). In practice nearly every PB persuasion campaign is video
# or video+static. Default to the video-only branch:
#   A1=0.35, A3=0.15, A4=0.30, A5=0.20, A2=null (redistributes).
# When A2 becomes available, include it at 0.20 and scale A1/A4/A5 down pro
# rata (handled by the redistribute-to-active pattern in the pillar builder).
ATTENTION_SIGNAL_WEIGHTS = {
    "A1": 0.35,    # Video completion quality
    "A2": 0.20,    # Audio completion quality
    "A3": 0.15,    # Viewability
    "A4": 0.30,    # Focused view / time-based attention
    "A5": 0.20,    # Creative fatigue
}

# Fatigue score mapping (A5)
A5_FATIGUE_SCORES = {
    "NONE":     90,
    "EARLY":    70,
    "MODERATE": 45,
    "SEVERE":   15,
}

# Fatigue classification thresholds (daily % change in attention metric).
# Spec: > -0.5 = NONE; -0.5 to -1.5 = EARLY; -1.5 to -3.0 = MODERATE; < -3.0 = SEVERE
A5_FATIGUE_THRESHOLDS = {
    "NONE":     -0.5,
    "EARLY":    -1.5,
    "MODERATE": -3.0,
}


# ── Resonance Signal Weights ──────────────────────────────────────

# R1 carries most weight as it's the most actionable signal with full
# data coverage. R2 is included at 0.25 but will guard-fail until Phase 3
# (earned data), redistributing its share to R1 and R3 pro rata.
# R3 depends on GA4 configuration — guard-fails if no GA4 URLs mapped.
RESONANCE_SIGNAL_WEIGHTS = {
    "R1": 0.45,    # Engagement quality ratio
    "R2": 0.25,    # Earned amplification (Phase 3)
    "R3": 0.30,    # Landing page engagement depth
}


# ── Conversion Pillar Weights ──────────────────────────────────────

CONVERSION_PILLAR_WEIGHTS = {
    "acquisition": 0.30,
    "funnel": 0.40,
    "quality": 0.30,
}

# Signal weights within Acquisition pillar
ACQUISITION_SIGNAL_WEIGHTS = {
    "C1": 0.45,    # CPA vs friction-adjusted target
    "C2": 0.35,    # Volume trajectory
    "C3": 0.20,    # CPA trend (deterioration detection)
}


# ── F1: Click-Through Rate (CTR) ────────────────────────────────────
#
# Platform-specific CTR benchmarks calibrated for political-advertising
# campaigns (conservative — PB's Phase 0 data informs these baselines).
# Non-clickable placements (CTV, DOOH, audio where applicable) return
# None and F1 guard-fails for spend on those platforms.
#
# Benchmark = "good but achievable" CTR; floor = critically underperforming.

F1_CTR_BENCHMARKS: dict[str, dict[str, float] | None] = {
    "meta":               {"benchmark": 0.009, "floor": 0.0035},   # 0.90% / 0.35%
    "facebook":           {"benchmark": 0.009, "floor": 0.0035},
    "instagram":          {"benchmark": 0.009, "floor": 0.0035},
    "linkedin":           {"benchmark": 0.006, "floor": 0.0025},
    "tiktok":             {"benchmark": 0.012, "floor": 0.005},
    "snapchat":           {"benchmark": 0.008, "floor": 0.003},
    "google_ads":         {"benchmark": 0.025, "floor": 0.010},
    "youtube":            {"benchmark": 0.004, "floor": 0.0015},
    "reddit":             {"benchmark": 0.003, "floor": 0.0012},
    "pinterest":          {"benchmark": 0.004, "floor": 0.0015},
    "stackadapt":         {"benchmark": 0.0012, "floor": 0.0005},
    "stackadapt_display": {"benchmark": 0.0012, "floor": 0.0005},
    "stackadapt_ctv":     None,   # Non-clickable
    "perion":             None,   # DOOH — no CTR
    "hivestack":          None,
}

F1_DEFAULT_BENCHMARK = {"benchmark": 0.006, "floor": 0.002}


def get_f1_benchmark(platform_id: str | None) -> dict[str, float] | None:
    """Look up CTR benchmark for a platform. Returns None for non-clickable
    placements (CTV, DOOH) so F1 can guard-fail that platform's share."""
    if not platform_id:
        return F1_DEFAULT_BENCHMARK
    key = platform_id.lower().replace(" ", "_")
    if key in F1_CTR_BENCHMARKS:
        return F1_CTR_BENCHMARKS[key]
    return F1_DEFAULT_BENCHMARK


# ── F2: Landing Page Load Rate ──────────────────────────────────────
#
# Fraction of clicks that register a landing_page_view. Captures
# bounce-before-load, slow LP load times, tracking mis-implementation,
# and mobile app intercepts (e.g. Meta's in-app browser quirks).
# Only applies to Arch A (landing page) traffic.

F2_BENCHMARK = 0.85      # 85% of clicks land on the page
F2_FLOOR = 0.50


# ── F3: Scroll / Form Discovery Rate ────────────────────────────────
#
# Composite of GA4 scroll_rate (scrolls/sessions) and form-discovery
# inferred from form position relative to the fold on mobile (from
# FFS inputs: below_fold_mobile flag).
#
# F3 = 0.70 * scroll_rate_score + 0.30 * form_discovery_score

F3_SCROLL_BENCHMARK = 0.50      # 50% of sessions register a scroll event
F3_SCROLL_FLOOR = 0.15
F3_SCROLL_WEIGHT = 0.70          # Weight of scroll_rate within F3
F3_DISCOVERY_WEIGHT = 0.30       # Weight of form-position discovery within F3

# Expected form-discovery rate (fraction of sessions that will reach the
# form) given its position on the page. Used as the normalization target
# for the discovery component.
F3_FORM_DISCOVERY_BY_POSITION = {
    "above_fold": 0.80,          # Form visible without scrolling
    "mid_page":   0.50,          # Form mid-page — typical
    "below_fold": 0.30,          # Form requires scroll to reach
    "unknown":    0.50,          # Default when FFS inputs missing
}


# ── F4: Form Completion Rate ────────────────────────────────────────
#
# Fraction of form_starts that become form_submits. FFS adjusts the
# expected completion rate — high-friction forms get a lower benchmark.
#
# Landing-page forms:
#   expected_completion = F4_BASE_COMPLETION_BENCHMARK * exp(-0.012 * ffs)
# In-platform forms (Meta Lead, LinkedIn Lead Gen, TikTok Instant):
#   expected_completion = min(base * F4_PLATFORM_FORM_BOOST, F4_PLATFORM_FORM_CAP)

F4_BASE_COMPLETION_BENCHMARK = 0.65   # 65% baseline for a zero-friction LP form
F4_PLATFORM_FORM_BOOST = 1.4           # In-platform forms convert ~40% better
F4_PLATFORM_FORM_CAP = 0.85            # Ceiling on in-platform form expectation
F4_COMPLETION_FLOOR = 0.20             # Below 20% completion = critical


# ── F5: Post-Conversion Activation ──────────────────────────────────
#
# Fraction of conversions that trigger a "secondary event" (key_event in
# GA4 terms — email open, account activation, second page view, etc.).
# High activation = the lead was engaged, not a drive-by form fill.

F5_ACTIVATION_BENCHMARK = 0.20   # 20% of conversions take a second meaningful action
F5_ACTIVATION_FLOOR = 0.0         # Any activation is positive


# ── Funnel Pillar Signal Weights (architecture-conditional) ─────────
#
# Arch A: Landing page flow. F3 (scroll/form discovery) and F4 (form
# completion) carry most weight — those are where LP-driven campaigns
# live or die.
FUNNEL_SIGNAL_WEIGHTS_ARCH_A = {
    "F1": 0.15,    # CTR
    "F2": 0.15,    # LP load rate
    "F3": 0.25,    # Scroll / form discovery
    "F4": 0.35,    # Form completion
    "F5": 0.10,    # Post-conversion activation
}

# Arch B: In-platform form flow (Meta Lead Ads, LinkedIn Lead Gen,
# TikTok Instant Forms). No landing page, so F2/F3 don't apply.
# F4 dominates — platform forms live and die on completion rate.
FUNNEL_SIGNAL_WEIGHTS_ARCH_B = {
    "F1": 0.30,    # CTR
    "F4": 0.55,    # Form completion (pre-filled, but still the core)
    "F5": 0.15,    # Post-conversion activation
}


# ── Audience Temperature Adjustments ────────────────────────────────

AUDIENCE_TEMP_MULTIPLIERS = {
    "member_list": 1.0,
    "retargeting": 0.60,
    "prospecting": 0.30,
}


def get_audience_temp_multiplier(audience_type: str | None) -> float:
    """CVR benchmark multiplier based on audience warmth."""
    if not audience_type:
        return 0.50  # Conservative default
    return AUDIENCE_TEMP_MULTIPLIERS.get(audience_type.lower(), 0.50)
