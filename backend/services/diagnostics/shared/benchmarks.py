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

# Effective-frequency floor used to derive planned reach when a media
# plan line has no frequency_cap. From Spenkuch & Toniatti (2018),
# persuasion messaging requires ~3 exposures before the message begins
# to land — so 3 is the minimum "effective" target. Using this as the
# denominator for planned_reach keeps the estimate conservative (larger
# denominator → smaller attainment ratio → harder to score STRONG).
EFFECTIVE_FREQ_FLOOR = 3.0

# D1 cannot score below WATCH (40) during the first N days of a flight.
# Early-flight reach numbers are noisy: platforms are still in learning
# phases, impression delivery is uneven, and reach often catches up in
# days 3-5. Suppressing ACTION states in this window prevents false
# alarms without hiding genuinely bad delivery at flight end.
D1_EARLY_FLIGHT_DAYS = 3

# Plausibility threshold for trusting the MediaPlanLine.planned_reach
# field. If planned_reach > planned_impressions * this ratio, the value
# is likely platform-reported potential reach (addressable audience)
# rather than effective reach. In that case we fall back to derivation.
# 0.5 corresponds to implied average frequency < 2.0 — below the
# effective-reach floor.
D1_PLANNED_REACH_PLAUSIBILITY_RATIO = 0.5

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


# Platform → default creative format mapping. Used as the PRIMARY source
# for format inference, since platform behaviour is more reliable than
# planner-entered channel_category. Best-effort — political PB campaigns
# overwhelmingly use video on social, static on display, and premium
# CTV/DOOH on stackadapt_ctv / perion / hivestack.
PLATFORM_DEFAULT_FORMAT = {
    "stackadapt_ctv":     "video_medium",   # 15-30s CTV bumpers
    "perion":             "dooh",
    "hivestack":          "dooh",
    "tiktok":             "video_short",
    "snapchat":           "video_short",
    "youtube":            "video_medium",   # 15-30s bumpers + pre-roll
    "meta":               "video_short",
    "facebook":           "video_short",
    "instagram":          "video_short",
    "linkedin":           "static",
    "google_ads":         "static",
    "reddit":             "static",
    "pinterest":          "static",
    "stackadapt":         "static",
    "stackadapt_display": "static",
}

# channel_category → format mapping. Used for CONFLICT DETECTION against
# the platform-based inference, so planners who fill this field get
# feedback when their input disagrees with the platform default.
CHANNEL_CATEGORY_TO_FORMAT = {
    "ctv":              "video_medium",
    "ott":              "video_medium",
    "dooh":             "dooh",
    "ooh":              "dooh",
    "audio":            "audio_long",
    "podcast":          "audio_long",
    "streaming_audio":  "audio_long",
    "display":          "static",
    "banner":           "static",
    "native":           "static",
    "video":            "video_short",
    "video_short":      "video_short",
    "video_medium":     "video_medium",
    "video_long":       "video_long",
    "static":           "static",
}


def _normalize_channel_category(cc: str) -> str:
    """Lowercase / strip / normalize channel_category for dict lookup."""
    return cc.lower().strip().replace(" ", "_").replace("-", "_")


def infer_creative_format(
    platform_id: str | None,
    channel_category: str | None = None,
) -> tuple[str, str | None]:
    """Infer creative format with optional conflict detection.

    platform_id is the PRIMARY source (we trust platform behavior more
    than planner-entered metadata). channel_category is consulted only
    to detect conflicts — if it maps to a different format than the
    platform, the caller gets a conflict note for surfacing in the
    diagnostic message.

    Returns:
        (format, conflict_note)
        conflict_note is None when there's no conflict or no
        channel_category was provided; otherwise a human-readable
        description of the mismatch.
    """
    # Primary: platform_id
    pf_format: str | None = None
    if platform_id:
        key = platform_id.lower().replace(" ", "_")
        pf_format = PLATFORM_DEFAULT_FORMAT.get(key)

    # Consult channel_category for conflict detection + secondary fallback
    cc_format: str | None = None
    if channel_category:
        cc_key = _normalize_channel_category(channel_category)
        cc_format = CHANNEL_CATEGORY_TO_FORMAT.get(cc_key)

    chosen = pf_format or cc_format or "video_medium"

    conflict: str | None = None
    if pf_format and cc_format and pf_format != cc_format:
        conflict = (
            f"{platform_id} suggests {pf_format} format but channel_category "
            f"'{channel_category}' suggests {cc_format} — using platform default."
        )

    return chosen, conflict


# ── D3: Frequency Distribution Health ───────────────────────────────
#
# D3 measures inter-platform concentration as a band-normalized CV
# (each platform's frequency divided by its format-optimal, then CV
# across those positions). cv_score = 1 - min(CV, 1.0).
#
# With this formulation:
#   - CV ≤ ~0.30 (cv_score ≥ 0.70): all platforms at similar band
#     positions → STRONG
#   - CV ~0.30–0.75 (cv_score 0.25–0.70): one platform drifting out
#     of alignment → WATCH
#   - CV ≥ 0.75 (cv_score ≤ 0.25): extreme concentration on one
#     platform → ACTION
#
# Thresholds calibrated 2026-04-20 alongside the D3 redesign.

D3_BENCHMARK = 0.70
D3_FLOOR = 0.25


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
#
# Benchmark set to 80% (not the IAB 70% floor). IAB 70% is the industry
# legal-minimum for "viewable"; good PB display campaigns typically land
# at 75-85%. Setting bench=80 means hitting IAB compliance lands in WATCH
# (score ~56), not STRONG — the rating reflects actual performance rather
# than bare compliance. Floor=50 means "more than half unseen" = ACTION.
#
# Coverage note threshold: diagnostic appends a "low coverage" caveat
# when fewer than COVERAGE_NOTE_THRESHOLD of total impressions were
# viewability-measured — it's a measurement-confidence signal, not a
# scoring penalty.

A3_BENCHMARK = 80
A3_FLOOR = 50
A3_COVERAGE_NOTE_THRESHOLD = 0.30


# ── A4: Focused View / ThruPlay Rate ───────────────────────────────
#
# Platform-specific benchmarks. None = "no meaningful focused-view
# metric for this inventory type" — the platform is excluded from A4
# entirely (same pattern as F1's non-clickable inventory). Display
# impressions don't carry a meaningful 3s/15s view metric so
# stackadapt_display is excluded. Platforms not in this dict fall back
# to A4_DEFAULT_BENCHMARK and are flagged in the diagnostic.
#
# Benchmarks in this dict are still Phase 0 estimates, calibrated
# against campaign 25042 only. A dedicated validation pass against
# historical campaigns is backlogged.

A4_BENCHMARKS: dict[str, dict[str, float] | None] = {
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
    "stackadapt_display": None,   # Display has no meaningful focused-view metric
    "stackadapt":         {"benchmark": 0.15, "floor": 0.05},
}

A4_DEFAULT_BENCHMARK = {"benchmark": 0.20, "floor": 0.06}


def get_a4_benchmark(
    platform_id: str,
) -> tuple[dict[str, float] | None, bool]:
    """Look up A4 focused view benchmark for a platform.

    Returns:
        (config, is_default)
        - config is None when the platform has no meaningful focused-view
          metric (e.g. display inventory) — the caller should skip it
          entirely. Otherwise a {"benchmark", "floor"} dict.
        - is_default is True when the platform wasn't in A4_BENCHMARKS
          and the default was substituted; the caller can surface this
          to planners so they know the score isn't platform-calibrated.
    """
    key = platform_id.lower().replace(" ", "_")
    if key in A4_BENCHMARKS:
        return A4_BENCHMARKS[key], False
    return A4_DEFAULT_BENCHMARK, True


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

# Signal weights within Distribution pillar.
#
# Rebalanced 2026-04-20 when D5 Delivery Cadence shipped:
#   D1 stays dominant (reach is the primary question), D2 slightly
#   reduced to make room for D5 as a co-equal first-order check
#   ("did the campaign actually run smoothly?"), D3/D4 down to 0.125
#   each as secondary/diagnostic signals.
DISTRIBUTION_SIGNAL_WEIGHTS = {
    "D1": 0.35,
    "D2": 0.25,
    "D3": 0.125,
    "D4": 0.125,
    "D5": 0.15,
}

# ── D5: Delivery Cadence ────────────────────────────────────────────
#
# Composite: 60% smoothness (CV of daily impressions) + 40% gap-day
# penalty. Per-platform, worst platform drives the score.

# Smoothness: coefficient of variation of a platform's daily impressions.
# CV ≤ 0.30 → STRONG (well-paced with normal daily variance).
# CV ≥ 1.00 → ACTION (stdev matches the mean — heavy bursting).
D5_CV_TARGET = 0.30
D5_CV_CEILING = 1.00

# Gap rate: zero-delivery days within a platform's active window
# (first non-zero day → eval_date) as a fraction of that window.
# 0% gaps → STRONG; 25% ( ≥1 in 4 days dark) → ACTION.
D5_GAP_TARGET = 0.0
D5_GAP_CEILING = 0.25

# Composite weights within D5
D5_SMOOTHNESS_WEIGHT = 0.60
D5_GAP_WEIGHT = 0.40

# Minimum data required before D5 can score (per platform).
# Need a week of daily data before CV is meaningful; flight-level
# guard (20% elapsed) applied separately in the signal function.
D5_MIN_DAILY_ROWS = 7
D5_MIN_FLIGHT_ELAPSED_FRACTION = 0.20

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
# Calibrated 2026-04-20: original bands (-0.5/-1.5/-3.0) flagged normal
# daily-reporting noise as EARLY fatigue and capped SEVERE at only ~21%
# total decline over 7 days. New bands align to practitioner judgment:
#   NONE      >-1.0%/day   (~7% total over 7 days — within noise)
#   EARLY     >-2.5%/day   (~16% total — meaningful drift)
#   MODERATE  >-5.0%/day   (~30% total — refresh needed)
#   SEVERE    ≤-5.0%/day   (catastrophic — immediate action)
A5_FATIGUE_THRESHOLDS = {
    "NONE":     -1.0,
    "EARLY":    -2.5,
    "MODERATE": -5.0,
}

# Minimum daily impressions (as a fraction of the platform window-mean) for
# a day to count in the A5 slope fit. Excludes dark days / reporting gaps
# that would otherwise dominate a low-volume series.
A5_MIN_DAY_IMP_FRACTION = 0.05


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
#
# Quality (Q1-Q3) is deferred indefinitely pending per-client CRM
# integration — without reliable disposition data, any "quality" score
# would be built on proxies (GA4 key_events, etc.) that can't truthfully
# answer "was this lead valuable?". Its original 0.30 weight has been
# redistributed proportionally between Acquisition and Funnel so the
# scored conversion health reflects actual measurement capability. When
# CRM integration eventually ships, revisit these weights.
#
#   Original:  Acq 0.30 | Funnel 0.40 | Quality 0.30
#   Now:       Acq 0.43 | Funnel 0.57   (proportional: 0.30/0.70, 0.40/0.70)

CONVERSION_PILLAR_WEIGHTS = {
    "acquisition": 0.43,
    "funnel": 0.57,
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
# F3 = 0.30 * scroll_rate_score + 0.70 * form_discovery_score
#
# Calibrated 2026-04-20: discovery is the closer conversion proxy — a
# visitor who scrolls but never reaches/starts the form means the page
# didn't work. Scroll stays in the blend as a supplementary page-
# engagement signal. When scroll tracking is absent (scrolls=0 on
# healthy sessions), the scroll component is dropped and the user is
# warned via the diagnostic message.

F3_SCROLL_BENCHMARK = 0.50      # 50% of sessions register a scroll event
F3_SCROLL_FLOOR = 0.15
F3_SCROLL_WEIGHT = 0.30          # Weight of scroll_rate within F3
F3_DISCOVERY_WEIGHT = 0.70       # Weight of form-position discovery within F3

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
# For Arch A (landing-page forms), the numerator is GA4 form_submits and
# the denominator is GA4 form_starts — a true form-completion fraction.
# FFS adjusts the expected completion rate; high-friction forms get a
# lower benchmark.
#
# For Arch B (in-platform forms: Meta Lead, LinkedIn Lead Gen, TikTok
# Instant), the platforms don't expose a form_start event. We proxy
# with on_platform_leads / clicks — a click→lead rate, NOT a true form
# completion rate. It runs much lower than Arch A's form→submit rate
# because clicks include people who tapped the CTA but never opened the
# form. Benchmark and floor are calibrated to click→lead specifically.
#
# Landing-page forms:
#   expected_completion = F4_BASE_COMPLETION_BENCHMARK * exp(-0.012 * ffs)
# In-platform forms, click→lead:
#   benchmark = F4_ARCH_B_CLICK_TO_LEAD_BENCHMARK
#   floor     = F4_ARCH_B_CLICK_TO_LEAD_FLOOR

F4_BASE_COMPLETION_BENCHMARK = 0.65   # 65% baseline for a zero-friction LP form
F4_PLATFORM_FORM_BOOST = 1.4           # (legacy — retained for backward compat)
F4_PLATFORM_FORM_CAP = 0.85            # (legacy — retained for backward compat)
F4_COMPLETION_FLOOR = 0.20             # Below 20% completion = critical (Arch A)

# Arch B click→lead semantics. Calibrated 2026-04-20 against industry
# data for Meta Lead Ads / LinkedIn Lead Gen / TikTok Instant Forms.
F4_ARCH_B_CLICK_TO_LEAD_BENCHMARK = 0.15   # 15% click→lead is a well-run form
F4_ARCH_B_CLICK_TO_LEAD_FLOOR = 0.03       # 3% or below = critical


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
