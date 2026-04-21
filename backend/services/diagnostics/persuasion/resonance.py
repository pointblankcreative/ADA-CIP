"""Resonance pillar signals (R1–R3) for persuasion campaigns.

"Is the message landing?"

R1: Engagement Quality Ratio — meaningful actions vs total engagement volume
R2: Viral/Earned Amplification — earned impressions vs paid impressions
R3: Landing Page Engagement Depth — GA4 engaged session rate + scroll depth

Phase 2 build. R1 and R3 have data coverage from fact_digital_daily and
fact_ga4_daily respectively. R2 guard-fails until viral/earned impression
columns are added to the transformation layer (requires direct platform
API connectors — Phase 3).
"""

from __future__ import annotations

from typing import Any

from backend.services.diagnostics.models import (
    CampaignData,
    PillarScore,
    SignalResult,
    StatusBand,
    status_band,
)
from backend.services.diagnostics.shared.benchmarks import (
    R1_BENCHMARK,
    R1_FLOOR,
    R2_BENCHMARK,
    R2_FLOOR,
    R3_BENCHMARK,
    R3_FLOOR,
    RESONANCE_SIGNAL_WEIGHTS,
)
from backend.services.diagnostics.shared.guards import (
    check_min_days,
    check_min_engagements,
    check_min_ga4_sessions,
    guard_resonance,
)
from backend.services.diagnostics.shared.normalization import (
    format_pct,
    normalize_linear,
    safe_div,
)


# ── Calibration constants (2026-04-20) ──────────────────────────────

# R1 engagement-volume floor: total_engagement / impressions must be at
# least this fraction for R1 to reach STRONG. Prevents the false-STRONG
# where a perfect quality ratio is computed over a trivial number of
# engagements on a massive-impression campaign.
R1_ENGAGEMENT_RATE_FLOOR_FOR_STRONG = 0.005  # 0.5%

# R1 worst-platform anchor cap — min score gap between overall and worst
# before the worst-platform anchor fires in the diagnostic.
R1_WORST_PLATFORM_MIN_GAP = 10.0

# R3 weights when scroll tracking is present (scrolls > 0 and sessions > 0)
R3_WEIGHTS_WITH_SCROLL = {"engaged_rate": 0.85, "scroll_rate": 0.15}
# When scroll tracking is absent (scrolls == 0), score on engaged rate
# only and flag the tracking gap in the diagnostic.
R3_WEIGHTS_WITHOUT_SCROLL = {"engaged_rate": 1.00, "scroll_rate": 0.00}


# ── Diagnostic message templates ────────────────────────────────────

R1_MESSAGES = {
    StatusBand.STRONG: (
        "Engagement quality ratio at {ratio} — the majority of interactions "
        "are deliberate actions (reactions, outbound clicks) rather than "
        "passive engagement.{worst_suffix}"
    ),
    StatusBand.WATCH: (
        "Engagement quality ratio at {ratio} — a moderate share of engagement "
        "is high-value. Review creative messaging; the audience is interacting "
        "but not always with intent.{worst_suffix}"
    ),
    StatusBand.ACTION: (
        "Engagement quality ratio at {ratio} — most engagement is low-value "
        "(passive views, auto-expansions). The message may not be compelling "
        "enough to drive deliberate interaction.{worst_suffix} Assess the "
        "creative CTA and messaging clarity."
    ),
}

R3_MESSAGES = {
    StatusBand.STRONG: (
        "Landing page engagement depth at {combined} (engaged session rate "
        "{engaged_rate}{scroll_suffix}). Visitors are absorbing the page "
        "content — the message is carrying through from ad to site."
    ),
    StatusBand.WATCH: (
        "Landing page engagement depth at {combined} (engaged session rate "
        "{engaged_rate}{scroll_suffix}). Some visitors are bouncing before "
        "engaging with the content. Review page load speed and above-the-fold "
        "messaging alignment with the ad creative."
    ),
    StatusBand.ACTION: (
        "Landing page engagement depth at {combined} (engaged session rate "
        "{engaged_rate}{scroll_suffix}). Most visitors are leaving without "
        "engaging — likely a disconnect between the ad promise and the "
        "landing page experience. Audit message continuity and page "
        "performance."
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


# ── Signal R1: Engagement Quality Ratio ──────────────────────────────


def compute_r1_engagement_quality(data: CampaignData) -> SignalResult:
    """R1: Engagement Quality Ratio.

    Decomposes total engagement into "quality" actions (deliberate
    interactions) vs total post_engagement, computed per-platform and
    impression-weighted.

    Calibration notes (2026-04-20):
        - Quality numerator = post_reactions + outbound_clicks only.
          video_views_3s was removed — a 3-second autoplay isn't a
          deliberate interaction, and counting it inflated video-heavy
          campaigns.
        - Per-platform computation + impression-weighted roll-up fixes
          the denominator mismatch where the old aggregate summed
          quality signals across all platforms but used Meta-only
          post_engagement as the denominator.
        - Engagement-volume floor: overall score is capped at WATCH
          when total_engagement / impressions < 0.5%. Prevents the
          "nobody engaged but the handful who did were deliberate"
          false-STRONG on low-engagement campaigns.
    """
    passed, reason = guard_resonance(data)
    if not passed:
        return _guard_fail("R1", "Engagement Quality Ratio", reason,
                           f"Insufficient data — {reason}.")

    # Per-platform computation
    platform_results: dict[str, dict[str, Any]] = {}
    total_engagement = 0
    total_quality = 0
    total_impressions = 0

    for p in data.platform_metrics:
        if p.post_engagement <= 0:
            continue
        quality = p.post_reactions + p.outbound_clicks
        # Cap per-platform quality at total engagement — some platforms
        # report components that overlap with the aggregate (e.g.
        # reactions counted inside post_engagement separately).
        quality = min(quality, p.post_engagement)
        p_ratio = quality / p.post_engagement
        p_score = normalize_linear(p_ratio, R1_FLOOR, R1_BENCHMARK)
        platform_results[p.platform_id] = {
            "post_engagement": p.post_engagement,
            "post_reactions": p.post_reactions,
            "outbound_clicks": p.outbound_clicks,
            "quality_engagement": quality,
            "quality_ratio": round(p_ratio, 3),
            "score": round(p_score, 1),
            "impressions": p.impressions,
        }
        total_engagement += p.post_engagement
        total_quality += quality
        total_impressions += p.impressions

    if total_engagement <= 0 or not platform_results:
        return _guard_fail("R1", "Engagement Quality Ratio", "no_engagement_data",
                           "No post engagement data reported by any platform — "
                           "signal will activate once engagement metrics flow.")

    # Impression-weighted roll-up of per-platform scores
    imp_weighted_total = sum(
        r["score"] * r["impressions"] for r in platform_results.values()
    )
    weighted_impressions = sum(r["impressions"] for r in platform_results.values())
    if weighted_impressions > 0:
        score = imp_weighted_total / weighted_impressions
    else:
        # Fall back to engagement-weighted if no impressions reported
        score = sum(
            r["score"] * r["post_engagement"] for r in platform_results.values()
        ) / total_engagement

    # Overall quality ratio for reporting (engagement-weighted, the
    # natural aggregation for a ratio)
    overall_ratio = total_quality / total_engagement

    # Engagement-volume floor — cap at WATCH when the campaign barely
    # engaged anyone, regardless of how pure the quality ratio looks.
    engagement_rate = (
        total_engagement / total_impressions if total_impressions > 0 else 0.0
    )
    volume_floor_triggered = False
    if score >= 70 and engagement_rate < R1_ENGAGEMENT_RATE_FLOOR_FOR_STRONG:
        score = 69.9  # Just below STRONG threshold
        volume_floor_triggered = True

    status = status_band(score)

    # Worst-platform anchor when there's meaningful divergence
    worst_suffix = ""
    if platform_results and status != StatusBand.STRONG:
        worst_pid, worst = min(
            platform_results.items(), key=lambda kv: kv[1]["score"]
        )
        if score - worst["score"] >= R1_WORST_PLATFORM_MIN_GAP:
            worst_suffix = (
                f" {worst_pid} lowest at {format_pct(worst['quality_ratio'])}"
            )

    template = R1_MESSAGES.get(status, R1_MESSAGES[StatusBand.WATCH])
    diagnostic = template.format(
        ratio=format_pct(overall_ratio),
        worst_suffix=worst_suffix,
    )

    if volume_floor_triggered:
        # Show engagement rate with 2 decimals — format_pct rounds to
        # whole percentages which displays 0.02% as "0%" and loses the
        # meaning of the floor.
        diagnostic = diagnostic + (
            f" Note: engagement volume is thin "
            f"({engagement_rate * 100:.2f}% of impressions) — "
            f"high quality ratio reflects a small base of engagers."
        )

    return SignalResult(
        id="R1",
        name="Engagement Quality Ratio",
        score=round(score, 1),
        status=status,
        raw_value=round(overall_ratio, 3),
        benchmark=R1_BENCHMARK,
        floor=R1_FLOOR,
        diagnostic=diagnostic,
        guard_passed=True,
        inputs={
            "quality_ratio": round(overall_ratio, 3),
            "total_engagement": total_engagement,
            "quality_engagement": total_quality,
            "engagement_rate": round(engagement_rate, 4),
            "volume_floor_triggered": volume_floor_triggered,
            "platforms": platform_results,
        },
    )


# ── Signal R2: Viral/Earned Amplification ────────────────────────────


def compute_r2_earned_amplification(data: CampaignData) -> SignalResult:
    """R2: Viral/Earned Amplification — earned impressions vs paid.

    GUARD-FAILED in Phase 2 — viral/earned impression columns
    (LinkedIn Viral_Impressions, Snapchat Earned_impressions, etc.)
    are not in the transformation layer. Funnel.io doesn't export them;
    would need direct platform API connectors (Phase 3).

    The signal shell is here so the engine and pillar builder can
    incorporate it once data is wired in.
    """
    return _guard_fail(
        "R2", "Earned Amplification", "no_earned_data_in_transformation",
        "Viral/earned impression metrics are not yet in the transformation "
        "layer — Funnel.io doesn't export them. Signal will activate once "
        "direct platform API connectors provide earned impression data "
        "(Phase 3).",
    )


# ── Signal R3: Landing Page Engagement Depth ──────────────────────────


def compute_r3_landing_page_depth(data: CampaignData) -> SignalResult:
    """R3: Landing Page Engagement Depth.

    Combines GA4 signals:
        - Engaged session rate: engaged_sessions / sessions
        - Scroll rate: scroll_events / sessions (when configured)

    Calibration notes (2026-04-20):
        - When scroll tracking is configured (scrolls > 0), weighted
          composite = 0.85 * engaged_rate + 0.15 * scroll_rate.
          The old 0.65/0.35 weighting double-counted engagement signal
          (engaged_session_rate already folds in bounce-inversion,
          session duration, and event engagement).
        - When scroll tracking is NOT configured (scrolls == 0 but
          sessions > 0), score on engaged_rate alone and flag the
          tracking gap in the diagnostic. Previously these clients
          were silently penalised.

    Guard: requires MIN_GA4_SESSIONS (20) to avoid noisy rates.
    """
    passed, reason = check_min_days(data.flight)
    if not passed:
        return _guard_fail("R3", "Landing Page Depth", reason,
                           f"Insufficient data — {reason}.")

    passed, reason = check_min_ga4_sessions(data)
    if not passed:
        return _guard_fail("R3", "Landing Page Depth", reason,
                           "Not enough GA4 sessions to produce stable rates — "
                           "signal will activate once session volume builds. "
                           "If GA4 URLs aren't configured for this project, "
                           "check the project_ga4_urls mapping table.")

    sessions = data.ga4.sessions
    engaged = data.ga4.engaged_sessions
    scrolls = data.ga4.scrolls

    # Rates (capped at 1.0 for edge cases)
    engaged_rate = min(safe_div(engaged, sessions, 0), 1.0)
    scroll_rate = min(safe_div(scrolls, sessions, 0), 1.0)

    # Scroll tracking presence: if zero scrolls across meaningful session
    # volume, assume tracking isn't configured rather than assume zero
    # real scrolling happened.
    scroll_tracking_present = scrolls > 0
    if scroll_tracking_present:
        weights = R3_WEIGHTS_WITH_SCROLL
        combined = (
            engaged_rate * weights["engaged_rate"]
            + scroll_rate * weights["scroll_rate"]
        )
    else:
        weights = R3_WEIGHTS_WITHOUT_SCROLL
        combined = engaged_rate  # score on engaged_rate alone

    combined_pct = combined * 100
    score = normalize_linear(combined_pct, R3_FLOOR, R3_BENCHMARK)
    status = status_band(score)

    # Scroll suffix for diagnostic — only include when tracking present
    if scroll_tracking_present:
        scroll_suffix = f", scroll rate {format_pct(scroll_rate)}"
    else:
        scroll_suffix = ""

    template = R3_MESSAGES.get(status, R3_MESSAGES[StatusBand.WATCH])
    diagnostic = template.format(
        combined=format_pct(combined),
        engaged_rate=format_pct(engaged_rate),
        scroll_suffix=scroll_suffix,
    )

    if not scroll_tracking_present:
        diagnostic = diagnostic + (
            " Note: no scroll events detected in GA4 — scoring on "
            "engaged session rate alone. Configure scroll tracking in "
            "GA4 for a fuller depth signal."
        )

    return SignalResult(
        id="R3",
        name="Landing Page Depth",
        score=round(score, 1),
        status=status,
        raw_value=round(combined, 3),
        benchmark=float(R3_BENCHMARK),
        floor=float(R3_FLOOR),
        diagnostic=diagnostic,
        guard_passed=True,
        inputs={
            "combined_pct": round(combined_pct, 1),
            "engaged_session_rate": round(engaged_rate, 3),
            "scroll_rate": round(scroll_rate, 3),
            "sessions": sessions,
            "engaged_sessions": engaged,
            "scrolls": scrolls,
            "scroll_tracking_present": scroll_tracking_present,
            "weights": weights,
        },
    )


# ── Pillar assembly ─────────────────────────────────────────────────


def compute_resonance_pillar(data: CampaignData) -> PillarScore:
    """Compute all Resonance signals and assemble the pillar score.

    Weights: R1=0.45, R2=0.25, R3=0.30.
    Guard-failed signals are excluded and their weight redistributes
    pro rata across active signals (same pattern as Distribution/Attention).
    """
    r1 = compute_r1_engagement_quality(data)
    r2 = compute_r2_earned_amplification(data)
    r3 = compute_r3_landing_page_depth(data)

    pillar = PillarScore(
        name="resonance",
        signals=[r1, r2, r3],
        weight=0.25,  # Persuasion pillar weight
    )

    active = [s for s in pillar.signals if s.guard_passed and s.score is not None]
    if active:
        weights = RESONANCE_SIGNAL_WEIGHTS
        # Fail loudly if a signal ID isn't in the weights table — adding a new
        # signal to compute_resonance_pillar without also updating
        # RESONANCE_SIGNAL_WEIGHTS would otherwise silently default to an
        # arbitrary weight and distort the pillar score.
        missing = [s.id for s in active if s.id not in weights]
        if missing:
            raise KeyError(
                f"RESONANCE_SIGNAL_WEIGHTS is missing entries for: {missing}. "
                f"Add them to shared.benchmarks before scoring."
            )
        weighted_sum = sum(s.score * weights[s.id] for s in active)
        total_weight = sum(weights[s.id] for s in active)
        pillar.score = round(weighted_sum / total_weight, 1) if total_weight > 0 else None
        pillar.status = status_band(pillar.score) if pillar.score is not None else None
    else:
        pillar.score = None
        pillar.status = None

    return pillar
