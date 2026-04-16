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


# ── Diagnostic message templates ────────────────────────────────────

R1_MESSAGES = {
    StatusBand.STRONG: (
        "Engagement quality ratio at {ratio} — the majority of interactions "
        "are meaningful actions (reactions, outbound clicks, video views) rather "
        "than passive engagement."
    ),
    StatusBand.WATCH: (
        "Engagement quality ratio at {ratio} — a moderate share of engagement "
        "is high-value. Review creative messaging; the audience is interacting "
        "but not always with intent."
    ),
    StatusBand.ACTION: (
        "Engagement quality ratio at {ratio} — most engagement is low-value "
        "(passive views, auto-expansions). The message may not be compelling "
        "enough to drive deliberate interaction. Assess the creative CTA and "
        "messaging clarity."
    ),
}

R3_MESSAGES = {
    StatusBand.STRONG: (
        "Landing page engagement depth at {combined} (engaged session rate "
        "{engaged_rate}, scroll rate {scroll_rate}). Visitors are absorbing "
        "the page content — the message is carrying through from ad to site."
    ),
    StatusBand.WATCH: (
        "Landing page engagement depth at {combined} (engaged session rate "
        "{engaged_rate}, scroll rate {scroll_rate}). Some visitors are "
        "bouncing before engaging with the content. Review page load speed "
        "and above-the-fold messaging alignment with the ad creative."
    ),
    StatusBand.ACTION: (
        "Landing page engagement depth at {combined} (engaged session rate "
        "{engaged_rate}, scroll rate {scroll_rate}). Most visitors are "
        "leaving without engaging — likely a disconnect between the ad "
        "promise and the landing page experience. Audit message continuity "
        "and page performance."
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

    Decomposes total engagement volume into "quality" actions
    (reactions, outbound clicks, video views) vs total post_engagement.
    A high ratio means the audience is responding with deliberate,
    meaningful actions rather than passive engagement.

    Quality engagements = post_reactions + outbound_clicks + video_views_3s
    Total engagements = post_engagement (Meta's aggregate engagement metric)

    When post_engagement is zero but we have other engagement signals,
    we fall back to computing quality from available metrics directly.
    """
    passed, reason = guard_resonance(data)
    if not passed:
        return _guard_fail("R1", "Engagement Quality Ratio", reason,
                           f"Insufficient data — {reason}.")

    # Aggregate across platforms
    total_engagement = sum(p.post_engagement for p in data.platform_metrics)
    total_reactions = sum(p.post_reactions for p in data.platform_metrics)
    total_outbound = sum(p.outbound_clicks for p in data.platform_metrics)
    total_video_3s = sum(p.video_views_3s for p in data.platform_metrics)

    quality_engagement = total_reactions + total_outbound + total_video_3s

    if total_engagement <= 0:
        return _guard_fail("R1", "Engagement Quality Ratio", "no_engagement_data",
                           "No post engagement data reported by any platform — "
                           "signal will activate once engagement metrics flow.")

    # Quality ratio: what fraction of engagements are high-value?
    # Cap at 1.0 — quality components can exceed post_engagement in some
    # platform reporting edge cases (video_views_3s counted separately
    # from post_engagement on some platforms).
    quality_ratio = min(quality_engagement / total_engagement, 1.0)

    score = normalize_linear(quality_ratio, R1_FLOOR, R1_BENCHMARK)
    status = status_band(score)

    template = R1_MESSAGES.get(status, R1_MESSAGES[StatusBand.WATCH])
    diagnostic = template.format(ratio=format_pct(quality_ratio))

    return SignalResult(
        id="R1",
        name="Engagement Quality Ratio",
        score=round(score, 1),
        status=status,
        raw_value=round(quality_ratio, 3),
        benchmark=R1_BENCHMARK,
        floor=R1_FLOOR,
        diagnostic=diagnostic,
        guard_passed=True,
        inputs={
            "quality_ratio": round(quality_ratio, 3),
            "total_engagement": total_engagement,
            "quality_engagement": quality_engagement,
            "breakdown": {
                "reactions": total_reactions,
                "outbound_clicks": total_outbound,
                "video_views_3s": total_video_3s,
            },
            "platforms": {
                p.platform_id: {
                    "post_engagement": p.post_engagement,
                    "post_reactions": p.post_reactions,
                    "outbound_clicks": p.outbound_clicks,
                    "video_views_3s": p.video_views_3s,
                }
                for p in data.platform_metrics
                if p.post_engagement > 0
            },
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

    Combines two GA4 signals:
        - Engaged session rate: user_engagements / sessions
        - Scroll rate: scroll_events / sessions

    Weighted composite: 65% engaged session rate + 35% scroll rate.
    This measures whether visitors who arrive from paid ads are actually
    absorbing the landing page content.

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

    # Weighted composite — engaged sessions matter more than scroll depth
    combined = engaged_rate * 0.65 + scroll_rate * 0.35
    combined_pct = combined * 100

    score = normalize_linear(combined_pct, R3_FLOOR, R3_BENCHMARK)
    status = status_band(score)

    template = R3_MESSAGES.get(status, R3_MESSAGES[StatusBand.WATCH])
    diagnostic = template.format(
        combined=format_pct(combined),
        engaged_rate=format_pct(engaged_rate),
        scroll_rate=format_pct(scroll_rate),
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
            "weights": {"engaged_rate": 0.65, "scroll_rate": 0.35},
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
