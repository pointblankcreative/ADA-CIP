"""Unit tests for the Resonance pillar (R1-R3) of the persuasion diagnostic."""

from __future__ import annotations

from datetime import date, timedelta

import pytest

from backend.services.diagnostics.models import (
    CampaignData,
    CampaignType,
    FlightContext,
    GA4Metrics,
    PlatformMetrics,
    StatusBand,
)
from backend.services.diagnostics.persuasion.resonance import (
    compute_r1_engagement_quality,
    compute_r2_earned_amplification,
    compute_r3_landing_page_depth,
    compute_resonance_pillar,
)


# ── Fixtures ─────────────────────────────────────────────────────────


def _flight(elapsed: int = 10, total: int = 30) -> FlightContext:
    start = date(2026, 4, 1)
    evaluation = start + timedelta(days=elapsed - 1)
    end = start + timedelta(days=total - 1)
    return FlightContext(
        flight_start=start,
        flight_end=end,
        evaluation_date=evaluation,
    )


def _campaign(
    platform_metrics: list[PlatformMetrics] | None = None,
    ga4: GA4Metrics | None = None,
    elapsed: int = 10,
    total: int = 30,
) -> CampaignData:
    return CampaignData(
        project_code="TEST-RES",
        campaign_type=CampaignType.PERSUASION,
        flight=_flight(elapsed, total),
        platform_metrics=platform_metrics or [],
        ga4=ga4 or GA4Metrics(),
    )


def _engaged_platform() -> PlatformMetrics:
    """A Meta platform with healthy engagement profile."""
    return PlatformMetrics(
        platform_id="facebook",
        spend=5_000,
        impressions=500_000,
        clicks=3_000,
        post_engagement=10_000,
        post_reactions=3_000,
        post_comments=500,
        outbound_clicks=1_500,
        video_views_3s=4_000,
    )


def _low_quality_platform() -> PlatformMetrics:
    """Platform with lots of engagement but mostly low-value."""
    return PlatformMetrics(
        platform_id="facebook",
        spend=3_000,
        impressions=400_000,
        clicks=2_000,
        post_engagement=8_000,
        post_reactions=200,
        post_comments=100,
        outbound_clicks=100,
        video_views_3s=100,
    )


def _healthy_ga4() -> GA4Metrics:
    """GA4 data with strong engagement."""
    return GA4Metrics(
        sessions=1_000,
        scrolls=650,
        engaged_sessions=700,
        form_starts=50,
        form_submits=20,
        key_events=15,
    )


def _weak_ga4() -> GA4Metrics:
    """GA4 data with poor engagement — high bounce, low scroll."""
    return GA4Metrics(
        sessions=500,
        scrolls=50,
        engaged_sessions=80,
        form_starts=5,
        form_submits=1,
        key_events=1,
    )


# ── R1: Engagement Quality Ratio ──────────────────────────────────────


def test_r1_guard_fails_without_engagement_data():
    """No engagement from any platform → guard-fail."""
    p = PlatformMetrics(
        platform_id="stackadapt",
        spend=1_000,
        impressions=200_000,
        clicks=500,
    )
    result = compute_r1_engagement_quality(_campaign([p]))
    assert result.guard_passed is False
    assert result.id == "R1"


def test_r1_guard_fails_with_insufficient_volume():
    """Under MIN_ENGAGEMENTS threshold → guard-fail."""
    p = PlatformMetrics(
        platform_id="facebook",
        spend=500,
        impressions=50_000,
        post_engagement=30,  # below 50 threshold
        post_reactions=10,
        outbound_clicks=5,
    )
    result = compute_r1_engagement_quality(_campaign([p]))
    assert result.guard_passed is False
    assert result.score is None


def test_r1_healthy_engagement_scores_strong():
    """High-quality engagement → STRONG."""
    result = compute_r1_engagement_quality(_campaign([_engaged_platform()]))
    assert result.guard_passed is True
    assert result.score is not None
    assert 0 <= result.score <= 100
    # reactions(3000) + outbound(1500) + video_3s(4000) = 8500 / 10000 = 0.85
    assert result.raw_value == pytest.approx(0.85, abs=0.01)
    assert result.status == StatusBand.STRONG


def test_r1_low_quality_engagement_scores_action():
    """Mostly passive engagement → ACTION."""
    result = compute_r1_engagement_quality(_campaign([_low_quality_platform()]))
    assert result.guard_passed is True
    # reactions(200) + outbound(100) + video_3s(100) = 400 / 8000 = 0.05
    assert result.raw_value == pytest.approx(0.05, abs=0.01)
    assert result.status == StatusBand.ACTION


def test_r1_caps_ratio_at_one():
    """Quality components can exceed post_engagement — ratio caps at 1.0."""
    p = PlatformMetrics(
        platform_id="facebook",
        spend=1_000,
        impressions=100_000,
        post_engagement=100,
        post_reactions=50,
        outbound_clicks=30,
        video_views_3s=80,  # 50+30+80 = 160 > 100
    )
    result = compute_r1_engagement_quality(_campaign([p]))
    assert result.guard_passed is True
    assert result.raw_value == 1.0


def test_r1_aggregates_across_platforms():
    """Multiple platforms contribute to a single R1 score."""
    p1 = PlatformMetrics(
        platform_id="facebook",
        spend=3_000,
        impressions=300_000,
        post_engagement=5_000,
        post_reactions=2_000,
        outbound_clicks=1_000,
        video_views_3s=500,
    )
    p2 = PlatformMetrics(
        platform_id="stackadapt",
        spend=2_000,
        impressions=200_000,
        post_engagement=3_000,
        post_reactions=200,
        outbound_clicks=50,
        video_views_3s=0,
    )
    result = compute_r1_engagement_quality(_campaign([p1, p2]))
    assert result.guard_passed is True
    # Total: (2000+1000+500+200+50+0) / (5000+3000) = 3750/8000 = 0.469
    assert result.raw_value == pytest.approx(0.469, abs=0.01)


def test_r1_guard_fails_on_day_zero():
    """Flight hasn't started yet → guard-fail."""
    result = compute_r1_engagement_quality(
        _campaign([_engaged_platform()], elapsed=0)
    )
    assert result.guard_passed is False


# ── R2: Earned Amplification ──────────────────────────────────────────


def test_r2_always_guard_fails_in_phase_2():
    """R2 is waiting on platform API connectors — always guard-fails."""
    result = compute_r2_earned_amplification(_campaign([_engaged_platform()]))
    assert result.guard_passed is False
    assert result.guard_reason == "no_earned_data_in_transformation"
    assert result.id == "R2"


# ── R3: Landing Page Engagement Depth ────────────────────────────────


def test_r3_guard_fails_without_ga4_sessions():
    """No GA4 data → guard-fail."""
    result = compute_r3_landing_page_depth(_campaign([_engaged_platform()]))
    assert result.guard_passed is False
    assert result.id == "R3"


def test_r3_guard_fails_with_insufficient_sessions():
    """Under MIN_GA4_SESSIONS → guard-fail."""
    ga4 = GA4Metrics(sessions=10, scrolls=5, engaged_sessions=7)
    result = compute_r3_landing_page_depth(
        _campaign([_engaged_platform()], ga4=ga4)
    )
    assert result.guard_passed is False


def test_r3_strong_engagement_scores_high():
    """Healthy GA4 engagement → STRONG."""
    result = compute_r3_landing_page_depth(
        _campaign([_engaged_platform()], ga4=_healthy_ga4())
    )
    assert result.guard_passed is True
    assert result.score is not None
    # engaged_rate = 700/1000 = 0.70, scroll_rate = 650/1000 = 0.65
    # combined = 0.70*0.65 + 0.65*0.35 = 0.455 + 0.2275 = 0.6825
    # combined_pct = 68.25
    assert result.raw_value == pytest.approx(0.683, abs=0.01)
    assert result.status == StatusBand.STRONG


def test_r3_weak_engagement_scores_action():
    """Poor GA4 engagement → ACTION."""
    result = compute_r3_landing_page_depth(
        _campaign([_engaged_platform()], ga4=_weak_ga4())
    )
    assert result.guard_passed is True
    # engaged_rate = 80/500 = 0.16, scroll_rate = 50/500 = 0.10
    # combined = 0.16*0.65 + 0.10*0.35 = 0.104 + 0.035 = 0.139
    # combined_pct = 13.9 — below 20% floor
    assert result.status == StatusBand.ACTION
    assert result.score == 0.0


def test_r3_guard_fails_on_day_zero():
    """Flight not started → guard-fail."""
    result = compute_r3_landing_page_depth(
        _campaign([_engaged_platform()], ga4=_healthy_ga4(), elapsed=0)
    )
    assert result.guard_passed is False


# ── Pillar rollup ───────────────────────────────────────────────────


def test_resonance_pillar_assembles_all_signals():
    """Pillar contains R1, R2, R3 signals."""
    pillar = compute_resonance_pillar(
        _campaign([_engaged_platform()], ga4=_healthy_ga4())
    )
    assert pillar.name == "resonance"
    assert pillar.weight == 0.25
    assert len(pillar.signals) == 3
    signal_ids = {s.id for s in pillar.signals}
    assert signal_ids == {"R1", "R2", "R3"}


def test_resonance_pillar_scores_with_r1_and_r3_active():
    """R2 guard-fails, but R1 + R3 produce a pillar score."""
    pillar = compute_resonance_pillar(
        _campaign([_engaged_platform()], ga4=_healthy_ga4())
    )
    active = [s for s in pillar.signals if s.guard_passed]
    assert {s.id for s in active} == {"R1", "R3"}
    assert pillar.score is not None
    assert pillar.status is not None


def test_resonance_pillar_scores_r1_only_without_ga4():
    """No GA4 → R3 guard-fails, only R1 active."""
    pillar = compute_resonance_pillar(
        _campaign([_engaged_platform()])
    )
    active = [s for s in pillar.signals if s.guard_passed]
    assert {s.id for s in active} == {"R1"}
    assert pillar.score is not None


def test_resonance_pillar_none_when_no_signals_active():
    """No engagement + no GA4 → every signal guard-fails."""
    p = PlatformMetrics(
        platform_id="stackadapt",
        spend=500,
        impressions=100_000,
        clicks=300,
    )
    pillar = compute_resonance_pillar(_campaign([p], elapsed=2))
    assert pillar.score is None
    assert pillar.status is None
