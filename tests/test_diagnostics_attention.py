"""Unit tests for the Attention pillar (A1-A5) of the persuasion diagnostic."""

from __future__ import annotations

from datetime import date, timedelta

import pytest

from backend.services.diagnostics.models import (
    CampaignData,
    CampaignType,
    DailyMetrics,
    FlightContext,
    PlatformMetrics,
    StatusBand,
)
from backend.services.diagnostics.persuasion.attention import (
    _classify_fatigue,
    _linear_slope,
    compute_a1_video_completion,
    compute_a2_audio_completion,
    compute_a3_viewability,
    compute_a4_focused_view,
    compute_a5_creative_fatigue,
    compute_attention_pillar,
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
    platform_metrics: list[PlatformMetrics],
    daily_metrics: list[DailyMetrics] | None = None,
    elapsed: int = 10,
    total: int = 30,
) -> CampaignData:
    return CampaignData(
        project_code="TEST-ATTN",
        campaign_type=CampaignType.PERSUASION,
        flight=_flight(elapsed, total),
        platform_metrics=platform_metrics,
        daily_metrics=daily_metrics or [],
    )


def _healthy_video_platform() -> PlatformMetrics:
    """A Meta-style platform with strong completion on a 30s video."""
    return PlatformMetrics(
        platform_id="facebook",
        spend=5_000,
        impressions=500_000,
        clicks=3_000,
        video_views_3s=150_000,
        thruplay=30_000,
        video_q25=100_000,
        video_q50=70_000,
        video_q75=55_000,
        video_q100=45_000,
    )


# ── Helper utility tests ────────────────────────────────────────────


def test_linear_slope_monotone_decreasing():
    assert _linear_slope([10, 9, 8, 7, 6, 5, 4]) == pytest.approx(-1.0)


def test_linear_slope_flat():
    assert _linear_slope([5, 5, 5, 5, 5, 5, 5]) == 0.0


def test_linear_slope_short_series_returns_zero():
    assert _linear_slope([1]) == 0.0
    assert _linear_slope([]) == 0.0


def test_classify_fatigue_bands():
    assert _classify_fatigue(0.5) == "NONE"
    assert _classify_fatigue(-0.2) == "NONE"
    assert _classify_fatigue(-1.0) == "EARLY"
    assert _classify_fatigue(-2.5) == "MODERATE"
    assert _classify_fatigue(-5.0) == "SEVERE"


# ── A1: Video Completion Quality ────────────────────────────────────


def test_a1_guard_fails_without_any_video_data():
    """Display-only campaign — no quartile data at all."""
    p = PlatformMetrics(
        platform_id="stackadapt",
        spend=1_000,
        impressions=200_000,
        clicks=1_500,
    )
    result = compute_a1_video_completion(_campaign([p]))
    assert result.guard_passed is False
    assert result.score is None
    assert result.id == "A1"


def test_a1_guard_fails_with_insufficient_starts():
    p = PlatformMetrics(
        platform_id="facebook",
        spend=500,
        impressions=200_000,
        video_views_3s=20,
        video_q25=10,
        video_q50=5,
        video_q75=3,
        video_q100=1,
    )
    result = compute_a1_video_completion(_campaign([p]))
    assert result.guard_passed is False


def test_a1_healthy_video_scores_above_floor():
    result = compute_a1_video_completion(_campaign([_healthy_video_platform()]))
    assert result.guard_passed is True
    assert result.score is not None
    assert 0 <= result.score <= 100
    assert result.inputs["diagnosis"] in {
        "HEALTHY", "HOOK_MISS", "HOOK_FAILURE", "MESSAGE_FATIGUE", "CTA_WEAKNESS"
    }


def test_a1_detects_hook_miss_when_q25_low():
    """Very low Q1 retention should flip the diagnosis to HOOK_MISS."""
    p = PlatformMetrics(
        platform_id="facebook",
        spend=1_000,
        impressions=400_000,
        video_views_3s=200_000,
        video_q25=50_000,       # 25% of starts — below 0.50 threshold
        video_q50=30_000,
        video_q75=20_000,
        video_q100=15_000,
    )
    result = compute_a1_video_completion(_campaign([p]))
    assert result.guard_passed is True
    assert result.inputs["diagnosis"] == "HOOK_MISS"


# ── A2: Audio Completion Quality ────────────────────────────────────


def test_a2_always_guard_fails_in_phase_2():
    """A2 is waiting on StackAdapt audio columns in transformation."""
    result = compute_a2_audio_completion(_campaign([_healthy_video_platform()]))
    assert result.guard_passed is False
    assert result.guard_reason == "no_audio_data_in_transformation"
    assert result.id == "A2"


# ── A3: Viewability ─────────────────────────────────────────────────


def test_a3_guard_fails_without_measured_impressions():
    p = _healthy_video_platform()  # no viewability fields
    result = compute_a3_viewability(_campaign([p]))
    assert result.guard_passed is False
    assert result.id == "A3"


def test_a3_strong_viewability_scores_high():
    p = PlatformMetrics(
        platform_id="stackadapt",
        spend=2_000,
        impressions=300_000,
        viewability_measured=200_000,
        viewability_viewed=180_000,  # 90%
    )
    result = compute_a3_viewability(_campaign([p]))
    assert result.guard_passed is True
    assert result.score is not None
    assert result.status == StatusBand.STRONG
    assert result.raw_value == pytest.approx(0.9, abs=1e-3)


def test_a3_poor_viewability_scores_action():
    p = PlatformMetrics(
        platform_id="stackadapt",
        spend=2_000,
        impressions=300_000,
        viewability_measured=200_000,
        viewability_viewed=40_000,  # 20% — below 40% floor
    )
    result = compute_a3_viewability(_campaign([p]))
    assert result.guard_passed is True
    assert result.status == StatusBand.ACTION


# ── A4: Focused View ────────────────────────────────────────────────


def test_a4_meta_uses_thruplay_rate():
    p = PlatformMetrics(
        platform_id="facebook",
        spend=5_000,
        impressions=1_000_000,
        video_views_3s=300_000,
        thruplay=100_000,  # 10% of impressions
        video_q25=250_000,
        video_q100=90_000,
    )
    result = compute_a4_focused_view(_campaign([p]))
    assert result.guard_passed is True
    assert result.score is not None
    assert result.inputs["platforms"]["facebook"]["metric"] == "ThruPlay (15s+)"
    # 10% thruplay rate vs 8% benchmark → should be STRONG
    assert result.status == StatusBand.STRONG


def test_a4_guard_fails_without_video_views():
    p = PlatformMetrics(
        platform_id="stackadapt",
        spend=2_000,
        impressions=300_000,
        clicks=500,
    )
    result = compute_a4_focused_view(_campaign([p]))
    assert result.guard_passed is False


def test_a4_weights_by_impressions_across_platforms():
    meta = PlatformMetrics(
        platform_id="facebook",
        spend=1_000,
        impressions=100_000,
        video_views_3s=30_000,
        thruplay=10_000,  # 10%
    )
    ctv = PlatformMetrics(
        platform_id="stackadapt_ctv",
        spend=1_000,
        impressions=50_000,
        video_views_3s=48_000,  # 96% completion
        video_q100=45_000,
    )
    result = compute_a4_focused_view(_campaign([meta, ctv]))
    assert result.guard_passed is True
    assert "facebook" in result.inputs["platforms"]
    assert "stackadapt_ctv" in result.inputs["platforms"]


# ── A5: Creative Fatigue ────────────────────────────────────────────


def test_a5_guard_fails_before_minimum_days():
    """A5 requires at least 7 days of delivery."""
    p = _healthy_video_platform()
    result = compute_a5_creative_fatigue(_campaign([p], elapsed=3))
    assert result.guard_passed is False
    assert "min_days" in (result.guard_reason or "")


def test_a5_stable_trend_classifies_none():
    p = _healthy_video_platform()
    # 7 days of flat-ish daily thruplay rate
    start = date(2026, 4, 1)
    dailies = [
        DailyMetrics(
            date=start + timedelta(days=i),
            platform_id="facebook",
            impressions=100_000,
            video_views_3s=30_000,
            thruplay=10_000,  # flat 33% thruplay/starts
        )
        for i in range(14)
    ]
    result = compute_a5_creative_fatigue(
        _campaign([p], daily_metrics=dailies, elapsed=14)
    )
    assert result.guard_passed is True
    assert result.inputs["fatigue_band"] == "NONE"
    assert result.score is not None and result.score >= 65


def test_a5_declining_trend_flags_fatigue():
    p = _healthy_video_platform()
    start = date(2026, 4, 1)
    # Thruplay rate drops sharply over the last 7 days
    thruplay_counts = [15_000, 13_000, 11_000, 9_500, 8_000, 6_500, 5_000,
                        4_500, 4_000, 3_500, 3_000, 2_500, 2_000, 1_500]
    dailies = [
        DailyMetrics(
            date=start + timedelta(days=i),
            platform_id="facebook",
            impressions=100_000,
            video_views_3s=30_000,
            thruplay=thruplay_counts[i],
        )
        for i in range(14)
    ]
    result = compute_a5_creative_fatigue(
        _campaign([p], daily_metrics=dailies, elapsed=14, total=30)
    )
    assert result.guard_passed is True
    assert result.inputs["fatigue_band"] in {"EARLY", "MODERATE", "SEVERE"}


# ── Pillar rollup ───────────────────────────────────────────────────


def test_attention_pillar_assembles_all_signals():
    p = _healthy_video_platform()
    p.viewability_measured = 400_000
    p.viewability_viewed = 300_000
    pillar = compute_attention_pillar(_campaign([p]))
    assert pillar.name == "attention"
    assert pillar.weight == 0.40
    assert len(pillar.signals) == 5
    signal_ids = {s.id for s in pillar.signals}
    assert signal_ids == {"A1", "A2", "A3", "A4", "A5"}


def test_attention_pillar_redistributes_weight_to_active_signals():
    """A2 + A5 guard-fail on a short flight — pillar still produces a score."""
    p = _healthy_video_platform()
    pillar = compute_attention_pillar(_campaign([p], elapsed=3))  # <7 days → A5 fails
    active = [s for s in pillar.signals if s.guard_passed]
    # A1 + A4 should be active, A2 always fails, A5 fails (elapsed<7),
    # A3 fails (no viewability data)
    assert {s.id for s in active} == {"A1", "A4"}
    assert pillar.score is not None


def test_attention_pillar_score_none_when_no_signals_active():
    """Display-only short flight — every attention signal guard-fails."""
    p = PlatformMetrics(
        platform_id="stackadapt",
        spend=500,
        impressions=100_000,
        clicks=300,
    )
    pillar = compute_attention_pillar(_campaign([p], elapsed=2))
    assert pillar.score is None
    assert pillar.status is None
