"""Unit tests for the Acquisition pillar (C1-C3) of the conversion diagnostic."""

from __future__ import annotations

from datetime import date, timedelta

import pytest

from backend.services.diagnostics.models import (
    CampaignData,
    CampaignType,
    DailyMetrics,
    FlightContext,
    GA4Metrics,
    MediaPlanLine,
    PlatformMetrics,
    StatusBand,
)
from backend.services.diagnostics.conversion.acquisition import (
    compute_c1_cpa_vs_target,
    compute_c2_volume_trajectory,
    compute_c3_cpa_trend,
    compute_acquisition_pillar,
)


# ── Fixtures ─────────────────────────────────────────────────────────


def _flight(elapsed: int = 14, total: int = 30) -> FlightContext:
    start = date(2026, 4, 1)
    evaluation = start + timedelta(days=elapsed - 1)
    end = start + timedelta(days=total - 1)
    return FlightContext(
        flight_start=start,
        flight_end=end,
        evaluation_date=evaluation,
    )


def _daily_metrics(
    days: int = 7,
    daily_spend: float = 500,
    daily_conversions: float = 10,
    start: date | None = None,
) -> list[DailyMetrics]:
    """Generate N days of daily metrics."""
    s = start or date(2026, 4, 1)
    return [
        DailyMetrics(
            date=s + timedelta(days=i),
            platform_id="facebook",
            spend=daily_spend,
            impressions=50_000,
            clicks=500,
            conversions=daily_conversions,
        )
        for i in range(days)
    ]


def _daily_metrics_declining(
    days: int = 7,
    base_spend: float = 500,
    start_conversions: float = 20,
    decline_rate: float = 0.15,
    start: date | None = None,
) -> list[DailyMetrics]:
    """Generate declining conversion data (for C3 deterioration)."""
    s = start or date(2026, 4, 1)
    result = []
    for i in range(days):
        conv = max(1, start_conversions * (1 - decline_rate * i))
        result.append(DailyMetrics(
            date=s + timedelta(days=i),
            platform_id="facebook",
            spend=base_spend,
            impressions=50_000,
            clicks=500,
            conversions=round(conv, 1),
        ))
    return result


def _media_plan(
    budget: float = 15_000,
    ffs: float | None = 25.0,
    audience: str | None = "member_list",
) -> list[MediaPlanLine]:
    return [
        MediaPlanLine(
            line_id="test-line-1",
            platform_id="facebook",
            planned_budget=budget,
            planned_impressions=1_500_000,
            frequency_cap=5.0,
            flight_start=date(2026, 4, 1),
            flight_end=date(2026, 4, 30),
            ffs_score=ffs,
            audience_type=audience,
            objective="conversions",
        )
    ]


def _campaign(
    platform_metrics: list[PlatformMetrics] | None = None,
    daily_metrics: list[DailyMetrics] | None = None,
    media_plan: list[MediaPlanLine] | None = None,
    elapsed: int = 14,
    total: int = 30,
) -> CampaignData:
    return CampaignData(
        project_code="TEST-CONV",
        campaign_type=CampaignType.CONVERSION,
        flight=_flight(elapsed, total),
        platform_metrics=platform_metrics or [],
        daily_metrics=daily_metrics or [],
        media_plan=media_plan or [],
        ga4=GA4Metrics(),
    )


def _conversion_platform(
    spend: float = 7_000,
    clicks: int = 3_500,
    conversions: float = 70,
) -> PlatformMetrics:
    """A Meta platform with conversion data."""
    return PlatformMetrics(
        platform_id="facebook",
        spend=spend,
        impressions=700_000,
        clicks=clicks,
        conversions=conversions,
    )


# ── C1: CPA vs Target ────────────────────────────────────────────────


class TestC1CpaVsTarget:

    def test_guard_insufficient_conversions(self):
        """C1 should guard-fail with < 5 conversions."""
        result = compute_c1_cpa_vs_target(
            _campaign(
                platform_metrics=[_conversion_platform(conversions=3)],
                media_plan=_media_plan(),
            )
        )
        assert not result.guard_passed
        assert "min_conversions" in (result.guard_reason or "")

    def test_guard_insufficient_spend(self):
        """C1 should guard-fail with < $10 spend."""
        result = compute_c1_cpa_vs_target(
            _campaign(
                platform_metrics=[_conversion_platform(spend=5, conversions=10)],
                media_plan=_media_plan(),
            )
        )
        assert not result.guard_passed
        assert "min_spend" in (result.guard_reason or "")

    def test_guard_no_ffs_no_clicks(self):
        """C1 should guard-fail if no FFS and no clicks to derive target."""
        result = compute_c1_cpa_vs_target(
            _campaign(
                platform_metrics=[_conversion_platform(clicks=0, conversions=10)],
                media_plan=_media_plan(ffs=None),
            )
        )
        assert not result.guard_passed
        assert "no_target_cpa" in (result.guard_reason or "")

    def test_strong_cpa(self):
        """CPA well below target should score STRONG."""
        # FFS=25, member_list → generous target; low actual CPA
        result = compute_c1_cpa_vs_target(
            _campaign(
                platform_metrics=[_conversion_platform(spend=500, clicks=500, conversions=50)],
                media_plan=_media_plan(ffs=25.0, audience="member_list"),
            )
        )
        assert result.guard_passed
        assert result.score is not None
        assert result.score >= 70
        assert result.status == StatusBand.STRONG

    def test_action_cpa(self):
        """Very expensive CPA should score ACTION."""
        # High spend, few conversions → terrible CPA
        result = compute_c1_cpa_vs_target(
            _campaign(
                platform_metrics=[_conversion_platform(spend=10_000, clicks=1_000, conversions=5)],
                media_plan=_media_plan(ffs=15.0, audience="member_list"),
            )
        )
        assert result.guard_passed
        assert result.score is not None
        assert result.score < 40
        assert result.status == StatusBand.ACTION

    def test_inputs_populated(self):
        """C1 should return audit inputs."""
        result = compute_c1_cpa_vs_target(
            _campaign(
                platform_metrics=[_conversion_platform()],
                media_plan=_media_plan(),
            )
        )
        assert result.guard_passed
        assert "actual_cpa" in result.inputs
        assert "target_cpa" in result.inputs
        assert "cpa_efficiency" in result.inputs

    def test_cold_audience_adjusts_target(self):
        """Prospecting audience should produce a higher target CPA (more forgiving)."""
        warm = compute_c1_cpa_vs_target(
            _campaign(
                platform_metrics=[_conversion_platform(spend=7_000, clicks=3_500, conversions=70)],
                media_plan=_media_plan(ffs=25.0, audience="member_list"),
            )
        )
        cold = compute_c1_cpa_vs_target(
            _campaign(
                platform_metrics=[_conversion_platform(spend=7_000, clicks=3_500, conversions=70)],
                media_plan=_media_plan(ffs=25.0, audience="prospecting"),
            )
        )
        # Same actual CPA, but prospecting should have a higher target
        assert cold.inputs["target_cpa"] > warm.inputs["target_cpa"]


# ── C2: Volume Trajectory ─────────────────────────────────────────────


class TestC2VolumeTrajectory:

    def test_guard_insufficient_days(self):
        """C2 should guard-fail with < 3 days of data."""
        result = compute_c2_volume_trajectory(
            _campaign(
                platform_metrics=[_conversion_platform()],
                daily_metrics=_daily_metrics(days=2),
                media_plan=_media_plan(),
                elapsed=2,
            )
        )
        assert not result.guard_passed

    def test_guard_insufficient_conversions(self):
        """C2 should guard-fail with < 5 total conversions."""
        result = compute_c2_volume_trajectory(
            _campaign(
                platform_metrics=[_conversion_platform(conversions=3)],
                daily_metrics=_daily_metrics(days=7, daily_conversions=0.4),
                media_plan=_media_plan(),
            )
        )
        assert not result.guard_passed

    def test_healthy_volume(self):
        """Meeting expected volume should score well."""
        result = compute_c2_volume_trajectory(
            _campaign(
                platform_metrics=[_conversion_platform(spend=7_000, clicks=3_500, conversions=70)],
                daily_metrics=_daily_metrics(days=7, daily_spend=1_000, daily_conversions=10),
                media_plan=_media_plan(budget=30_000, ffs=25.0),
            )
        )
        assert result.guard_passed
        assert result.score is not None
        assert result.score > 0
        assert "rolling_avg_daily" in result.inputs

    def test_low_volume_scores_lower(self):
        """Well below expected volume should score lower."""
        result = compute_c2_volume_trajectory(
            _campaign(
                platform_metrics=[_conversion_platform(spend=7_000, clicks=3_500, conversions=14)],
                daily_metrics=_daily_metrics(days=7, daily_spend=1_000, daily_conversions=2),
                media_plan=_media_plan(budget=30_000, ffs=25.0),
            )
        )
        assert result.guard_passed
        assert result.score is not None
        # Low volume should score lower than healthy volume
        assert result.score < 75


# ── C3: CPA Trend ─────────────────────────────────────────────────────


class TestC3CpaTrend:

    def test_guard_insufficient_days(self):
        """C3 should guard-fail with < 3 days."""
        result = compute_c3_cpa_trend(
            _campaign(
                platform_metrics=[_conversion_platform()],
                daily_metrics=_daily_metrics(days=2),
                elapsed=2,
            )
        )
        assert not result.guard_passed

    def test_stable_trend(self):
        """Stable CPA should score STRONG."""
        result = compute_c3_cpa_trend(
            _campaign(
                platform_metrics=[_conversion_platform(conversions=70)],
                daily_metrics=_daily_metrics(days=7, daily_spend=1_000, daily_conversions=10),
            )
        )
        assert result.guard_passed
        assert result.score is not None
        assert result.inputs["trend"] == "STABLE_OR_IMPROVING"
        assert result.score >= 70

    def test_deteriorating_trend(self):
        """Declining conversions (rising CPA) should score low."""
        result = compute_c3_cpa_trend(
            _campaign(
                platform_metrics=[_conversion_platform(conversions=60)],
                daily_metrics=_daily_metrics_declining(
                    days=7, base_spend=1_000, start_conversions=20, decline_rate=0.12,
                ),
            )
        )
        assert result.guard_passed
        assert result.score is not None
        # CPA is rising as conversions decline
        assert result.inputs["daily_change_pct"] > 0

    def test_learning_phase_floors_score(self):
        """During learning phase, C3 should floor at WATCH level."""
        result = compute_c3_cpa_trend(
            _campaign(
                platform_metrics=[_conversion_platform(conversions=15)],
                daily_metrics=_daily_metrics_declining(
                    days=5, base_spend=500, start_conversions=5, decline_rate=0.2,
                ),
                elapsed=5,
            )
        )
        assert result.guard_passed
        if result.score is not None:
            # Should be floored at 55 during learning phase
            assert result.score >= 55 or result.inputs.get("is_learning_phase") is False


# ── Pillar Assembly ──────────────────────────────────────────────────


class TestAcquisitionPillar:

    def test_pillar_with_all_signals(self):
        """Pillar should have 3 signals and compute a weighted score."""
        pillar = compute_acquisition_pillar(
            _campaign(
                platform_metrics=[_conversion_platform()],
                daily_metrics=_daily_metrics(days=7, daily_spend=1_000, daily_conversions=10),
                media_plan=_media_plan(),
            )
        )
        assert pillar.name == "acquisition"
        assert len(pillar.signals) == 3
        assert all(s.id in ("C1", "C2", "C3") for s in pillar.signals)

        # At least some signals should have scores
        scored = [s for s in pillar.signals if s.guard_passed and s.score is not None]
        assert len(scored) > 0

    def test_pillar_weight(self):
        """Pillar weight should be 0.43 (Quality deferred; proportional share of 0.30/0.70)."""
        pillar = compute_acquisition_pillar(
            _campaign(
                platform_metrics=[_conversion_platform()],
                daily_metrics=_daily_metrics(days=7),
                media_plan=_media_plan(),
            )
        )
        assert pillar.weight == 0.43

    def test_pillar_score_bounded(self):
        """Pillar score should be 0-100 when computed."""
        pillar = compute_acquisition_pillar(
            _campaign(
                platform_metrics=[_conversion_platform()],
                daily_metrics=_daily_metrics(days=7, daily_spend=1_000, daily_conversions=10),
                media_plan=_media_plan(),
            )
        )
        if pillar.score is not None:
            assert 0 <= pillar.score <= 100

    def test_all_guard_fail_gives_none_score(self):
        """If all signals guard-fail, pillar score should be None."""
        pillar = compute_acquisition_pillar(
            _campaign(
                platform_metrics=[_conversion_platform(conversions=0, spend=0)],
                daily_metrics=[],
            )
        )
        assert pillar.score is None
        assert pillar.status is None
