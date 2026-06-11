"""Unit tests for the Distribution pillar (D1–D5) of the persuasion diagnostic.

Currently focused on D5 Delivery Cadence calibration (shipped 2026-04-20).
D1–D4 are exercised via the engine-mixed integration tests; dedicated
unit coverage for them is backlogged.
"""

from __future__ import annotations

from datetime import date, timedelta

import pytest

from backend.services.diagnostics.models import (
    CampaignData,
    CampaignType,
    DailyMetrics,
    FlightContext,
    MediaPlanLine,
    PlatformMetrics,
    StatusBand,
)
from backend.services.diagnostics.persuasion.distribution import (
    compute_d5_delivery_cadence,
)


# ── Fixtures ─────────────────────────────────────────────────────────


FLIGHT_START = date(2026, 4, 1)


def _flight(elapsed: int = 14, total: int = 30) -> FlightContext:
    end = FLIGHT_START + timedelta(days=total - 1)
    evaluation = FLIGHT_START + timedelta(days=elapsed - 1)
    return FlightContext(
        flight_start=FLIGHT_START,
        flight_end=end,
        evaluation_date=evaluation,
    )


def _campaign(
    daily_metrics: list[DailyMetrics] | None = None,
    platform_metrics: list[PlatformMetrics] | None = None,
    media_plan: list[MediaPlanLine] | None = None,
    elapsed: int = 14,
    total: int = 30,
) -> CampaignData:
    return CampaignData(
        project_code="TEST-DIST",
        campaign_type=CampaignType.PERSUASION,
        flight=_flight(elapsed, total),
        platform_metrics=platform_metrics or [],
        daily_metrics=daily_metrics or [],
        media_plan=media_plan or [],
    )


def _smooth_series(
    platform_id: str,
    n_days: int,
    daily_imp: int = 50_000,
    start_offset: int = 0,
) -> list[DailyMetrics]:
    """Perfectly-smooth daily delivery — baseline for a STRONG platform."""
    return [
        DailyMetrics(
            date=FLIGHT_START + timedelta(days=start_offset + i),
            platform_id=platform_id,
            impressions=daily_imp,
        )
        for i in range(n_days)
    ]


def _bursty_series(
    platform_id: str,
    n_days: int,
    start_offset: int = 0,
) -> list[DailyMetrics]:
    """Highly variable daily delivery — alternating 10k / 100k impressions.
    CV on alternating [10000, 100000] = ~0.82 (stdev/mean)."""
    rows = []
    for i in range(n_days):
        imp = 100_000 if i % 2 == 0 else 10_000
        rows.append(DailyMetrics(
            date=FLIGHT_START + timedelta(days=start_offset + i),
            platform_id=platform_id,
            impressions=imp,
        ))
    return rows


def _series_with_gaps(
    platform_id: str,
    n_days: int,
    gap_days: list[int],
    daily_imp: int = 50_000,
) -> list[DailyMetrics]:
    """Smooth delivery except specific days are zero (dark days within window).
    `gap_days` is a list of 0-indexed day offsets (from flight_start) that
    should be zero-delivery; the first row is always non-zero so the active
    window starts on day 0."""
    rows = []
    for i in range(n_days):
        imp = 0 if i in gap_days else daily_imp
        rows.append(DailyMetrics(
            date=FLIGHT_START + timedelta(days=i),
            platform_id=platform_id,
            impressions=imp,
        ))
    return rows


# ── D5: Delivery Cadence ────────────────────────────────────────────


class TestD5DeliveryCadence:

    def test_guard_early_flight(self):
        """Flight <20% elapsed → guard-fail (too early to score)."""
        # 5 days elapsed of 30 = 16.7% — under the 20% threshold
        data = _campaign(
            daily_metrics=_smooth_series("facebook", 5),
            elapsed=5,
            total=30,
        )
        result = compute_d5_delivery_cadence(data)
        assert not result.guard_passed
        assert result.guard_reason == "early_flight"

    def test_guard_no_daily_data(self):
        """No DailyMetrics rows → guard-fail."""
        data = _campaign(daily_metrics=[], elapsed=14, total=30)
        result = compute_d5_delivery_cadence(data)
        assert not result.guard_passed
        assert result.guard_reason == "no_daily_data"

    def test_guard_insufficient_per_platform_data(self):
        """Every platform has <7 daily rows → guard-fail with
        insufficient_data."""
        # 5 days, past the 20% flight threshold, but not enough to
        # compute a stable CV on any single platform
        data = _campaign(
            daily_metrics=_smooth_series("facebook", 5),
            elapsed=14,
            total=30,
        )
        result = compute_d5_delivery_cadence(data)
        assert not result.guard_passed
        assert result.guard_reason == "insufficient_data"

    def test_smooth_single_platform_scores_strong(self):
        """Perfectly smooth delivery → STRONG.

        normalize_inverse maps 'at target' → 75 and boosts below-target
        toward 100. With CV=0 (below target 0.30) + 0 gaps, the composite
        lands in the low 80s — solidly in the STRONG band (>=70) which is
        the right semantic outcome.
        """
        data = _campaign(
            daily_metrics=_smooth_series("facebook", 14),
            elapsed=14,
            total=30,
        )
        result = compute_d5_delivery_cadence(data)
        assert result.guard_passed
        assert result.status == StatusBand.STRONG
        assert result.score >= 80
        # Smooth series has CV=0
        assert result.raw_value == pytest.approx(0.0, abs=0.001)

    def test_bursty_platform_scores_lower(self):
        """Alternating 10k/100k impressions — CV well above target."""
        data = _campaign(
            daily_metrics=_bursty_series("facebook", 14),
            elapsed=14,
            total=30,
        )
        result = compute_d5_delivery_cadence(data)
        assert result.guard_passed
        # CV on [10000, 100000] alternating ≈ 0.79-0.82 → WATCH band
        assert result.raw_value > 0.70
        assert result.status in {StatusBand.WATCH, StatusBand.ACTION}

    def test_dark_days_penalize_score(self):
        """Zero-delivery days within the active window drag score down."""
        # 14 days with 4 dark days (28.5% gap rate, above 25% ceiling
        # → gap_score = 0). CV is also elevated by the zeros.
        data = _campaign(
            daily_metrics=_series_with_gaps(
                "facebook", 14, gap_days=[3, 5, 7, 10]
            ),
            elapsed=14,
            total=30,
        )
        result = compute_d5_delivery_cadence(data)
        assert result.guard_passed
        platform_info = result.inputs["platforms"][0]
        assert platform_info["gap_days"] == 4
        assert platform_info["gap_rate"] == pytest.approx(4 / 14, abs=0.01)
        assert platform_info["gap_score"] == pytest.approx(0.0, abs=1.0)
        assert result.status in {StatusBand.WATCH, StatusBand.ACTION}

    def test_staggered_launch_not_penalized_for_preflight_zeros(self):
        """Platform that starts on day 5 should not be dinged for days 0-4.
        Active window starts on first non-zero day."""
        # Platform has zeros for first 5 days, then smooth for 10 days
        rows = []
        for i in range(5):
            rows.append(DailyMetrics(
                date=FLIGHT_START + timedelta(days=i),
                platform_id="linkedin",
                impressions=0,
            ))
        rows.extend(_smooth_series("linkedin", 10, start_offset=5))
        data = _campaign(
            daily_metrics=rows,
            elapsed=15,
            total=30,
        )
        result = compute_d5_delivery_cadence(data)
        assert result.guard_passed
        # Window starts on day 5 (first non-zero) → 10 days, 0 gaps
        platform_info = result.inputs["platforms"][0]
        assert platform_info["window_days"] == 10
        assert platform_info["gap_days"] == 0
        assert result.status == StatusBand.STRONG

    def test_worst_platform_drives_score(self):
        """Two platforms: one smooth, one bursty. Score should follow the
        bursty platform, not average them out."""
        smooth_rows = _smooth_series("facebook", 14)
        bursty_rows = _bursty_series("stackadapt", 14)
        data = _campaign(
            daily_metrics=smooth_rows + bursty_rows,
            elapsed=14,
            total=30,
        )
        result = compute_d5_delivery_cadence(data)
        assert result.guard_passed
        assert result.inputs["worst_platform"] == "stackadapt"
        # Score should match the bursty platform's score, not a blend
        per_platform = {
            p["platform_id"]: p["score"]
            for p in result.inputs["platforms"]
        }
        assert result.score == pytest.approx(per_platform["stackadapt"], abs=0.1)
        assert per_platform["stackadapt"] < per_platform["facebook"]

    def test_skipped_platforms_surfaced_in_diagnostic(self):
        """Platforms with <7 days get listed as skipped in the diagnostic,
        but they don't block a scorable neighbour."""
        # Facebook: 14 smooth days (scorable)
        # LinkedIn: 3 days (too few to score)
        scorable = _smooth_series("facebook", 14)
        partial = [
            DailyMetrics(
                date=FLIGHT_START + timedelta(days=i),
                platform_id="linkedin",
                impressions=10_000,
            )
            for i in range(3)
        ]
        data = _campaign(
            daily_metrics=scorable + partial,
            elapsed=14,
            total=30,
        )
        result = compute_d5_delivery_cadence(data)
        assert result.guard_passed
        assert result.status == StatusBand.STRONG
        # Platform-label pass (AI-115): copy now uses display labels.
        assert "LinkedIn" in result.diagnostic
        skipped_ids = {
            s["platform_id"] for s in result.inputs["skipped_platforms"]
        }
        assert "linkedin" in skipped_ids

    def test_composite_weights_are_reported(self):
        """Inputs should surface the composite weights so the UI can
        show the breakdown."""
        data = _campaign(
            daily_metrics=_smooth_series("facebook", 14),
            elapsed=14,
            total=30,
        )
        result = compute_d5_delivery_cadence(data)
        assert result.inputs["composite_weights"]["smoothness"] == 0.60
        assert result.inputs["composite_weights"]["gap"] == 0.40
