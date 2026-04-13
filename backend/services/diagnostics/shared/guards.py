"""Guard checks — defensive gates that prevent signals from producing
misleading scores when there isn't enough data.

Every guard returns (passed: bool, reason: str | None).
Signal functions call the relevant guard first and short-circuit with
guard_passed=False if it fails.

Conservative by design: when in doubt, withhold the score rather than
show a number that could mislead (Phase 0 ethos: under-promise,
over-deliver).
"""

from __future__ import annotations

from backend.services.diagnostics.models import CampaignData, FlightContext

# ── Threshold constants ─────────────────────────────────────────────

MIN_IMPRESSIONS = 1_000
MIN_DAYS_ELAPSED = 1
MIN_SPEND = 10.0
MIN_VIDEO_STARTS = 100
MIN_ENGAGEMENTS = 50
MIN_CLICKS = 30
MIN_GA4_SESSIONS = 20
MIN_CONVERSIONS_FOR_CPA = 5


# ── Guard functions ─────────────────────────────────────────────────


def check_min_days(flight: FlightContext, min_days: int = MIN_DAYS_ELAPSED) -> tuple[bool, str | None]:
    """At least N days elapsed since flight start."""
    if flight.elapsed_days < min_days:
        return False, f"min_days_{min_days}"
    return True, None


def check_min_impressions(data: CampaignData, threshold: int = MIN_IMPRESSIONS) -> tuple[bool, str | None]:
    """Enough impressions across all platforms to produce stable rates."""
    total = data.total_impressions
    if total < threshold:
        return False, f"min_impressions_{threshold}"
    return True, None


def check_min_spend(data: CampaignData, threshold: float = MIN_SPEND) -> tuple[bool, str | None]:
    """Enough spend to produce meaningful cost metrics."""
    if data.total_spend < threshold:
        return False, f"min_spend_{threshold}"
    return True, None


def check_min_video_starts(data: CampaignData, threshold: int = MIN_VIDEO_STARTS) -> tuple[bool, str | None]:
    """Enough video starts to produce stable completion rates."""
    total = data.total_video_views_3s
    if total < threshold:
        return False, f"min_video_starts_{threshold}"
    return True, None


def check_min_engagements(data: CampaignData, threshold: int = MIN_ENGAGEMENTS) -> tuple[bool, str | None]:
    """Enough engagements to decompose quality ratios."""
    if data.total_post_engagement < threshold:
        return False, f"min_engagements_{threshold}"
    return True, None


def check_min_clicks(data: CampaignData, threshold: int = MIN_CLICKS) -> tuple[bool, str | None]:
    """Enough clicks for CTR-based signals."""
    if data.total_clicks < threshold:
        return False, f"min_clicks_{threshold}"
    return True, None


def check_min_ga4_sessions(data: CampaignData, threshold: int = MIN_GA4_SESSIONS) -> tuple[bool, str | None]:
    """Enough GA4 sessions for funnel analysis."""
    if data.ga4.sessions < threshold:
        return False, f"min_ga4_sessions_{threshold}"
    return True, None


def check_min_conversions(data: CampaignData, threshold: int = MIN_CONVERSIONS_FOR_CPA) -> tuple[bool, str | None]:
    """Enough conversions to produce a stable CPA."""
    if data.total_conversions < threshold:
        return False, f"min_conversions_{threshold}"
    return True, None


def check_has_media_plan(data: CampaignData) -> tuple[bool, str | None]:
    """Media plan data is available."""
    if not data.media_plan:
        return False, "no_media_plan"
    return True, None


def check_has_planned_impressions(data: CampaignData) -> tuple[bool, str | None]:
    """Media plan has planned impressions (needed for reach attainment)."""
    if data.planned_impressions <= 0:
        return False, "no_planned_impressions"
    return True, None


def check_has_reach_data(data: CampaignData) -> tuple[bool, str | None]:
    """At least one platform reports reach (from fact_adset_daily JOIN)."""
    if data.total_reach <= 0:
        return False, "no_reach_data"
    return True, None


# ── Composite guards ────────────────────────────────────────────────


def guard_distribution(data: CampaignData) -> tuple[bool, str | None]:
    """Standard guard for Distribution pillar signals."""
    passed, reason = check_min_days(data.flight)
    if not passed:
        return passed, reason
    return check_min_impressions(data)


def guard_attention(data: CampaignData) -> tuple[bool, str | None]:
    """Standard guard for Attention pillar signals."""
    passed, reason = check_min_days(data.flight)
    if not passed:
        return passed, reason
    return check_min_video_starts(data)


def guard_resonance(data: CampaignData) -> tuple[bool, str | None]:
    """Standard guard for Resonance pillar signals."""
    passed, reason = check_min_days(data.flight)
    if not passed:
        return passed, reason
    return check_min_engagements(data)
