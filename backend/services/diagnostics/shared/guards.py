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
MIN_VIEWABILITY_MEASURED = 1_000
MIN_DAYS_FOR_FATIGUE = 7
MIN_FORM_STARTS = 10           # F4: minimum form_starts for a stable completion rate
MIN_FORM_SUBMITS = 3           # F5: minimum submissions before we score activation
MIN_LP_SESSIONS_FOR_F2 = 50    # F2: need enough clicks+sessions to score LP load
MIN_CLICKS_FOR_F1 = 500        # F1: CTR stability threshold at campaign level


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


def check_has_quartile_data(data: CampaignData) -> tuple[bool, str | None]:
    """At least one platform has video quartile completion data."""
    totals = sum(
        p.video_q25 + p.video_q50 + p.video_q75 + p.video_q100
        for p in data.platform_metrics
    )
    if totals <= 0:
        return False, "no_quartile_data"
    return True, None


def check_has_viewability_data(
    data: CampaignData, threshold: int = MIN_VIEWABILITY_MEASURED
) -> tuple[bool, str | None]:
    """At least one platform reports measured viewability impressions."""
    total_measured = sum(p.viewability_measured for p in data.platform_metrics)
    if total_measured < threshold:
        return False, f"min_viewability_measured_{threshold}"
    return True, None


def check_min_days_for_fatigue(
    flight: FlightContext, min_days: int = MIN_DAYS_FOR_FATIGUE
) -> tuple[bool, str | None]:
    """Fatigue trend detection needs a rolling window of at least N days."""
    if flight.elapsed_days < min_days:
        return False, f"min_days_{min_days}"
    return True, None


# ── Composite guards ────────────────────────────────────────────────


def guard_distribution(data: CampaignData) -> tuple[bool, str | None]:
    """Standard guard for Distribution pillar signals."""
    passed, reason = check_min_days(data.flight)
    if not passed:
        return passed, reason
    return check_min_impressions(data)


def guard_attention(data: CampaignData) -> tuple[bool, str | None]:
    """Standard guard for Attention pillar signals (shared min thresholds).

    Individual A-signals apply their own additional guards (quartile data,
    viewability data, fatigue window length, etc.) on top of this.
    """
    passed, reason = check_min_days(data.flight)
    if not passed:
        return passed, reason
    return check_min_impressions(data)


def guard_resonance(data: CampaignData) -> tuple[bool, str | None]:
    """Standard guard for Resonance pillar signals."""
    passed, reason = check_min_days(data.flight)
    if not passed:
        return passed, reason
    return check_min_engagements(data)


# ── Funnel-specific guards ──────────────────────────────────────────
#
# F2-F5 depend on data beyond the standard impressions/spend floor — GA4
# sessions, form events, landing page views. Each signal calls the
# appropriate guard and short-circuits when data isn't ready.


def check_has_landing_page_data(
    data: CampaignData, threshold: int = MIN_LP_SESSIONS_FOR_F2
) -> tuple[bool, str | None]:
    """F2 needs both click volume and landing_page_views reported."""
    if data.total_clicks < threshold:
        return False, f"min_clicks_for_lp_{threshold}"
    total_lp_views = sum(p.landing_page_views for p in data.platform_metrics)
    if total_lp_views <= 0:
        return False, "no_landing_page_views"
    return True, None


def check_min_form_starts(
    data: CampaignData, threshold: int = MIN_FORM_STARTS
) -> tuple[bool, str | None]:
    """F4 needs enough form_starts for a stable completion rate."""
    if data.ga4.form_starts < threshold:
        return False, f"min_form_starts_{threshold}"
    return True, None


def check_min_form_submits(
    data: CampaignData, threshold: int = MIN_FORM_SUBMITS
) -> tuple[bool, str | None]:
    """F5 needs some form submissions before we can meaningfully score
    post-conversion activation."""
    if data.ga4.form_submits < threshold:
        return False, f"min_form_submits_{threshold}"
    return True, None


def check_has_form_friction_data(data: CampaignData) -> tuple[bool, str | None]:
    """At least one conversion line must have FFS inputs for F4 to pick the
    right benchmark (LP vs in-platform form)."""
    has_ffs = any(
        l.ffs_score is not None or l.ffs_inputs is not None
        for l in data.media_plan
    )
    if not has_ffs:
        return False, "no_ffs_data"
    return True, None


def guard_funnel(data: CampaignData) -> tuple[bool, str | None]:
    """Base guard for Funnel pillar signals — enough days + impressions to
    start evaluating conversion flow mechanics."""
    passed, reason = check_min_days(data.flight)
    if not passed:
        return passed, reason
    return check_min_impressions(data)
