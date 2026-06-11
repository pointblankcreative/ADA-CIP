"""Unit tests for the Funnel pillar (F1-F5) of the conversion diagnostic.

Covers:
    - F1 CTR with platform-weighted benchmark + non-clickable handling
    - F2 LP load rate with Arch B guard-fail
    - F3 scroll/discovery composite with form-position awareness
    - F4 form completion with FFS-adjusted target + platform-form path
    - F5 post-conversion activation (GA4 key_events + on_platform_leads
      fallback)
    - Pillar rollup: Arch A only, Arch B only, mixed architecture blend
    - Line-level architecture classification via ffs_inputs and objective
"""

from __future__ import annotations

from datetime import date, timedelta

import pytest

from backend.services.diagnostics.models import (
    AudienceType,
    CampaignData,
    CampaignType,
    FlightContext,
    GA4Metrics,
    MediaPlanLine,
    PlatformMetrics,
    StatusBand,
)
from backend.services.diagnostics.conversion.funnel import (
    _classify_line_architecture,
    _compute_arch_mix,
    _dominant_form_position,
    compute_f1_ctr,
    compute_f2_lp_load_rate,
    compute_f3_scroll_discovery,
    compute_f4_form_completion,
    compute_f5_activation,
    compute_funnel_pillar,
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


def _campaign(
    platform_metrics: list[PlatformMetrics] | None = None,
    media_plan: list[MediaPlanLine] | None = None,
    ga4: GA4Metrics | None = None,
    elapsed: int = 14,
    total: int = 30,
) -> CampaignData:
    return CampaignData(
        project_code="TEST-FUN",
        campaign_type=CampaignType.CONVERSION,
        flight=_flight(elapsed, total),
        platform_metrics=platform_metrics or [],
        media_plan=media_plan or [],
        ga4=ga4 or GA4Metrics(),
    )


def _arch_a_line(
    budget: float = 10_000,
    ffs: float | None = 25.0,
    line_id: str = "line-a",
    below_fold: bool = False,
    platform_id: str = "facebook",
) -> MediaPlanLine:
    """Landing-page media plan line (Arch A)."""
    return MediaPlanLine(
        line_id=line_id,
        platform_id=platform_id,
        planned_budget=budget,
        planned_impressions=1_000_000,
        audience_type=AudienceType.MEMBER_LIST,
        ffs_score=ffs,
        ffs_inputs={"is_platform_form": False, "below_fold_mobile": below_fold},
        objective="Conversion",
    )


def _arch_b_line(
    budget: float = 5_000,
    line_id: str = "line-b",
) -> MediaPlanLine:
    """In-platform form media plan line (Arch B)."""
    return MediaPlanLine(
        line_id=line_id,
        platform_id="facebook",
        planned_budget=budget,
        planned_impressions=500_000,
        audience_type=AudienceType.PROSPECTING,
        ffs_score=10.0,
        ffs_inputs={"is_platform_form": True},
        objective="Lead Gen - Meta Instant Form",
    )


def _meta_platform(
    spend: float = 7_000,
    impressions: int = 750_000,
    clicks: int = 7_500,
    lp_views: int = 6_000,
    on_platform_leads: float = 0,
) -> PlatformMetrics:
    return PlatformMetrics(
        platform_id="facebook",
        spend=spend,
        impressions=impressions,
        clicks=clicks,
        landing_page_views=lp_views,
        on_platform_leads=on_platform_leads,
    )


def _ctv_platform(spend: float = 3_000, impressions: int = 500_000) -> PlatformMetrics:
    """Non-clickable CTV placement — F1 should exclude its share."""
    return PlatformMetrics(
        platform_id="stackadapt_ctv",
        spend=spend,
        impressions=impressions,
        clicks=0,
    )


def _healthy_ga4() -> GA4Metrics:
    # Tuned so the healthy fixture produces STRONG across F3 and F5.
    # Recalibrated 2026-04-20 after F3 weights flipped to 30/70
    # (scroll/discovery) — discovery is now the dominant F3 component.
    # - scroll_rate = 3000/5000 = 0.60  (above 0.50 benchmark)
    # - discovery_rate = 2500/5000 = 0.50  (at the mid_page target)
    # - F5 activation = (425-325)/325 ≈ 0.308 (above the 0.20 benchmark)
    return GA4Metrics(
        sessions=5_000,
        scrolls=3_000,
        engaged_sessions=3_500,
        form_starts=2_500,
        form_submits=325,
        key_events=425,   # 100 post-submit key events after subtracting 325 submits
    )


def _weak_ga4() -> GA4Metrics:
    return GA4Metrics(
        sessions=2_000,
        scrolls=300,
        engaged_sessions=500,
        form_starts=50,
        form_submits=10,
        key_events=12,
    )


# ── Architecture classification ─────────────────────────────────────


class TestArchitectureClassification:

    def test_line_with_platform_form_flag_is_arch_b(self):
        line = _arch_b_line()
        assert _classify_line_architecture(line) == "arch_b"

    def test_line_without_flag_is_arch_a(self):
        line = _arch_a_line()
        assert _classify_line_architecture(line) == "arch_a"

    def test_objective_keyword_triggers_arch_b(self):
        """Objective text like 'Instant Form' should flip to Arch B even
        without the explicit ffs_inputs flag."""
        line = MediaPlanLine(
            line_id="test",
            planned_budget=1_000,
            objective="Lead Gen – Meta Instant Form",
        )
        assert _classify_line_architecture(line) == "arch_b"

    def test_mix_returns_spend_shares(self):
        data = _campaign(media_plan=[
            _arch_a_line(budget=8_000),
            _arch_b_line(budget=2_000),
        ])
        mix = _compute_arch_mix(data)
        assert mix.arch_a_share == pytest.approx(0.8, abs=0.001)
        assert mix.arch_b_share == pytest.approx(0.2, abs=0.001)
        assert mix.is_mixed
        assert not mix.is_arch_a_only
        assert not mix.is_arch_b_only

    def test_pure_arch_a(self):
        data = _campaign(media_plan=[_arch_a_line()])
        mix = _compute_arch_mix(data)
        assert mix.is_arch_a_only
        assert mix.arch_a_share == 1.0
        assert mix.arch_b_share == 0.0

    def test_pure_arch_b(self):
        data = _campaign(media_plan=[_arch_b_line()])
        mix = _compute_arch_mix(data)
        assert mix.is_arch_b_only
        assert mix.arch_b_share == 1.0

    def test_dominant_form_position_reads_fold_flag(self):
        below = _arch_a_line(below_fold=True)
        above = _arch_a_line(below_fold=False)
        assert _dominant_form_position([below, below, above]) == "below_fold"
        assert _dominant_form_position([above, above]) == "mid_page"
        assert _dominant_form_position([]) == "unknown"


# ── F1: CTR ─────────────────────────────────────────────────────────


class TestF1ClickThroughRate:

    def test_guard_insufficient_days(self):
        result = compute_f1_ctr(
            _campaign(
                platform_metrics=[_meta_platform()],
                media_plan=[_arch_a_line()],
                elapsed=0,
            )
        )
        assert not result.guard_passed

    def test_guard_insufficient_clicks(self):
        result = compute_f1_ctr(
            _campaign(
                platform_metrics=[_meta_platform(clicks=10, impressions=5_000)],
                media_plan=[_arch_a_line()],
            )
        )
        # Still passes the guard_funnel (impressions=5000 > 1000) but fails
        # the min_clicks(30) guard inside F1.
        assert not result.guard_passed
        assert "min_clicks" in (result.guard_reason or "")

    def test_healthy_ctr_scores_above_benchmark(self):
        """A Meta campaign with CTR ~1% (above 0.90% benchmark) should
        land in STRONG territory."""
        result = compute_f1_ctr(
            _campaign(
                platform_metrics=[_meta_platform(
                    impressions=500_000, clicks=5_000,
                )],
                media_plan=[_arch_a_line()],
            )
        )
        assert result.guard_passed
        assert result.raw_value == pytest.approx(0.01, abs=0.0001)  # 1.0%
        assert result.score is not None
        assert result.status == StatusBand.STRONG

    def test_weak_ctr_scores_action(self):
        result = compute_f1_ctr(
            _campaign(
                platform_metrics=[_meta_platform(
                    impressions=1_000_000, clicks=1_000,  # 0.1% CTR
                )],
                media_plan=[_arch_a_line()],
            )
        )
        assert result.guard_passed
        assert result.status == StatusBand.ACTION

    def test_non_clickable_platforms_excluded(self):
        """CTV spend shouldn't drag the CTR benchmark — it's excluded from
        both numerator and denominator."""
        result = compute_f1_ctr(
            _campaign(
                platform_metrics=[
                    _meta_platform(impressions=500_000, clicks=5_000),
                    _ctv_platform(spend=3_000, impressions=500_000),
                ],
                media_plan=[_arch_a_line()],
            )
        )
        assert result.guard_passed
        # CTR computed only over Meta (500k impr / 5k clicks = 1.0%)
        assert result.raw_value == pytest.approx(0.01, abs=0.0001)
        assert result.inputs["non_clickable_spend"] == 3_000


# ── F2: LP Load Rate ────────────────────────────────────────────────


class TestF2LandingPageLoadRate:

    def test_arch_b_only_guard_fails(self):
        """Pure Arch B campaign has no LP to score."""
        from backend.services.diagnostics.conversion.funnel import _compute_arch_mix
        data = _campaign(
            platform_metrics=[_meta_platform(on_platform_leads=50)],
            media_plan=[_arch_b_line()],
        )
        mix = _compute_arch_mix(data)
        result = compute_f2_lp_load_rate(data, mix)
        assert not result.guard_passed
        assert result.guard_reason == "arch_b_only"

    def test_no_lp_views_guard_fails(self):
        from backend.services.diagnostics.conversion.funnel import _compute_arch_mix
        data = _campaign(
            platform_metrics=[_meta_platform(lp_views=0)],
            media_plan=[_arch_a_line()],
        )
        mix = _compute_arch_mix(data)
        result = compute_f2_lp_load_rate(data, mix)
        assert not result.guard_passed

    def test_healthy_load_rate_scores_strong(self):
        from backend.services.diagnostics.conversion.funnel import _compute_arch_mix
        data = _campaign(
            platform_metrics=[_meta_platform(clicks=5_000, lp_views=4_500)],
            media_plan=[_arch_a_line()],
        )
        mix = _compute_arch_mix(data)
        result = compute_f2_lp_load_rate(data, mix)
        assert result.guard_passed
        assert result.raw_value == pytest.approx(0.9, abs=0.01)
        assert result.status == StatusBand.STRONG

    def test_poor_load_rate_scores_action(self):
        from backend.services.diagnostics.conversion.funnel import _compute_arch_mix
        data = _campaign(
            platform_metrics=[_meta_platform(clicks=5_000, lp_views=1_500)],
            media_plan=[_arch_a_line()],
        )
        mix = _compute_arch_mix(data)
        result = compute_f2_lp_load_rate(data, mix)
        assert result.guard_passed
        # 30% load rate — well below the 50% floor
        assert result.raw_value == pytest.approx(0.3, abs=0.01)
        assert result.status == StatusBand.ACTION

    def test_load_rate_capped_at_one(self):
        """Over-counted LP views should cap at 1.0."""
        from backend.services.diagnostics.conversion.funnel import _compute_arch_mix
        data = _campaign(
            platform_metrics=[_meta_platform(clicks=1_000, lp_views=1_500)],
            media_plan=[_arch_a_line()],
        )
        mix = _compute_arch_mix(data)
        result = compute_f2_lp_load_rate(data, mix)
        assert result.guard_passed
        assert result.raw_value == 1.0

    def test_flags_overcounting_when_lp_views_exceed_clicks(self):
        """When LP views exceed clicks by >10%, flag overcounting in diagnostic."""
        from backend.services.diagnostics.conversion.funnel import _compute_arch_mix
        data = _campaign(
            platform_metrics=[_meta_platform(clicks=1_000, lp_views=1_500)],
            media_plan=[_arch_a_line()],
        )
        mix = _compute_arch_mix(data)
        result = compute_f2_lp_load_rate(data, mix)
        assert result.guard_passed
        assert result.inputs.get("overcounting_flagged") is True
        assert result.inputs.get("raw_rate") == pytest.approx(1.5, abs=0.01)

    def test_does_not_flag_overcounting_at_normal_levels(self):
        """Healthy ~90% load rate should not trigger overcounting flag."""
        from backend.services.diagnostics.conversion.funnel import _compute_arch_mix
        data = _campaign(
            platform_metrics=[_meta_platform(clicks=5_000, lp_views=4_500)],
            media_plan=[_arch_a_line()],
        )
        mix = _compute_arch_mix(data)
        result = compute_f2_lp_load_rate(data, mix)
        assert result.guard_passed
        assert result.inputs.get("overcounting_flagged") is False

    def test_excludes_non_reporting_platforms_from_score(self):
        """Platforms with clicks but no LP views tracked as non-reporting only."""
        from backend.services.diagnostics.conversion.funnel import _compute_arch_mix
        data = _campaign(
            platform_metrics=[
                _meta_platform(clicks=2_000, lp_views=1_800),
                PlatformMetrics(
                    platform_id="stackadapt",
                    spend=1_000,
                    impressions=100_000,
                    clicks=500,
                    landing_page_views=0,
                ),
            ],
            media_plan=[_arch_a_line()],
        )
        mix = _compute_arch_mix(data)
        result = compute_f2_lp_load_rate(data, mix)
        assert result.guard_passed
        # Rate only from reporting platform: 1800/2000 = 0.90
        assert result.raw_value == pytest.approx(0.9, abs=0.01)
        assert "stackadapt" in result.inputs.get("non_reporting_platforms", [])

    def test_prefers_outbound_clicks_when_populated(self):
        """Meta case: clicks includes noise (reactions, profile clicks).
        When outbound_clicks is populated, it should be the denominator —
        otherwise healthy campaigns look catastrophically bad."""
        from backend.services.diagnostics.conversion.funnel import _compute_arch_mix
        data = _campaign(
            platform_metrics=[
                PlatformMetrics(
                    platform_id="facebook",
                    spend=7_000,
                    impressions=750_000,
                    clicks=15_000,           # Total clicks (includes on-platform noise)
                    outbound_clicks=5_000,   # Clicks that actually tried to leave
                    landing_page_views=4_500,
                ),
            ],
            media_plan=[_arch_a_line()],
        )
        mix = _compute_arch_mix(data)
        result = compute_f2_lp_load_rate(data, mix)
        assert result.guard_passed
        # Should use outbound_clicks: 4500/5000 = 0.90, not 4500/15000 = 0.30
        assert result.raw_value == pytest.approx(0.9, abs=0.01)
        assert result.status == StatusBand.STRONG
        assert result.inputs["reporting_denominator"] == 5_000
        assert (
            result.inputs["platforms"]["facebook"]["denominator_source"]
            == "outbound_clicks"
        )

    def test_arch_b_only_platform_excluded_on_mixed_campaign(self):
        """26018 regression: Meta runs pure lead-form lines but emits a
        trickle of incidental landing_page_views, which used to make it a
        'reporting' platform and score F2 at ~0 off lead-form clicks.
        With the Arch A platform (google_ads) not reporting LP views,
        F2 must guard-fail rather than score."""
        from backend.services.diagnostics.conversion.funnel import _compute_arch_mix
        data = _campaign(
            platform_metrics=[
                _meta_platform(clicks=4_300, lp_views=2, on_platform_leads=1_026),
                PlatformMetrics(
                    platform_id="google_ads",
                    spend=430,
                    impressions=10_000,
                    clicks=25,
                    landing_page_views=0,
                ),
            ],
            media_plan=[
                _arch_b_line(budget=3_283),  # facebook, is_platform_form=True
                _arch_a_line(budget=430, line_id="line-g", platform_id="google_ads"),
            ],
        )
        mix = _compute_arch_mix(data)
        assert mix.is_mixed
        result = compute_f2_lp_load_rate(data, mix)
        assert not result.guard_passed
        assert result.guard_reason == "no_arch_a_lp_reporting"
        # Platform-label pass (AI-115): "facebook" renders as Meta.
        assert "Meta" in result.diagnostic

    def test_arch_b_platform_excluded_but_arch_a_platform_scores(self):
        """When an Arch A platform reports LP views, the Arch B platform's
        clicks and stray LP views must not drag the rate."""
        from backend.services.diagnostics.conversion.funnel import _compute_arch_mix
        data = _campaign(
            platform_metrics=[
                _meta_platform(clicks=4_000, lp_views=3),  # lead-form noise
                PlatformMetrics(
                    platform_id="stackadapt",
                    spend=1_000,
                    impressions=100_000,
                    clicks=2_000,
                    landing_page_views=1_800,
                ),
            ],
            media_plan=[
                _arch_b_line(budget=5_000),  # facebook
                _arch_a_line(budget=2_000, line_id="line-s", platform_id="stackadapt"),
            ],
        )
        mix = _compute_arch_mix(data)
        result = compute_f2_lp_load_rate(data, mix)
        assert result.guard_passed
        # Rate from stackadapt only: 1800/2000 = 0.90
        assert result.raw_value == pytest.approx(0.9, abs=0.01)
        assert result.inputs["excluded_arch_b_platforms"] == ["facebook"]
        assert (
            result.inputs["platforms"]["facebook"]["excluded_reason"]
            == "arch_b_platform_form"
        )

    def test_platform_with_both_arch_lines_stays_included(self):
        """A platform carrying both Arch A and Arch B lines can't be split
        at line grain — it stays in F2 (phase-2-5 limitation)."""
        from backend.services.diagnostics.conversion.funnel import _compute_arch_mix
        data = _campaign(
            platform_metrics=[_meta_platform(clicks=5_000, lp_views=4_500)],
            media_plan=[
                _arch_a_line(budget=8_000),   # facebook
                _arch_b_line(budget=2_000),   # facebook
            ],
        )
        mix = _compute_arch_mix(data)
        result = compute_f2_lp_load_rate(data, mix)
        assert result.guard_passed
        assert result.raw_value == pytest.approx(0.9, abs=0.01)
        assert result.inputs["excluded_arch_b_platforms"] == []

    def test_falls_back_to_clicks_when_outbound_not_populated(self):
        """Platforms that don't report outbound_clicks (DSPs, LinkedIn
        historically) should keep using clicks as the denominator."""
        from backend.services.diagnostics.conversion.funnel import _compute_arch_mix
        data = _campaign(
            platform_metrics=[
                PlatformMetrics(
                    platform_id="stackadapt",
                    spend=3_000,
                    impressions=400_000,
                    clicks=2_000,
                    outbound_clicks=0,       # Not populated by this platform
                    landing_page_views=1_800,
                ),
            ],
            media_plan=[_arch_a_line()],
        )
        mix = _compute_arch_mix(data)
        result = compute_f2_lp_load_rate(data, mix)
        assert result.guard_passed
        assert result.raw_value == pytest.approx(0.9, abs=0.01)
        assert result.inputs["reporting_denominator"] == 2_000
        assert (
            result.inputs["platforms"]["stackadapt"]["denominator_source"]
            == "clicks"
        )


# ── F3: Scroll / Form Discovery ─────────────────────────────────────


class TestF3ScrollDiscovery:

    def test_arch_b_only_guard_fails(self):
        from backend.services.diagnostics.conversion.funnel import _compute_arch_mix
        data = _campaign(
            platform_metrics=[_meta_platform()],
            media_plan=[_arch_b_line()],
            ga4=_healthy_ga4(),
        )
        mix = _compute_arch_mix(data)
        result = compute_f3_scroll_discovery(data, mix)
        assert not result.guard_passed
        assert result.guard_reason == "arch_b_only"

    def test_no_ga4_sessions_guard_fails(self):
        from backend.services.diagnostics.conversion.funnel import _compute_arch_mix
        data = _campaign(
            platform_metrics=[_meta_platform()],
            media_plan=[_arch_a_line()],
            ga4=GA4Metrics(sessions=5, scrolls=2),
        )
        mix = _compute_arch_mix(data)
        result = compute_f3_scroll_discovery(data, mix)
        assert not result.guard_passed

    def test_healthy_scores_strong(self):
        from backend.services.diagnostics.conversion.funnel import _compute_arch_mix
        data = _campaign(
            platform_metrics=[_meta_platform()],
            media_plan=[_arch_a_line()],
            ga4=_healthy_ga4(),
        )
        mix = _compute_arch_mix(data)
        result = compute_f3_scroll_discovery(data, mix)
        assert result.guard_passed
        assert result.status == StatusBand.STRONG
        assert result.inputs["scroll_rate"] == pytest.approx(0.6, abs=0.01)

    def test_position_target_below_fold_is_lower(self):
        """A below-fold form has a lower discovery target, so the same raw
        discovery rate should produce a higher normalized score relative
        to a mid-page form."""
        from backend.services.diagnostics.conversion.funnel import _compute_arch_mix
        ga4 = GA4Metrics(sessions=1_000, scrolls=500, form_starts=300, form_submits=100)

        # Same metrics, different form position
        data_below = _campaign(
            platform_metrics=[_meta_platform()],
            media_plan=[_arch_a_line(below_fold=True)],
            ga4=ga4,
        )
        data_above = _campaign(
            platform_metrics=[_meta_platform()],
            media_plan=[_arch_a_line(below_fold=False)],
            ga4=ga4,
        )
        mix_below = _compute_arch_mix(data_below)
        mix_above = _compute_arch_mix(data_above)
        score_below = compute_f3_scroll_discovery(data_below, mix_below).inputs["discovery_score"]
        score_above = compute_f3_scroll_discovery(data_above, mix_above).inputs["discovery_score"]
        # 30% discovery vs below_fold target (0.30) scores higher than
        # 30% discovery vs mid_page target (0.50).
        assert score_below > score_above

    def test_weak_scores_action(self):
        from backend.services.diagnostics.conversion.funnel import _compute_arch_mix
        data = _campaign(
            platform_metrics=[_meta_platform()],
            media_plan=[_arch_a_line()],
            ga4=_weak_ga4(),
        )
        mix = _compute_arch_mix(data)
        result = compute_f3_scroll_discovery(data, mix)
        assert result.guard_passed
        assert result.status == StatusBand.ACTION

    def test_scores_discovery_alone_when_scroll_tracking_absent(self):
        """Scroll events absent on healthy sessions → drop scroll component,
        score on discovery alone, flag in diagnostic."""
        from backend.services.diagnostics.conversion.funnel import _compute_arch_mix
        ga4 = GA4Metrics(
            sessions=5_000,
            scrolls=0,  # tracking not wired up
            engaged_sessions=3_500,
            form_starts=2_500,  # healthy 50% discovery
        )
        data = _campaign(
            platform_metrics=[_meta_platform()],
            media_plan=[_arch_a_line()],
            ga4=ga4,
        )
        mix = _compute_arch_mix(data)
        result = compute_f3_scroll_discovery(data, mix)
        assert result.guard_passed
        assert result.inputs["scroll_tracking_present"] is False
        # raw_value should equal discovery_rate only (no scroll blend)
        assert result.raw_value == pytest.approx(0.50, abs=0.01)
        # Discovery alone at target → STRONG
        assert result.status == StatusBand.STRONG
        # Plain-language pass (AI-115): flag now reads "GA4 isn't
        # recording scroll events here, so this reads on form discovery
        # alone."
        assert "scroll events" in result.diagnostic.lower()
        assert "form discovery alone" in result.diagnostic.lower()

    def test_scroll_absent_with_weak_discovery_scores_action(self):
        """Scroll absent + weak discovery → ACTION on discovery alone,
        no silent drag from the missing scroll signal."""
        from backend.services.diagnostics.conversion.funnel import _compute_arch_mix
        ga4 = GA4Metrics(
            sessions=5_000,
            scrolls=0,
            engaged_sessions=3_500,
            form_starts=250,  # 5% discovery — very low
        )
        data = _campaign(
            platform_metrics=[_meta_platform()],
            media_plan=[_arch_a_line()],
            ga4=ga4,
        )
        mix = _compute_arch_mix(data)
        result = compute_f3_scroll_discovery(data, mix)
        assert result.guard_passed
        assert result.inputs["scroll_tracking_present"] is False
        assert result.status == StatusBand.ACTION


# ── F4: Form Completion ─────────────────────────────────────────────


class TestF4FormCompletion:

    def test_guard_insufficient_form_starts(self):
        from backend.services.diagnostics.conversion.funnel import _compute_arch_mix
        data = _campaign(
            platform_metrics=[_meta_platform()],
            media_plan=[_arch_a_line()],
            ga4=GA4Metrics(sessions=1_000, form_starts=5, form_submits=2),
        )
        mix = _compute_arch_mix(data)
        result = compute_f4_form_completion(data, mix)
        assert not result.guard_passed

    def test_arch_a_scores_against_ffs_target(self):
        """FFS 25 → target ~0.65 * exp(-0.012*25) ≈ 0.482. Completion at
        65% should score above the target → STRONG."""
        from backend.services.diagnostics.conversion.funnel import _compute_arch_mix
        data = _campaign(
            platform_metrics=[_meta_platform()],
            media_plan=[_arch_a_line(ffs=25.0)],
            ga4=GA4Metrics(form_starts=500, form_submits=325, sessions=2_000),
        )
        mix = _compute_arch_mix(data)
        result = compute_f4_form_completion(data, mix)
        assert result.guard_passed
        assert result.raw_value == pytest.approx(0.65, abs=0.01)
        # target ~0.48, actual 0.65 → well above → STRONG
        assert result.status == StatusBand.STRONG

    def test_arch_a_high_friction_lowers_target(self):
        """FFS 70 sharply lowers the expected completion — a 50% actual
        completion against a ~0.28 target should land in STRONG, not WATCH."""
        from backend.services.diagnostics.conversion.funnel import _compute_arch_mix
        data = _campaign(
            platform_metrics=[_meta_platform()],
            media_plan=[_arch_a_line(ffs=70.0)],
            ga4=GA4Metrics(form_starts=200, form_submits=100, sessions=1_000),
        )
        mix = _compute_arch_mix(data)
        result = compute_f4_form_completion(data, mix)
        # benchmark should reflect the high FFS adjustment
        assert result.benchmark < 0.35

    def test_arch_b_uses_click_to_lead_benchmark(self):
        """Pure Arch B: scored on click→lead (NOT form→submit). Healthy
        click→lead rate should score STRONG against the 15% benchmark."""
        from backend.services.diagnostics.conversion.funnel import _compute_arch_mix
        data = _campaign(
            platform_metrics=[_meta_platform(clicks=1_000, on_platform_leads=200)],
            media_plan=[_arch_b_line()],
            # No GA4 form_starts — Arch B doesn't need them
            ga4=GA4Metrics(),
        )
        mix = _compute_arch_mix(data)
        result = compute_f4_form_completion(data, mix)
        assert result.guard_passed
        # 200 leads / 1000 clicks = 0.20 click→lead (above 15% benchmark)
        assert result.raw_value == pytest.approx(0.20, abs=0.01)
        assert result.benchmark == pytest.approx(0.15, abs=0.001)
        assert result.floor == pytest.approx(0.03, abs=0.001)
        assert result.status == StatusBand.STRONG
        assert result.inputs["measurement"] == "click_to_lead"

    def test_arch_b_typical_click_to_lead_is_no_longer_stuck_on_action(self):
        """Realistic 10% click→lead rate (common for Meta Lead Ads) now
        produces WATCH rather than ACTION under the old form-benchmark."""
        from backend.services.diagnostics.conversion.funnel import _compute_arch_mix
        data = _campaign(
            platform_metrics=[_meta_platform(clicks=2_000, on_platform_leads=200)],
            media_plan=[_arch_b_line()],
            ga4=GA4Metrics(),
        )
        mix = _compute_arch_mix(data)
        result = compute_f4_form_completion(data, mix)
        assert result.guard_passed
        # 10% click→lead: above 3% floor, below 15% benchmark → WATCH
        assert result.raw_value == pytest.approx(0.10, abs=0.01)
        assert result.status == StatusBand.WATCH

    def test_arch_b_low_click_to_lead_scores_action(self):
        """Under the 3% floor → ACTION with click→lead diagnostic."""
        from backend.services.diagnostics.conversion.funnel import _compute_arch_mix
        data = _campaign(
            platform_metrics=[_meta_platform(clicks=2_000, on_platform_leads=40)],
            media_plan=[_arch_b_line()],
            ga4=GA4Metrics(),
        )
        mix = _compute_arch_mix(data)
        result = compute_f4_form_completion(data, mix)
        assert result.guard_passed
        # 2% click→lead, below 3% floor → ACTION
        assert result.raw_value == pytest.approx(0.02, abs=0.01)
        assert result.status == StatusBand.ACTION
        # Plain-language pass (AI-115): Arch B copy now describes the
        # click→lead rate as people tapping the ad and completing the
        # in-platform form.
        assert "in-platform form" in result.diagnostic.lower()

    def test_arch_a_flags_missing_ffs_in_diagnostic(self):
        """When FFS is missing on Arch A lines, the diagnostic should
        surface it rather than silently scoring against the base."""
        from backend.services.diagnostics.conversion.funnel import _compute_arch_mix
        data = _campaign(
            platform_metrics=[_meta_platform()],
            media_plan=[_arch_a_line(ffs=None)],
            ga4=GA4Metrics(form_starts=400, form_submits=180, sessions=2_000),
        )
        mix = _compute_arch_mix(data)
        result = compute_f4_form_completion(data, mix)
        assert result.guard_passed
        assert result.inputs["ffs_available"] is False
        assert "ffs" in result.diagnostic.lower() or "friction score" in result.diagnostic.lower()

    def test_arch_a_with_ffs_does_not_flag(self):
        """When FFS is present, no missing-FFS flag in the diagnostic."""
        from backend.services.diagnostics.conversion.funnel import _compute_arch_mix
        data = _campaign(
            platform_metrics=[_meta_platform()],
            media_plan=[_arch_a_line(ffs=25.0)],
            ga4=GA4Metrics(form_starts=500, form_submits=325, sessions=2_000),
        )
        mix = _compute_arch_mix(data)
        result = compute_f4_form_completion(data, mix)
        assert result.guard_passed
        assert result.inputs["ffs_available"] is True
        assert "no form friction score" not in result.diagnostic.lower()

    def test_arch_b_no_leads_guard_fails(self):
        from backend.services.diagnostics.conversion.funnel import _compute_arch_mix
        data = _campaign(
            platform_metrics=[_meta_platform(clicks=1_000, on_platform_leads=0)],
            media_plan=[_arch_b_line()],
        )
        mix = _compute_arch_mix(data)
        result = compute_f4_form_completion(data, mix)
        assert not result.guard_passed
        assert result.guard_reason == "no_in_platform_leads"

    def test_mixed_blends_benchmark(self):
        """Mixed campaign: target should blend Arch A (FFS-adjusted) and
        Arch B (boosted) benchmarks pro-rata by spend share."""
        from backend.services.diagnostics.conversion.funnel import (
            _arch_a_f4_target, _arch_b_f4_target, _compute_arch_mix
        )
        data = _campaign(
            platform_metrics=[_meta_platform()],
            media_plan=[
                _arch_a_line(budget=8_000, ffs=25.0),
                _arch_b_line(budget=2_000),
            ],
            ga4=GA4Metrics(form_starts=400, form_submits=200, sessions=2_000),
        )
        mix = _compute_arch_mix(data)
        result = compute_f4_form_completion(data, mix)
        assert result.guard_passed
        # Expected target = 0.8 * target_a + 0.2 * target_b
        target_a, _ = _arch_a_f4_target(mix.arch_a_lines)
        expected_target = (
            target_a * mix.arch_a_share
            + _arch_b_f4_target() * mix.arch_b_share
        )
        assert result.benchmark == pytest.approx(round(expected_target, 3), abs=0.01)


# ── F5: Post-Conversion Activation ──────────────────────────────────


class TestF5Activation:

    def test_no_conversions_guard_fails(self):
        from backend.services.diagnostics.conversion.funnel import _compute_arch_mix
        data = _campaign(
            platform_metrics=[_meta_platform()],
            media_plan=[_arch_a_line()],
            ga4=GA4Metrics(form_submits=0),
        )
        mix = _compute_arch_mix(data)
        result = compute_f5_activation(data, mix)
        assert not result.guard_passed

    def test_healthy_activation_scores_strong(self):
        """GA4 key_events include form_submits — we subtract them to
        count only follow-on activations. 325 submits, 425 key_events →
        100 activations / 325 submits ≈ 30.8% → above 20% benchmark."""
        from backend.services.diagnostics.conversion.funnel import _compute_arch_mix
        data = _campaign(
            platform_metrics=[_meta_platform()],
            media_plan=[_arch_a_line()],
            ga4=_healthy_ga4(),
        )
        mix = _compute_arch_mix(data)
        result = compute_f5_activation(data, mix)
        assert result.guard_passed
        assert result.raw_value == pytest.approx(0.308, abs=0.01)
        assert result.status == StatusBand.STRONG

    def test_weak_activation_scores_action(self):
        from backend.services.diagnostics.conversion.funnel import _compute_arch_mix
        data = _campaign(
            platform_metrics=[_meta_platform()],
            media_plan=[_arch_a_line()],
            ga4=GA4Metrics(
                sessions=1_000, form_starts=50, form_submits=10, key_events=10,
            ),
        )
        mix = _compute_arch_mix(data)
        result = compute_f5_activation(data, mix)
        assert result.guard_passed
        # 0 follow-on events / 10 submits = 0 → score 0 → ACTION
        assert result.status == StatusBand.ACTION

    def test_arch_b_only_guard_fails(self):
        """Pure Arch B has no reliable post-conversion GA4 signal —
        F5 guard-fails cleanly and drops out of the pillar."""
        from backend.services.diagnostics.conversion.funnel import _compute_arch_mix
        data = _campaign(
            platform_metrics=[_meta_platform(on_platform_leads=100)],
            media_plan=[_arch_b_line()],
            ga4=GA4Metrics(key_events=25),   # events shouldn't matter here
        )
        mix = _compute_arch_mix(data)
        result = compute_f5_activation(data, mix)
        assert not result.guard_passed
        assert result.guard_reason == "arch_b_no_activation_signal"

    def test_mixed_uses_on_platform_leads_when_no_form_submits(self):
        """Mixed campaign with no GA4 form_submits yet but in-platform
        leads flowing — F5 should still score using leads as denominator."""
        from backend.services.diagnostics.conversion.funnel import _compute_arch_mix
        data = _campaign(
            platform_metrics=[_meta_platform(on_platform_leads=100)],
            media_plan=[
                _arch_a_line(budget=8_000),
                _arch_b_line(budget=2_000),
            ],
            ga4=GA4Metrics(key_events=25, sessions=1_000),
        )
        mix = _compute_arch_mix(data)
        result = compute_f5_activation(data, mix)
        assert result.guard_passed
        # 25 / 100 = 0.25 (above 0.20 benchmark)
        assert result.raw_value == pytest.approx(0.25, abs=0.01)

    def test_zero_key_events_guard_fails_not_action(self):
        """Healthy conversions but no key_events configured → guard-fail
        with tracking-setup message, not false ACTION."""
        from backend.services.diagnostics.conversion.funnel import _compute_arch_mix
        data = _campaign(
            platform_metrics=[_meta_platform()],
            media_plan=[_arch_a_line()],
            ga4=GA4Metrics(
                sessions=2_000,
                form_starts=500,
                form_submits=100,
                key_events=0,  # tracking not configured
            ),
        )
        mix = _compute_arch_mix(data)
        result = compute_f5_activation(data, mix)
        assert not result.guard_passed
        assert result.guard_reason == "no_key_events_configured"


# ── Pillar rollup ───────────────────────────────────────────────────


class TestFunnelPillar:

    def test_pure_arch_a_pillar(self):
        pillar = compute_funnel_pillar(
            _campaign(
                platform_metrics=[_meta_platform(
                    impressions=500_000, clicks=5_000, lp_views=4_500,
                )],
                media_plan=[_arch_a_line()],
                ga4=_healthy_ga4(),
            )
        )
        assert pillar.name == "funnel"
        assert pillar.weight == 0.57
        assert len(pillar.signals) == 5
        active = {s.id for s in pillar.signals if s.guard_passed}
        # All five should pass
        assert active == {"F1", "F2", "F3", "F4", "F5"}
        assert pillar.score is not None

    def test_pure_arch_b_pillar(self):
        """Arch B: F2/F3/F5 all guard-fail (no LP, no post-conversion GA4
        signal); only F1 + F4 are active."""
        pillar = compute_funnel_pillar(
            _campaign(
                platform_metrics=[_meta_platform(
                    impressions=500_000, clicks=5_000,
                    on_platform_leads=400, lp_views=0,
                )],
                media_plan=[_arch_b_line()],
                ga4=GA4Metrics(key_events=100),  # Follow-on events, no form_submits
            )
        )
        active = {s.id for s in pillar.signals if s.guard_passed}
        assert "F1" in active
        assert "F4" in active
        assert "F2" not in active
        assert "F3" not in active
        assert "F5" not in active
        assert pillar.score is not None

    def test_mixed_campaign_pillar_blends(self):
        """Mixed: all 5 signals active; pillar score uses blended weights."""
        pillar = compute_funnel_pillar(
            _campaign(
                platform_metrics=[_meta_platform(
                    impressions=500_000, clicks=5_000,
                    lp_views=4_500, on_platform_leads=200,
                )],
                media_plan=[
                    _arch_a_line(budget=8_000),
                    _arch_b_line(budget=2_000),
                ],
                ga4=_healthy_ga4(),
            )
        )
        active = {s.id for s in pillar.signals if s.guard_passed}
        assert active == {"F1", "F2", "F3", "F4", "F5"}
        assert pillar.score is not None

    def test_pillar_none_when_all_guard_fail(self):
        """Day 0 flight — all signals should guard-fail on min_days."""
        pillar = compute_funnel_pillar(
            _campaign(
                platform_metrics=[_meta_platform()],
                media_plan=[_arch_a_line()],
                ga4=_healthy_ga4(),
                elapsed=0,
            )
        )
        assert pillar.score is None
        assert pillar.status is None
        # All signals should be present in the pillar structure even
        # when guards fail — frontend needs a consistent shape.
        assert len(pillar.signals) == 5
