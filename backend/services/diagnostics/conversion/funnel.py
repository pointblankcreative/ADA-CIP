"""Funnel pillar signals (F1–F5) for conversion campaigns.

"Does the message convert through the funnel?"

F1: Click-Through Rate           — are people clicking the ads?
F2: Landing Page Load Rate       — do clicks reach the LP? (Arch A only)
F3: Scroll / Form Discovery Rate — do visitors reach the form? (Arch A only)
F4: Form Completion Rate         — do form_starts become submissions?
F5: Post-Conversion Activation   — do submissions trigger follow-on events?

ARCHITECTURE DETECTION (line-level)
───────────────────────────────────
A single campaign can contain both landing-page-flow lines ("Arch A")
and in-platform-form lines ("Arch B" — Meta Lead Ads, LinkedIn Lead
Gen, TikTok Instant Forms, etc.). The arch is classified per media plan
line using ``ffs_inputs.is_platform_form`` (primary) and an objective-
keyword heuristic (fallback).

Pillar scoring blends the two architectures pro-rata by planned spend
share. When all lines are one architecture, the blend degenerates to
that architecture's weights.

KNOWN LIMITATION
────────────────
Architecture is determined at the *media plan line* level. If a single
line mixes LP-flow ad sets with in-platform-form ad sets, they'll all
score under whichever architecture the line is tagged with. Closing
that gap requires preserving ad-set grain for form metrics in
``engine.py`` (currently aggregated to platform-level before reaching
the diagnostic pillars). Tracked as a Phase 2.5 follow-up.

Data sources:
    fact_digital_daily    — spend, clicks, impressions, leads,
                            on_platform_leads, landing_page_views
    fact_ga4_daily        — sessions, scrolls, form_starts,
                            form_submits, key_events
    media_plan_lines      — planned_budget, ffs_inputs, objective

For Meta specifically, the ``leads`` vs ``on_platform_leads`` split on
fact_digital_daily is used to refine F4 even when line-level classification
is coarse.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any

from backend.services.diagnostics.models import (
    CampaignData,
    MediaPlanLine,
    PillarScore,
    SignalResult,
    StatusBand,
    status_band,
)
from backend.services.diagnostics.shared.benchmarks import (
    F1_DEFAULT_BENCHMARK,
    F2_BENCHMARK,
    F2_FLOOR,
    F3_DISCOVERY_WEIGHT,
    F3_FORM_DISCOVERY_BY_POSITION,
    F3_SCROLL_BENCHMARK,
    F3_SCROLL_FLOOR,
    F3_SCROLL_WEIGHT,
    F4_BASE_COMPLETION_BENCHMARK,
    F4_COMPLETION_FLOOR,
    F4_PLATFORM_FORM_BOOST,
    F4_PLATFORM_FORM_CAP,
    F5_ACTIVATION_BENCHMARK,
    F5_ACTIVATION_FLOOR,
    FUNNEL_SIGNAL_WEIGHTS_ARCH_A,
    FUNNEL_SIGNAL_WEIGHTS_ARCH_B,
    get_f1_benchmark,
)
from backend.services.diagnostics.shared.guards import (
    check_has_form_friction_data,
    check_has_landing_page_data,
    check_min_clicks,
    check_min_days,
    check_min_form_starts,
    check_min_form_submits,
    check_min_ga4_sessions,
    guard_funnel,
)
from backend.services.diagnostics.shared.normalization import (
    clamp,
    format_pct,
    normalize_linear,
    safe_div,
)


# ── Architecture classification helpers ────────────────────────────

# Objective keywords that indicate in-platform lead forms when FFS flag
# isn't set. Order matters — "instant form" should hit before "form".
_ARCH_B_KEYWORDS = (
    "instant form",
    "instant_form",
    "lead gen form",
    "lead_gen_form",
    "lead form",
    "lead_form",
    "in-platform",
    "in platform",
    "on-platform",
    "on platform",
    "meta lead",
    "linkedin lead gen",
    "tiktok instant",
)


def _classify_line_architecture(line: MediaPlanLine) -> str:
    """Classify a media plan line as 'arch_a' (landing page) or 'arch_b'
    (in-platform form).

    Priority:
        1. ffs_inputs.is_platform_form (explicit, dashboard-sourced)
        2. objective keyword match
        3. default to 'arch_a' (landing page is the common case)
    """
    if line.ffs_inputs and line.ffs_inputs.get("is_platform_form") is True:
        return "arch_b"

    objective = (line.objective or "").lower()
    if any(kw in objective for kw in _ARCH_B_KEYWORDS):
        return "arch_b"

    return "arch_a"


@dataclass
class ArchMix:
    """Line-level architecture classification for a campaign, summarised by
    planned-spend share."""

    arch_a_share: float      # 0.0 – 1.0 of planned spend on Arch A
    arch_b_share: float      # 0.0 – 1.0 of planned spend on Arch B
    arch_a_lines: list[MediaPlanLine]
    arch_b_lines: list[MediaPlanLine]

    @property
    def is_arch_a_only(self) -> bool:
        return self.arch_a_share > 0 and self.arch_b_share == 0

    @property
    def is_arch_b_only(self) -> bool:
        return self.arch_b_share > 0 and self.arch_a_share == 0

    @property
    def is_mixed(self) -> bool:
        return self.arch_a_share > 0 and self.arch_b_share > 0


def _compute_arch_mix(data: CampaignData) -> ArchMix:
    """Split media plan lines by architecture and compute spend shares."""
    arch_a_lines: list[MediaPlanLine] = []
    arch_b_lines: list[MediaPlanLine] = []
    arch_a_budget = 0.0
    arch_b_budget = 0.0

    for line in data.media_plan:
        arch = _classify_line_architecture(line)
        if arch == "arch_b":
            arch_b_lines.append(line)
            arch_b_budget += line.planned_budget
        else:
            arch_a_lines.append(line)
            arch_a_budget += line.planned_budget

    total = arch_a_budget + arch_b_budget
    if total <= 0:
        # No planned budget — default to pure Arch A so downstream scoring
        # still has a deterministic path (pillar weights will just apply).
        return ArchMix(
            arch_a_share=1.0 if arch_a_lines else 0.0,
            arch_b_share=1.0 if (arch_b_lines and not arch_a_lines) else 0.0,
            arch_a_lines=arch_a_lines,
            arch_b_lines=arch_b_lines,
        )

    return ArchMix(
        arch_a_share=arch_a_budget / total,
        arch_b_share=arch_b_budget / total,
        arch_a_lines=arch_a_lines,
        arch_b_lines=arch_b_lines,
    )


def _dominant_form_position(lines: list[MediaPlanLine]) -> str:
    """Determine the dominant form position for F3 discovery scoring.

    Reads ``ffs_inputs.below_fold_mobile`` from Arch A lines; if no FFS
    data is present, returns 'unknown'.
    """
    below = 0
    above_or_mid = 0
    for line in lines:
        if not line.ffs_inputs:
            continue
        if line.ffs_inputs.get("below_fold_mobile") is True:
            below += 1
        elif line.ffs_inputs.get("below_fold_mobile") is False:
            above_or_mid += 1

    if below == 0 and above_or_mid == 0:
        return "unknown"
    if below > above_or_mid:
        return "below_fold"
    # When position data says "not below fold" we assume mid_page — a safe
    # middle expectation. A future extension could add an explicit
    # "above_fold" flag in FFS inputs.
    return "mid_page"


# ── Diagnostic message templates ───────────────────────────────────

F1_MESSAGES = {
    StatusBand.STRONG: (
        "Click-through rate at {ctr} is beating benchmark "
        "({benchmark}). Creative is earning the click."
    ),
    StatusBand.WATCH: (
        "CTR at {ctr} is below the {benchmark} benchmark. "
        "Review creative hook strength and headline clarity."
    ),
    StatusBand.ACTION: (
        "CTR at {ctr} is well below the {benchmark} benchmark. "
        "Creative isn't earning attention — refresh hook, headline, "
        "and targeting relevance."
    ),
}

F2_MESSAGES = {
    StatusBand.STRONG: (
        "Landing page load rate at {rate} — nearly every click reaches "
        "the page. No tracking or bounce issues."
    ),
    StatusBand.WATCH: (
        "Landing page load rate at {rate} — {drop_pct} of clicks aren't "
        "registering a page view. Check page load speed and verify the "
        "pixel fires before the user can bounce."
    ),
    StatusBand.ACTION: (
        "Landing page load rate at {rate} — {drop_pct} of clicks are "
        "dropping before the page loads. Likely page performance issue, "
        "tracking misconfiguration, or in-app browser incompatibility."
    ),
}

F3_MESSAGES = {
    StatusBand.STRONG: (
        "Scroll & form discovery at {combined} — visitors are reaching "
        "the form. Page flow is carrying the user to action."
    ),
    StatusBand.WATCH: (
        "Scroll & form discovery at {combined} (scroll {scroll}). "
        "Some visitors aren't reaching the form. Consider moving the "
        "form higher on the page or reducing content above it."
    ),
    StatusBand.ACTION: (
        "Scroll & form discovery at {combined} (scroll {scroll}). "
        "Most visitors don't reach the form. Move the form above the "
        "fold or restructure the page hierarchy."
    ),
}

F4_MESSAGES = {
    StatusBand.STRONG: (
        "Form completion rate at {rate} (friction-adjusted target "
        "{target}). Form friction is well-tuned for the audience."
    ),
    StatusBand.WATCH: (
        "Form completion rate at {rate} vs {target} friction-adjusted "
        "target. Some form-starters are abandoning — review required "
        "fields and field order."
    ),
    StatusBand.ACTION: (
        "Form completion rate at {rate} — well below the {target} "
        "friction-adjusted target. Form friction is blocking "
        "conversions. Reduce field count, remove non-essential "
        "required fields, or consider an in-platform lead form."
    ),
}

F5_MESSAGES = {
    StatusBand.STRONG: (
        "Post-conversion activation at {rate} — leads are taking a "
        "meaningful second action. Good lead quality signal."
    ),
    StatusBand.WATCH: (
        "Activation at {rate} of conversions — some leads are engaging "
        "past form submit. Review post-submit experience and welcome "
        "email/SMS for second-touch opportunities."
    ),
    StatusBand.ACTION: (
        "Activation at {rate} — few leads take a second action after "
        "submitting. Lead quality may be thin; review creative-to-LP "
        "alignment and audience targeting."
    ),
}


# ── Internal helpers ───────────────────────────────────────────────


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


def _campaign_ctr_benchmark(data: CampaignData) -> tuple[float, float, dict[str, Any]]:
    """Compute a spend-weighted CTR benchmark across the campaign's platforms.

    Platforms with no CTR (CTV, DOOH) contribute their spend to a
    non-clickable bucket — their share is excluded from the benchmark
    average and from the actual-CTR denominator.

    Returns (benchmark, floor, inputs_dict).
    """
    clickable_spend = 0.0
    weighted_benchmark = 0.0
    weighted_floor = 0.0
    non_clickable_spend = 0.0
    per_platform: dict[str, Any] = {}

    for p in data.platform_metrics:
        bench = get_f1_benchmark(p.platform_id)
        if bench is None:
            non_clickable_spend += p.spend
            per_platform[p.platform_id] = {"non_clickable": True, "spend": p.spend}
            continue
        clickable_spend += p.spend
        weighted_benchmark += bench["benchmark"] * p.spend
        weighted_floor += bench["floor"] * p.spend
        per_platform[p.platform_id] = {
            "spend": p.spend,
            "clicks": p.clicks,
            "impressions": p.impressions,
            "benchmark": bench["benchmark"],
            "floor": bench["floor"],
        }

    if clickable_spend > 0:
        benchmark = weighted_benchmark / clickable_spend
        floor = weighted_floor / clickable_spend
    else:
        benchmark = F1_DEFAULT_BENCHMARK["benchmark"]
        floor = F1_DEFAULT_BENCHMARK["floor"]

    return benchmark, floor, {
        "non_clickable_spend": round(non_clickable_spend, 2),
        "clickable_spend": round(clickable_spend, 2),
        "per_platform": per_platform,
    }


# ── Signal F1: Click-Through Rate ──────────────────────────────────


def compute_f1_ctr(data: CampaignData) -> SignalResult:
    """F1: Click-Through Rate.

    Spend-weighted CTR benchmark across the campaign's clickable
    platforms. CTV and DOOH spend is excluded from both the benchmark
    and the actual-CTR denominator so those placements don't drag the
    signal either way.
    """
    passed, reason = guard_funnel(data)
    if not passed:
        return _guard_fail("F1", "Click-Through Rate", reason,
                           f"Insufficient data — {reason}.")

    passed, reason = check_min_clicks(data, threshold=30)
    if not passed:
        return _guard_fail("F1", "Click-Through Rate", reason,
                           "Insufficient clicks to compute a stable CTR.")

    # Compute actual CTR only over clickable placements
    clickable_impressions = 0
    clickable_clicks = 0
    for p in data.platform_metrics:
        if get_f1_benchmark(p.platform_id) is None:
            continue
        clickable_impressions += p.impressions
        clickable_clicks += p.clicks

    if clickable_impressions <= 0:
        return _guard_fail("F1", "Click-Through Rate", "no_clickable_impressions",
                           "No impressions on clickable placements — F1 doesn't apply.")

    actual_ctr = clickable_clicks / clickable_impressions
    benchmark, floor, bench_inputs = _campaign_ctr_benchmark(data)

    score = normalize_linear(actual_ctr, floor, benchmark)
    status = status_band(score)

    template = F1_MESSAGES.get(status, F1_MESSAGES[StatusBand.WATCH])
    diagnostic = template.format(
        ctr=format_pct(actual_ctr, decimals=2),
        benchmark=format_pct(benchmark, decimals=2),
    )

    return SignalResult(
        id="F1",
        name="Click-Through Rate",
        score=round(score, 1),
        status=status,
        raw_value=round(actual_ctr, 5),
        benchmark=round(benchmark, 5),
        floor=round(floor, 5),
        diagnostic=diagnostic,
        guard_passed=True,
        inputs={
            "actual_ctr": round(actual_ctr, 5),
            "clickable_clicks": clickable_clicks,
            "clickable_impressions": clickable_impressions,
            "benchmark": round(benchmark, 5),
            **bench_inputs,
        },
    )


# ── Signal F2: Landing Page Load Rate ──────────────────────────────


def compute_f2_lp_load_rate(data: CampaignData, arch_mix: ArchMix) -> SignalResult:
    """F2: Landing Page Load Rate — landing_page_views / clicks.

    Only applies to Arch A (landing page) traffic. Guard-fails for
    Arch-B-only campaigns since there's no landing page to score.
    """
    if arch_mix.is_arch_b_only:
        return _guard_fail(
            "F2", "Landing Page Load Rate", "arch_b_only",
            "All lines use in-platform forms — no landing page to score. "
            "F2 doesn't apply.",
        )

    passed, reason = guard_funnel(data)
    if not passed:
        return _guard_fail("F2", "Landing Page Load Rate", reason,
                           f"Insufficient data — {reason}.")

    passed, reason = check_has_landing_page_data(data)
    if not passed:
        return _guard_fail(
            "F2", "Landing Page Load Rate", reason,
            "No landing_page_view events reported. Verify the pixel is "
            "firing on the LP and that clicks are reaching the page.",
        )

    total_clicks = data.total_clicks
    total_lp_views = sum(p.landing_page_views for p in data.platform_metrics)

    if total_clicks <= 0:
        return _guard_fail("F2", "Landing Page Load Rate", "no_clicks",
                           "No clicks reported — cannot compute LP load rate.")

    # LP views can exceed clicks in edge cases (tracking overcounting,
    # organic visits landing on the same URL). Cap at 1.0 to avoid
    # spurious "beating benchmark" scores.
    load_rate = min(safe_div(total_lp_views, total_clicks, 0), 1.0)

    score = normalize_linear(load_rate, F2_FLOOR, F2_BENCHMARK)
    status = status_band(score)

    drop_pct = format_pct(max(0.0, 1.0 - load_rate))
    template = F2_MESSAGES.get(status, F2_MESSAGES[StatusBand.WATCH])
    diagnostic = template.format(
        rate=format_pct(load_rate),
        drop_pct=drop_pct,
    )

    return SignalResult(
        id="F2",
        name="Landing Page Load Rate",
        score=round(score, 1),
        status=status,
        raw_value=round(load_rate, 3),
        benchmark=F2_BENCHMARK,
        floor=F2_FLOOR,
        diagnostic=diagnostic,
        guard_passed=True,
        inputs={
            "load_rate": round(load_rate, 3),
            "total_clicks": total_clicks,
            "total_landing_page_views": total_lp_views,
            "arch_a_share": round(arch_mix.arch_a_share, 3),
        },
    )


# ── Signal F3: Scroll / Form Discovery Rate ────────────────────────


def compute_f3_scroll_discovery(
    data: CampaignData, arch_mix: ArchMix
) -> SignalResult:
    """F3: Scroll & Form Discovery.

    Composite of GA4 scroll_rate and form-position-adjusted discovery.
    Only applies to Arch A (landing page) traffic.
    """
    if arch_mix.is_arch_b_only:
        return _guard_fail(
            "F3", "Scroll & Form Discovery", "arch_b_only",
            "All lines use in-platform forms — no landing page scroll "
            "to measure. F3 doesn't apply.",
        )

    passed, reason = check_min_days(data.flight)
    if not passed:
        return _guard_fail("F3", "Scroll & Form Discovery", reason,
                           f"Insufficient data — {reason}.")

    passed, reason = check_min_ga4_sessions(data)
    if not passed:
        return _guard_fail(
            "F3", "Scroll & Form Discovery", reason,
            "Not enough GA4 sessions to score scroll & form discovery. "
            "If GA4 URLs aren't configured for this project, check the "
            "project_ga4_urls mapping table.",
        )

    sessions = data.ga4.sessions
    scrolls = data.ga4.scrolls
    form_starts = data.ga4.form_starts

    scroll_rate = min(safe_div(scrolls, sessions, 0), 1.0)
    discovery_rate = min(safe_div(form_starts, sessions, 0), 1.0)

    # Score scroll component using the standard scroll benchmark.
    scroll_score = normalize_linear(
        scroll_rate, F3_SCROLL_FLOOR, F3_SCROLL_BENCHMARK
    )

    # Score discovery component against the form-position-adjusted target
    # (above-fold forms expect higher discovery; below-fold forms expect
    # less). Floor is always 0 — any discovery is directional signal.
    position = _dominant_form_position(arch_mix.arch_a_lines)
    discovery_target = F3_FORM_DISCOVERY_BY_POSITION[position]
    discovery_score = normalize_linear(
        discovery_rate, 0.0, discovery_target
    )

    # Weighted composite
    combined_score = (
        scroll_score * F3_SCROLL_WEIGHT
        + discovery_score * F3_DISCOVERY_WEIGHT
    )
    score = clamp(combined_score, 0.0, 100.0)

    # Combined metric used for diagnostic messaging — raw rate blend.
    combined_rate = (
        scroll_rate * F3_SCROLL_WEIGHT
        + discovery_rate * F3_DISCOVERY_WEIGHT
    )

    status = status_band(score)
    template = F3_MESSAGES.get(status, F3_MESSAGES[StatusBand.WATCH])
    diagnostic = template.format(
        combined=format_pct(combined_rate),
        scroll=format_pct(scroll_rate),
    )

    return SignalResult(
        id="F3",
        name="Scroll & Form Discovery",
        score=round(score, 1),
        status=status,
        raw_value=round(combined_rate, 3),
        benchmark=round(
            F3_SCROLL_BENCHMARK * F3_SCROLL_WEIGHT
            + discovery_target * F3_DISCOVERY_WEIGHT,
            3,
        ),
        floor=round(F3_SCROLL_FLOOR * F3_SCROLL_WEIGHT, 3),
        diagnostic=diagnostic,
        guard_passed=True,
        inputs={
            "scroll_rate": round(scroll_rate, 3),
            "discovery_rate": round(discovery_rate, 3),
            "scroll_score": round(scroll_score, 1),
            "discovery_score": round(discovery_score, 1),
            "form_position": position,
            "discovery_target": discovery_target,
            "sessions": sessions,
            "scrolls": scrolls,
            "form_starts": form_starts,
            "weights": {
                "scroll": F3_SCROLL_WEIGHT,
                "discovery": F3_DISCOVERY_WEIGHT,
            },
        },
    )


# ── Signal F4: Form Completion Rate ────────────────────────────────


def _arch_a_f4_target(arch_a_lines: list[MediaPlanLine]) -> float:
    """Friction-adjusted completion target for landing-page forms.

    expected_completion = base * exp(-0.012 * avg_ffs)

    Uses spend-weighted FFS when multiple Arch A lines exist. Falls back
    to ``F4_BASE_COMPLETION_BENCHMARK`` if no FFS is available.
    """
    total_budget = sum(l.planned_budget for l in arch_a_lines)
    if total_budget <= 0:
        ffs_vals = [l.ffs_score for l in arch_a_lines if l.ffs_score is not None]
        if not ffs_vals:
            return F4_BASE_COMPLETION_BENCHMARK
        avg_ffs = sum(ffs_vals) / len(ffs_vals)
        return F4_BASE_COMPLETION_BENCHMARK * math.exp(-0.012 * avg_ffs)

    weighted_ffs = 0.0
    weighted_budget = 0.0
    for line in arch_a_lines:
        if line.ffs_score is None:
            continue
        weighted_ffs += line.ffs_score * line.planned_budget
        weighted_budget += line.planned_budget

    if weighted_budget <= 0:
        return F4_BASE_COMPLETION_BENCHMARK

    avg_ffs = weighted_ffs / weighted_budget
    return F4_BASE_COMPLETION_BENCHMARK * math.exp(-0.012 * avg_ffs)


def _arch_b_f4_target() -> float:
    """In-platform forms benchmark — higher base because forms are
    pre-filled from the user's platform profile."""
    return min(
        F4_BASE_COMPLETION_BENCHMARK * F4_PLATFORM_FORM_BOOST,
        F4_PLATFORM_FORM_CAP,
    )


def compute_f4_form_completion(
    data: CampaignData, arch_mix: ArchMix
) -> SignalResult:
    """F4: Form Completion Rate.

    form_submits / form_starts, scored against a friction-adjusted target.
    The target blends Arch A (FFS-adjusted) and Arch B (platform-form
    boosted) benchmarks pro-rata by spend share.

    When Meta's ``on_platform_leads`` column is populated we use it as a
    secondary cross-check — if Meta reports lots of in-platform leads
    but the campaign is tagged as Arch A, the completion-rate blend
    shifts slightly toward Arch B to reflect reality.
    """
    passed, reason = guard_funnel(data)
    if not passed:
        return _guard_fail("F4", "Form Completion Rate", reason,
                           f"Insufficient data — {reason}.")

    # Arch B campaigns don't use GA4 form events — they use platform
    # leads (e.g. Meta on_platform_leads). Handle them separately.
    if arch_mix.is_arch_b_only:
        total_on_platform_leads = sum(
            p.on_platform_leads for p in data.platform_metrics
        )
        total_clicks = data.total_clicks
        if total_clicks <= 0:
            return _guard_fail(
                "F4", "Form Completion Rate", "no_clicks",
                "No clicks reported for Arch B campaign.",
            )
        if total_on_platform_leads < 1:
            return _guard_fail(
                "F4", "Form Completion Rate", "no_in_platform_leads",
                "No in-platform lead form fills reported yet — signal "
                "will activate once leads start flowing.",
            )
        # For Arch B, "form completion" proxied as leads per click. This
        # undercounts true completion rate (the true denominator is
        # people who *opened* the form, which Meta doesn't expose) but
        # gives a directional signal against the platform-form benchmark.
        completion_rate = min(
            safe_div(total_on_platform_leads, total_clicks, 0), 1.0
        )
        target = _arch_b_f4_target()
    else:
        # FFS data is optional for Arch A — _arch_a_f4_target() falls back
        # to the base benchmark when ffs_score is missing. We intentionally
        # do *not* guard-fail on missing FFS here.
        passed, reason = check_min_form_starts(data)
        if not passed:
            return _guard_fail(
                "F4", "Form Completion Rate", reason,
                "Not enough form_starts events yet — signal will "
                "activate once form engagement builds.",
            )

        form_starts = data.ga4.form_starts
        form_submits = data.ga4.form_submits
        completion_rate = min(safe_div(form_submits, form_starts, 0), 1.0)

        # Target: pro-rata blend of Arch A and Arch B benchmarks.
        target_a = _arch_a_f4_target(arch_mix.arch_a_lines)
        target_b = _arch_b_f4_target()
        target = (
            target_a * arch_mix.arch_a_share
            + target_b * arch_mix.arch_b_share
        )

    score = normalize_linear(
        completion_rate,
        F4_COMPLETION_FLOOR,
        target,
    )
    status = status_band(score)

    template = F4_MESSAGES.get(status, F4_MESSAGES[StatusBand.WATCH])
    diagnostic = template.format(
        rate=format_pct(completion_rate),
        target=format_pct(target),
    )

    return SignalResult(
        id="F4",
        name="Form Completion Rate",
        score=round(score, 1),
        status=status,
        raw_value=round(completion_rate, 3),
        benchmark=round(target, 3),
        floor=F4_COMPLETION_FLOOR,
        diagnostic=diagnostic,
        guard_passed=True,
        inputs={
            "completion_rate": round(completion_rate, 3),
            "friction_adjusted_target": round(target, 3),
            "form_starts": data.ga4.form_starts,
            "form_submits": data.ga4.form_submits,
            "on_platform_leads": sum(
                p.on_platform_leads for p in data.platform_metrics
            ),
            "arch_a_share": round(arch_mix.arch_a_share, 3),
            "arch_b_share": round(arch_mix.arch_b_share, 3),
        },
    )


# ── Signal F5: Post-Conversion Activation ──────────────────────────


def compute_f5_activation(data: CampaignData, arch_mix: ArchMix) -> SignalResult:
    """F5: Post-Conversion Activation.

    Fraction of conversions that trigger a GA4 key_event beyond the
    form submit itself. Proxies lead quality — leads who take a second
    action are more likely to be real, engaged, and to convert further
    down the funnel.
    """
    passed, reason = check_min_days(data.flight)
    if not passed:
        return _guard_fail("F5", "Post-Conversion Activation", reason,
                           f"Insufficient data — {reason}.")

    # Count form_submits OR on_platform_leads as the denominator — whichever
    # the campaign is using.
    form_submits = data.ga4.form_submits
    on_platform_leads = sum(
        p.on_platform_leads for p in data.platform_metrics
    )
    # Prefer GA4 form_submits when present; fall back to on_platform_leads
    # for Arch B campaigns.
    denominator = form_submits if form_submits > 0 else on_platform_leads

    if denominator < 1:
        return _guard_fail(
            "F5", "Post-Conversion Activation", "no_conversions",
            "No form submits or in-platform leads yet — signal will "
            "activate once conversions start flowing.",
        )

    passed, reason = check_min_form_submits(data)
    # If we're relying on on_platform_leads (Arch B), the GA4 form_submit
    # guard doesn't apply — only enforce it when form_submits is the source.
    if not passed and form_submits > 0:
        return _guard_fail(
            "F5", "Post-Conversion Activation", reason,
            "Not enough form submits to score activation yet.",
        )

    key_events = data.ga4.key_events

    # Subtract form_submits from key_events to avoid double-counting: many
    # GA4 configs mark form_submit itself as a key_event.
    activation_events = max(0, key_events - form_submits)
    activation_rate = min(safe_div(activation_events, denominator, 0), 1.0)

    score = normalize_linear(
        activation_rate,
        F5_ACTIVATION_FLOOR,
        F5_ACTIVATION_BENCHMARK,
    )
    status = status_band(score)

    template = F5_MESSAGES.get(status, F5_MESSAGES[StatusBand.WATCH])
    diagnostic = template.format(rate=format_pct(activation_rate))

    return SignalResult(
        id="F5",
        name="Post-Conversion Activation",
        score=round(score, 1),
        status=status,
        raw_value=round(activation_rate, 3),
        benchmark=F5_ACTIVATION_BENCHMARK,
        floor=F5_ACTIVATION_FLOOR,
        diagnostic=diagnostic,
        guard_passed=True,
        inputs={
            "activation_rate": round(activation_rate, 3),
            "activation_events": activation_events,
            "key_events": key_events,
            "form_submits": form_submits,
            "on_platform_leads": on_platform_leads,
            "denominator": denominator,
        },
    )


# ── Pillar assembly ────────────────────────────────────────────────


def _blend_signal_weights(arch_mix: ArchMix) -> dict[str, float]:
    """Blend Arch A and Arch B signal weights pro-rata by spend share.

    Pure Arch A → FUNNEL_SIGNAL_WEIGHTS_ARCH_A
    Pure Arch B → FUNNEL_SIGNAL_WEIGHTS_ARCH_B
    Mixed       → spend-weighted blend (signals missing in Arch B get
                  only their Arch A share, which is correct — F2 and F3
                  only apply to LP-flow traffic).
    """
    share_a = arch_mix.arch_a_share
    share_b = arch_mix.arch_b_share
    total = share_a + share_b
    if total <= 0:
        # Degenerate: no spend classified. Default to Arch A (most common).
        return dict(FUNNEL_SIGNAL_WEIGHTS_ARCH_A)

    blended: dict[str, float] = {}
    for sig_id in ("F1", "F2", "F3", "F4", "F5"):
        w_a = FUNNEL_SIGNAL_WEIGHTS_ARCH_A.get(sig_id, 0.0)
        w_b = FUNNEL_SIGNAL_WEIGHTS_ARCH_B.get(sig_id, 0.0)
        w = (w_a * share_a + w_b * share_b) / total
        if w > 0:
            blended[sig_id] = w

    return blended


def compute_funnel_pillar(data: CampaignData) -> PillarScore:
    """Compute all Funnel signals and assemble the pillar score.

    Per-line architecture classification + pro-rata weight blend:
        Arch A (LP flow):     F1 + F2 + F3 + F4 + F5
        Arch B (in-platform): F1 + F4 + F5 (F2/F3 guard-fail)
        Mixed:                 all 5 signals, blended weights

    Guard-failed signals are excluded and their weight redistributes
    pro rata across active signals (same pattern as Distribution /
    Attention / Resonance / Acquisition).
    """
    arch_mix = _compute_arch_mix(data)

    f1 = compute_f1_ctr(data)
    f2 = compute_f2_lp_load_rate(data, arch_mix)
    f3 = compute_f3_scroll_discovery(data, arch_mix)
    f4 = compute_f4_form_completion(data, arch_mix)
    f5 = compute_f5_activation(data, arch_mix)

    pillar = PillarScore(
        name="funnel",
        signals=[f1, f2, f3, f4, f5],
        weight=0.40,  # Conversion pillar weight (from benchmarks)
    )

    active = [s for s in pillar.signals if s.guard_passed and s.score is not None]
    if active:
        weights = _blend_signal_weights(arch_mix)
        # Fail loudly if a signal ID isn't in the blended weights — same
        # defensive pattern as Resonance.
        missing = [s.id for s in active if s.id not in weights]
        if missing:
            raise KeyError(
                f"Funnel blended weights missing entries for: {missing}. "
                f"Check FUNNEL_SIGNAL_WEIGHTS_ARCH_A / _ARCH_B in "
                f"shared.benchmarks."
            )
        weighted_sum = sum(s.score * weights[s.id] for s in active)
        total_weight = sum(weights[s.id] for s in active)
        pillar.score = round(weighted_sum / total_weight, 1) if total_weight > 0 else None
        pillar.status = status_band(pillar.score) if pillar.score is not None else None
    else:
        pillar.score = None
        pillar.status = None

    return pillar
