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
the diagnostic pillars) and moving FFS collection to ad-set grain.
See ``docs/diagnostics/phase-2-5-arch-mixing.md`` for the full design
note and revisit criteria.

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
    F4_ARCH_B_CLICK_TO_LEAD_BENCHMARK,
    F4_ARCH_B_CLICK_TO_LEAD_FLOOR,
    F4_BASE_COMPLETION_BENCHMARK,
    F4_COMPLETION_FLOOR,
    F4_PLATFORM_FORM_BOOST,
    F4_PLATFORM_FORM_CAP,
    F5_ACTIVATION_BENCHMARK,
    F5_ACTIVATION_FLOOR,
    FUNNEL_SIGNAL_WEIGHTS_ARCH_A,
    FUNNEL_SIGNAL_WEIGHTS_ARCH_B,
    MIN_PILLAR_COVERAGE,
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
        "CTR at {ctr} is below the {benchmark} benchmark.{worst_suffix} "
        "Review creative hook strength and headline clarity."
    ),
    StatusBand.ACTION: (
        "CTR at {ctr} is well below the {benchmark} benchmark.{worst_suffix} "
        "Creative isn't earning attention — refresh hook, headline, "
        "and targeting relevance."
    ),
}

# Worst-platform anchor config: only surface a per-platform callout when
# the worst platform's score sits this many points below the overall F1
# score, so we don't blame a minor platform for the overall read.
F1_WORST_PLATFORM_MIN_GAP = 10.0

# Minimum impressions for a platform to contribute its own CTR to the
# per-platform score roll-up. Below this, the platform's CTR is too noisy
# to score — its impressions still count toward the actual_ctr display,
# but it doesn't drive the pillar score.
F1_PER_PLATFORM_MIN_IMPRESSIONS = 1_000

F2_MESSAGES = {
    StatusBand.STRONG: (
        "Landing page load rate at {rate} — nearly every click reaches "
        "the page. No tracking or bounce issues.{overcounting_suffix}"
    ),
    StatusBand.WATCH: (
        "Landing page load rate at {rate} — {drop_pct} of clicks aren't "
        "registering a page view. Check page load speed and verify the "
        "pixel fires before the user can bounce.{overcounting_suffix}"
    ),
    StatusBand.ACTION: (
        "Landing page load rate at {rate} — {drop_pct} of clicks are "
        "dropping before the page loads. Likely page performance issue, "
        "tracking misconfiguration, or in-app browser incompatibility."
        "{overcounting_suffix}"
    ),
}

# F2 flags overcounting (lp_views > clicks) when the raw ratio exceeds
# this multiplier. Below this we treat it as measurement noise.
F2_OVERCOUNT_FLAG_THRESHOLD = 1.10

F3_MESSAGES = {
    StatusBand.STRONG: (
        "Form discovery at {discovery} — visitors are reaching the "
        "form. Page flow is carrying the user to action.{scroll_suffix}"
    ),
    StatusBand.WATCH: (
        "Form discovery at {discovery}. Some visitors aren't reaching "
        "the form. Consider moving the form higher on the page or "
        "reducing content above it.{scroll_suffix}"
    ),
    StatusBand.ACTION: (
        "Form discovery at {discovery}. Most visitors don't reach the "
        "form. Move the form above the fold or restructure the page "
        "hierarchy.{scroll_suffix}"
    ),
}

# If GA4 reports healthy session volume but zero scroll events, assume
# scroll tracking isn't wired up rather than penalising the campaign.
F3_SCROLL_ABSENT_FLAG = (
    " Scroll tracking not detected — scoring on form discovery alone."
)

F4_MESSAGES = {
    StatusBand.STRONG: (
        "Form completion rate at {rate} (friction-adjusted target "
        "{target}). Form friction is well-tuned for the audience."
        "{ffs_suffix}"
    ),
    StatusBand.WATCH: (
        "Form completion rate at {rate} vs {target} friction-adjusted "
        "target. Some form-starters are abandoning — review required "
        "fields and field order.{ffs_suffix}"
    ),
    StatusBand.ACTION: (
        "Form completion rate at {rate} — well below the {target} "
        "friction-adjusted target. Form friction is blocking "
        "conversions. Reduce field count, remove non-essential "
        "required fields, or consider an in-platform lead form."
        "{ffs_suffix}"
    ),
}

# Flag shown in F4 diagnostic when FFS data is missing for Arch A lines
# — the target is the generic base benchmark rather than a form-specific
# friction-adjusted number.
F4_NO_FFS_FLAG = (
    " Note: no Form Friction Score on file — target is a generic "
    "baseline. Add FFS inputs for a benchmark tuned to this form."
)

F4_ARCH_B_MESSAGES = {
    StatusBand.STRONG: (
        "Click→lead rate at {rate} (platform-form target {target}). "
        "In-platform form is converting well."
    ),
    StatusBand.WATCH: (
        "Click→lead rate at {rate} vs {target} target. Some people who "
        "tap the CTA aren't completing the form — review form length, "
        "required fields, and lead-capture value prop."
    ),
    StatusBand.ACTION: (
        "Click→lead rate at {rate} — well below the {target} target. "
        "Most people who tap the CTA aren't completing the form. "
        "Reduce required fields or revisit creative-to-form alignment."
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


# ── Signal F1: Click-Through Rate ──────────────────────────────────


def compute_f1_ctr(data: CampaignData) -> SignalResult:
    """F1: Click-Through Rate.

    Calibrated 2026-04-20:
      - Per-platform scoring with impression-weighted roll-up. Removes
        the spend-weighted-benchmark / impression-weighted-actual
        denominator mismatch that made campaigns with high-CPM platforms
        (LinkedIn, Google Ads) look systematically worse than they were.
      - Worst-platform anchor: when overall F1 is WATCH/ACTION and the
        worst platform scores ≥10pt below overall, the diagnostic names
        that platform so the user knows where the drop is coming from.

    Non-clickable placements (CTV, DOOH) are excluded from both the
    per-platform roll-up and the actual-CTR display.
    """
    passed, reason = guard_funnel(data)
    if not passed:
        return _guard_fail("F1", "Click-Through Rate", reason,
                           f"Insufficient data — {reason}.")

    passed, reason = check_min_clicks(data, threshold=30)
    if not passed:
        return _guard_fail("F1", "Click-Through Rate", reason,
                           "Insufficient clicks to compute a stable CTR.")

    # Walk the platforms. For each clickable platform with enough
    # impressions, compute its CTR, score against its own benchmark, and
    # retain it for the impression-weighted roll-up. Low-volume clickable
    # platforms still contribute to the actual_ctr display but not to the
    # roll-up score (their CTR is too noisy to score).
    clickable_impressions = 0
    clickable_clicks = 0
    non_clickable_spend = 0.0
    per_platform: dict[str, Any] = {}
    per_platform_scores: list[tuple[str, float, int, float, float]] = []
    # each tuple: (platform_id, score, impressions, platform_ctr, benchmark)

    weighted_benchmark_num = 0.0
    weighted_benchmark_den = 0

    for p in data.platform_metrics:
        bench = get_f1_benchmark(p.platform_id)
        if bench is None:
            non_clickable_spend += p.spend
            per_platform[p.platform_id] = {
                "non_clickable": True,
                "spend": round(p.spend, 2),
            }
            continue

        clickable_impressions += p.impressions
        clickable_clicks += p.clicks

        # Impression-weighted average of per-platform benchmarks, used for
        # diagnostic display only (so the user sees a single "vs X.XX%"
        # number that matches the scored comparison).
        weighted_benchmark_num += bench["benchmark"] * p.impressions
        weighted_benchmark_den += p.impressions

        platform_entry: dict[str, Any] = {
            "spend": round(p.spend, 2),
            "clicks": p.clicks,
            "impressions": p.impressions,
            "benchmark": bench["benchmark"],
            "floor": bench["floor"],
        }

        if p.impressions < F1_PER_PLATFORM_MIN_IMPRESSIONS:
            # Too few impressions to score — keep the platform in the
            # inputs dict for transparency but skip the score roll-up.
            platform_entry["ctr"] = (
                round(p.clicks / p.impressions, 5) if p.impressions > 0 else None
            )
            platform_entry["score"] = None
            platform_entry["skipped_reason"] = (
                f"under_{F1_PER_PLATFORM_MIN_IMPRESSIONS}_impressions"
            )
            per_platform[p.platform_id] = platform_entry
            continue

        p_ctr = p.clicks / p.impressions
        p_score = normalize_linear(p_ctr, bench["floor"], bench["benchmark"])
        platform_entry["ctr"] = round(p_ctr, 5)
        platform_entry["score"] = round(p_score, 1)
        per_platform[p.platform_id] = platform_entry
        per_platform_scores.append(
            (p.platform_id, p_score, p.impressions, p_ctr, bench["benchmark"])
        )

    if clickable_impressions <= 0:
        return _guard_fail(
            "F1", "Click-Through Rate", "no_clickable_impressions",
            "No impressions on clickable placements — F1 doesn't apply.",
        )

    if not per_platform_scores:
        return _guard_fail(
            "F1", "Click-Through Rate", "insufficient_per_platform_impressions",
            f"No clickable platform has {F1_PER_PLATFORM_MIN_IMPRESSIONS:,}+ "
            "impressions yet. CTR estimates are too noisy to score.",
        )

    # Impression-weighted roll-up across per-platform scores
    total_imps = sum(imps for _, _, imps, _, _ in per_platform_scores)
    score = sum(s * imps for _, s, imps, _, _ in per_platform_scores) / total_imps
    score = clamp(score, 0, 100)
    status = status_band(score)

    actual_ctr = clickable_clicks / clickable_impressions
    display_benchmark = (
        weighted_benchmark_num / weighted_benchmark_den
        if weighted_benchmark_den > 0
        else F1_DEFAULT_BENCHMARK["benchmark"]
    )

    # Worst-platform anchor (matches R1/A5 pattern)
    worst_suffix = ""
    worst_platform_info: dict[str, Any] | None = None
    if status in (StatusBand.WATCH, StatusBand.ACTION) and len(per_platform_scores) > 1:
        worst_platform, worst_score, _, worst_ctr, worst_bench = min(
            per_platform_scores, key=lambda row: row[1]
        )
        if (score - worst_score) >= F1_WORST_PLATFORM_MIN_GAP:
            worst_suffix = (
                f" {worst_platform}'s CTR ({format_pct(worst_ctr, decimals=2)} "
                f"vs {format_pct(worst_bench, decimals=2)} benchmark) is "
                "dragging the campaign down."
            )
            worst_platform_info = {
                "platform_id": worst_platform,
                "score": round(worst_score, 1),
                "ctr": round(worst_ctr, 5),
                "benchmark": round(worst_bench, 5),
            }

    template = F1_MESSAGES.get(status, F1_MESSAGES[StatusBand.WATCH])
    diagnostic = template.format(
        ctr=format_pct(actual_ctr, decimals=2),
        benchmark=format_pct(display_benchmark, decimals=2),
        worst_suffix=worst_suffix,
    )

    inputs: dict[str, Any] = {
        "actual_ctr": round(actual_ctr, 5),
        "clickable_clicks": clickable_clicks,
        "clickable_impressions": clickable_impressions,
        "benchmark": round(display_benchmark, 5),
        "non_clickable_spend": round(non_clickable_spend, 2),
        "platforms": per_platform,
        "scored_platform_count": len(per_platform_scores),
    }
    if worst_platform_info is not None:
        inputs["worst_platform"] = worst_platform_info

    return SignalResult(
        id="F1",
        name="Click-Through Rate",
        score=round(score, 1),
        status=status,
        raw_value=round(actual_ctr, 5),
        benchmark=round(display_benchmark, 5),
        floor=None,
        diagnostic=diagnostic,
        guard_passed=True,
        inputs=inputs,
    )


# ── Signal F2: Landing Page Load Rate ──────────────────────────────


def compute_f2_lp_load_rate(data: CampaignData, arch_mix: ArchMix) -> SignalResult:
    """F2: Landing Page Load Rate — landing_page_views / clicks.

    Calibrated 2026-04-20:
      - Per-platform: only platforms that actually report
        landing_page_views contribute. Platforms without LP-view
        reporting (historically StackAdapt, some DSPs) no longer count
        their clicks in the denominator, which used to make mixed-
        platform campaigns look worse than they were.
      - Denominator: prefer ``outbound_clicks`` when > 0, falling back to
        ``clicks``. Meta's ``clicks`` counts all clicks (likes, reactions,
        profile clicks, video play clicks) — only ``outbound_clicks`` are
        clicks that were ever meant to leave the platform, so ``clicks``
        systematically under-reports load rate. Platforms that don't
        populate ``outbound_clicks`` (DSPs, LinkedIn historically) keep
        using ``clicks``.
      - Overcounting transparency: when the raw ratio exceeds 1.10
        (landing-page events outnumbering clicks by >10%), the
        diagnostic flags a likely pixel/organic-traffic issue instead of
        silently capping at 1.0 and reporting STRONG.
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

    # Build the per-platform view: only platforms where LP views are
    # reported (>0) contribute to the rate. Platforms without LP-view
    # reporting are surfaced in inputs so the user can see coverage gaps.
    #
    # Denominator selection: prefer outbound_clicks when > 0, else fall
    # back to total clicks. Meta's "clicks" includes on-platform noise
    # (reactions, profile clicks, etc.) so using it as the denominator
    # makes load rate look catastrophically bad. Outbound_clicks is the
    # fair measure of "clicks that were meant to reach the LP".
    per_platform: dict[str, Any] = {}
    reporting_denominator = 0
    reporting_views = 0
    non_reporting_clicks = 0
    non_reporting_platforms: list[str] = []

    for p in data.platform_metrics:
        denom = p.outbound_clicks if p.outbound_clicks > 0 else p.clicks
        denom_source = (
            "outbound_clicks" if p.outbound_clicks > 0 else "clicks"
        )
        if p.landing_page_views > 0:
            reporting_denominator += denom
            reporting_views += p.landing_page_views
            per_platform[p.platform_id] = {
                "clicks": p.clicks,
                "outbound_clicks": p.outbound_clicks,
                "denominator": denom,
                "denominator_source": denom_source,
                "landing_page_views": p.landing_page_views,
                "rate": round(
                    p.landing_page_views / denom, 3
                ) if denom > 0 else None,
                "reporting": True,
            }
        elif p.clicks > 0:
            non_reporting_clicks += denom
            non_reporting_platforms.append(p.platform_id)
            per_platform[p.platform_id] = {
                "clicks": p.clicks,
                "outbound_clicks": p.outbound_clicks,
                "denominator": denom,
                "denominator_source": denom_source,
                "landing_page_views": 0,
                "rate": None,
                "reporting": False,
                "excluded_reason": "no_lp_view_reporting",
            }

    if reporting_denominator <= 0:
        return _guard_fail(
            "F2", "Landing Page Load Rate", "no_reporting_platforms",
            "No platform is reporting landing_page_views. Verify the LP "
            "pixel is firing and that each platform's conversions API "
            "is wired up.",
        )

    # Raw rate first (for overcounting detection), then clamped rate
    raw_rate = reporting_views / reporting_denominator
    overcounting = raw_rate > F2_OVERCOUNT_FLAG_THRESHOLD
    load_rate = min(raw_rate, 1.0)

    score = normalize_linear(load_rate, F2_FLOOR, F2_BENCHMARK)
    status = status_band(score)

    overcounting_suffix = ""
    if overcounting:
        overcounting_suffix = (
            f" Note: landing-page events exceed click count by "
            f"{(raw_rate - 1) * 100:.0f}% — likely organic traffic on "
            "the same URL or double-counting from a misconfigured pixel."
        )

    drop_pct = format_pct(max(0.0, 1.0 - load_rate))
    template = F2_MESSAGES.get(status, F2_MESSAGES[StatusBand.WATCH])
    diagnostic = template.format(
        rate=format_pct(load_rate),
        drop_pct=drop_pct,
        overcounting_suffix=overcounting_suffix,
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
            "raw_rate": round(raw_rate, 3),
            "overcounting_flagged": overcounting,
            "reporting_denominator": reporting_denominator,
            "reporting_landing_page_views": reporting_views,
            "non_reporting_clicks": non_reporting_clicks,
            "non_reporting_platforms": non_reporting_platforms,
            "platforms": per_platform,
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

    # Detect absent scroll tracking. If sessions cleared the GA4 guard
    # (healthy volume) but zero scroll events fired, we assume tracking
    # isn't wired up rather than penalising the campaign. Score on
    # discovery alone in that case and flag it in the diagnostic.
    scroll_tracking_present = scrolls > 0

    # Discovery component — form-position-adjusted target (above-fold
    # forms expect higher discovery; below-fold less). Floor is always
    # 0 — any discovery is directional signal.
    position = _dominant_form_position(arch_mix.arch_a_lines)
    discovery_target = F3_FORM_DISCOVERY_BY_POSITION[position]
    discovery_score = normalize_linear(
        discovery_rate, 0.0, discovery_target
    )

    if scroll_tracking_present:
        scroll_score = normalize_linear(
            scroll_rate, F3_SCROLL_FLOOR, F3_SCROLL_BENCHMARK
        )
        combined_score = (
            scroll_score * F3_SCROLL_WEIGHT
            + discovery_score * F3_DISCOVERY_WEIGHT
        )
        combined_rate = (
            scroll_rate * F3_SCROLL_WEIGHT
            + discovery_rate * F3_DISCOVERY_WEIGHT
        )
        effective_benchmark = round(
            F3_SCROLL_BENCHMARK * F3_SCROLL_WEIGHT
            + discovery_target * F3_DISCOVERY_WEIGHT,
            3,
        )
        effective_floor = round(F3_SCROLL_FLOOR * F3_SCROLL_WEIGHT, 3)
        scroll_suffix = ""
    else:
        # Drop scroll component — discovery alone drives the score.
        scroll_score = 0.0
        combined_score = discovery_score
        combined_rate = discovery_rate
        effective_benchmark = round(discovery_target, 3)
        effective_floor = 0.0
        scroll_suffix = F3_SCROLL_ABSENT_FLAG

    score = clamp(combined_score, 0.0, 100.0)

    status = status_band(score)
    template = F3_MESSAGES.get(status, F3_MESSAGES[StatusBand.WATCH])
    diagnostic = template.format(
        discovery=format_pct(discovery_rate),
        scroll_suffix=scroll_suffix,
    )

    return SignalResult(
        id="F3",
        name="Scroll & Form Discovery",
        score=round(score, 1),
        status=status,
        raw_value=round(combined_rate, 3),
        benchmark=effective_benchmark,
        floor=effective_floor,
        diagnostic=diagnostic,
        guard_passed=True,
        inputs={
            "scroll_rate": round(scroll_rate, 3),
            "discovery_rate": round(discovery_rate, 3),
            "scroll_score": round(scroll_score, 1),
            "discovery_score": round(discovery_score, 1),
            "scroll_tracking_present": scroll_tracking_present,
            "form_position": position,
            "discovery_target": discovery_target,
            "sessions": sessions,
            "scrolls": scrolls,
            "form_starts": form_starts,
            "weights": (
                {
                    "scroll": F3_SCROLL_WEIGHT,
                    "discovery": F3_DISCOVERY_WEIGHT,
                }
                if scroll_tracking_present
                else {"scroll": 0.0, "discovery": 1.0}
            ),
        },
    )


# ── Signal F4: Form Completion Rate ────────────────────────────────


def _arch_a_f4_target(
    arch_a_lines: list[MediaPlanLine],
) -> tuple[float, bool]:
    """Friction-adjusted completion target for landing-page forms.

    expected_completion = base * exp(-0.012 * avg_ffs)

    Uses spend-weighted FFS when multiple Arch A lines exist. Falls back
    to ``F4_BASE_COMPLETION_BENCHMARK`` if no FFS is available.

    Returns (target, ffs_available) so the caller can flag to the user
    when the fallback is in use.
    """
    total_budget = sum(l.planned_budget for l in arch_a_lines)
    if total_budget <= 0:
        ffs_vals = [l.ffs_score for l in arch_a_lines if l.ffs_score is not None]
        if not ffs_vals:
            return F4_BASE_COMPLETION_BENCHMARK, False
        avg_ffs = sum(ffs_vals) / len(ffs_vals)
        return F4_BASE_COMPLETION_BENCHMARK * math.exp(-0.012 * avg_ffs), True

    weighted_ffs = 0.0
    weighted_budget = 0.0
    for line in arch_a_lines:
        if line.ffs_score is None:
            continue
        weighted_ffs += line.ffs_score * line.planned_budget
        weighted_budget += line.planned_budget

    if weighted_budget <= 0:
        return F4_BASE_COMPLETION_BENCHMARK, False

    avg_ffs = weighted_ffs / weighted_budget
    return F4_BASE_COMPLETION_BENCHMARK * math.exp(-0.012 * avg_ffs), True


def _arch_b_f4_target() -> float:
    """Legacy in-platform-form *form-completion* benchmark.

    Retained for mixed-architecture blends where we still need a
    form_submits/form_starts-compatible number for the Arch B share
    pro-rata rollup. Not used as a standalone Arch B benchmark any
    more — Arch B campaigns are scored against
    ``F4_ARCH_B_CLICK_TO_LEAD_BENCHMARK`` directly.
    """
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
    # leads (e.g. Meta on_platform_leads). Scored against a click→lead
    # benchmark (NOT form-completion) because platforms don't expose a
    # form_start denominator.
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

        click_to_lead = min(
            safe_div(total_on_platform_leads, total_clicks, 0), 1.0
        )
        target = F4_ARCH_B_CLICK_TO_LEAD_BENCHMARK
        floor = F4_ARCH_B_CLICK_TO_LEAD_FLOOR

        score = normalize_linear(click_to_lead, floor, target)
        status = status_band(score)

        template = F4_ARCH_B_MESSAGES.get(
            status, F4_ARCH_B_MESSAGES[StatusBand.WATCH]
        )
        diagnostic = template.format(
            rate=format_pct(click_to_lead),
            target=format_pct(target),
        )

        return SignalResult(
            id="F4",
            name="Form Completion Rate",
            score=round(score, 1),
            status=status,
            raw_value=round(click_to_lead, 3),
            benchmark=round(target, 3),
            floor=floor,
            diagnostic=diagnostic,
            guard_passed=True,
            inputs={
                "measurement": "click_to_lead",
                "click_to_lead_rate": round(click_to_lead, 3),
                "click_to_lead_benchmark": round(target, 3),
                "click_to_lead_floor": floor,
                "clicks": total_clicks,
                "on_platform_leads": total_on_platform_leads,
                "arch_a_share": round(arch_mix.arch_a_share, 3),
                "arch_b_share": round(arch_mix.arch_b_share, 3),
            },
        )

    # Arch A (or mixed): use GA4 form_submits / form_starts as the true
    # completion fraction. FFS is optional — falls back to base benchmark
    # with a diagnostic flag so the user knows to add FFS data.
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
    target_a, ffs_available = _arch_a_f4_target(arch_mix.arch_a_lines)
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

    ffs_suffix = "" if ffs_available else F4_NO_FFS_FLAG
    template = F4_MESSAGES.get(status, F4_MESSAGES[StatusBand.WATCH])
    diagnostic = template.format(
        rate=format_pct(completion_rate),
        target=format_pct(target),
        ffs_suffix=ffs_suffix,
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
            "measurement": "form_submit_over_form_start",
            "completion_rate": round(completion_rate, 3),
            "friction_adjusted_target": round(target, 3),
            "ffs_available": ffs_available,
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

    F5 depends on the landing page firing GA4 key_events after the form
    submit. This isn't reliable for every campaign:

    * Pure Arch B (in-platform forms) rarely send users to an LP, so
      GA4 typically sees nothing and F5 would read a false ACTION.
      Guard-fail Arch B entirely.
    * Arch A campaigns still sometimes launch without GA4 key_events
      configured at all. When we see healthy conversions but zero
      key_events, we assume the tracking wasn't wired up and guard-fail
      with a message telling the user to configure it.
    """
    passed, reason = check_min_days(data.flight)
    if not passed:
        return _guard_fail("F5", "Post-Conversion Activation", reason,
                           f"Insufficient data — {reason}.")

    # Arch B has no reliable post-conversion signal — in-platform forms
    # don't route users through an LP that fires GA4 events. Drop the
    # signal cleanly rather than reading false ACTION.
    if arch_mix.is_arch_b_only:
        return _guard_fail(
            "F5", "Post-Conversion Activation", "arch_b_no_activation_signal",
            "In-platform forms don't produce post-conversion GA4 events. "
            "F5 doesn't apply to pure Arch B campaigns.",
        )

    # Count form_submits OR on_platform_leads as the denominator — whichever
    # the campaign is using.
    form_submits = data.ga4.form_submits
    on_platform_leads = sum(
        p.on_platform_leads for p in data.platform_metrics
    )
    # Prefer GA4 form_submits when present; fall back to on_platform_leads
    # for mixed campaigns where the Arch A portion hasn't submitted yet.
    denominator = form_submits if form_submits > 0 else on_platform_leads

    if denominator < 1:
        return _guard_fail(
            "F5", "Post-Conversion Activation", "no_conversions",
            "No form submits or in-platform leads yet — signal will "
            "activate once conversions start flowing.",
        )

    passed, reason = check_min_form_submits(data)
    # If we're relying on on_platform_leads (Arch B component of mixed),
    # the GA4 form_submit guard doesn't apply — only enforce when
    # form_submits is the source.
    if not passed and form_submits > 0:
        return _guard_fail(
            "F5", "Post-Conversion Activation", reason,
            "Not enough form submits to score activation yet.",
        )

    key_events = data.ga4.key_events

    # Detect missing key_event tracking. If we have healthy conversions
    # but zero GA4 key_events, assume tracking wasn't configured rather
    # than scoring a false ACTION. Same philosophy as R3/F3 missing-
    # scroll fix.
    if key_events == 0:
        return _guard_fail(
            "F5", "Post-Conversion Activation", "no_key_events_configured",
            "No GA4 key_events recorded alongside the conversions — "
            "post-conversion tracking appears to not be configured. "
            "Add a secondary-action key_event in GA4 (e.g. thank-you "
            "page view, account creation, calendar booking) to "
            "activate this signal.",
        )

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
    Attention / Resonance / Acquisition) — but only above the coverage
    floor (AI-040): if less than MIN_PILLAR_COVERAGE of the blended
    design weight reported, the pillar score is withheld.
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
        weight=0.57,  # Conversion pillar weight (Quality deferred; see benchmarks)
    )

    # Coverage-gated weighted average (AI-040). The blend only carries
    # signals with weight > 0, so structurally-absent signals (F2/F3 on a
    # pure Arch-B plan) get a 0.0 entry and never count against coverage.
    # Active signals missing from the blend are left OUT of the weight
    # table so apply_weighted_score raises KeyError — same strict
    # fail-loudly contract as before (and as Resonance).
    blended = _blend_signal_weights(arch_mix)
    weights = {
        s.id: blended.get(s.id, 0.0)
        for s in pillar.signals
        if s.id in blended or not (s.guard_passed and s.score is not None)
    }
    pillar.apply_weighted_score(weights, min_coverage=MIN_PILLAR_COVERAGE)

    return pillar
