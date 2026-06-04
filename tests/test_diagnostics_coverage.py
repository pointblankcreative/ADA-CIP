"""Coverage-floor tests for the diagnostic engine (AI-040).

The engine previously had two silent weight-renormalization layers with
no coverage floor: a pillar would score off a single guard-passed signal
(denominator shrank to that signal's weight), and the health rollup
silently dropped unscored pillars. On project 26018 this rendered
"100 STRONG" off F1 alone — 15% of funnel design weight, 8.55% of total
conversion weight.

These tests cover:
    1. The 26018 regression: single-signal funnel is INSUFFICIENT DATA
    2. Funnel scores at/above the coverage floor with metadata attached
    3. Pure Arch-B coverage denominator excludes structurally-absent F2/F3
    4. Acquisition with everything guard-failed → coverage 0.0
    5. Health withheld when weighted coverage < MIN_HEALTH_COVERAGE
    6. Health scored (with coverage exposed) when funnel fully reports
    7. apply_weighted_score strict mode raises KeyError on missing weights
    8. to_bq_row carries coverage keys inside the pillars JSON
Plus: _row_to_diagnostic legacy-row handling (no coverage keys → None).
"""

from __future__ import annotations

from datetime import date, timedelta

import pytest

from backend.routers.diagnostics import _row_to_diagnostic
from backend.services.diagnostics.conversion.acquisition import (
    compute_acquisition_pillar,
)
from backend.services.diagnostics.conversion.funnel import (
    compute_funnel_pillar,
)
from backend.services.diagnostics.models import (
    AudienceType,
    CampaignData,
    CampaignType,
    DiagnosticOutput,
    FlightContext,
    GA4Metrics,
    MediaPlanLine,
    PillarScore,
    PlatformMetrics,
    SignalResult,
    StatusBand,
)
from backend.services.diagnostics.shared.benchmarks import (
    MIN_HEALTH_COVERAGE,
    MIN_PILLAR_COVERAGE,
)


# ── Fixtures (mirroring tests/test_diagnostics_funnel.py patterns) ──


def _flight(elapsed: int = 24, total: int = 30) -> FlightContext:
    start = date(2026, 5, 6)
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
    elapsed: int = 24,
    total: int = 30,
) -> CampaignData:
    return CampaignData(
        project_code="26018",
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
) -> MediaPlanLine:
    """Landing-page media plan line (Arch A)."""
    return MediaPlanLine(
        line_id=line_id,
        platform_id="facebook",
        planned_budget=budget,
        planned_impressions=1_000_000,
        audience_type=AudienceType.MEMBER_LIST,
        ffs_score=ffs,
        ffs_inputs={"is_platform_form": False, "below_fold_mobile": False},
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


def _signal(
    sid: str,
    score: float | None,
    guard_passed: bool = True,
) -> SignalResult:
    return SignalResult(
        id=sid,
        name=f"{sid} Signal",
        score=score,
        status=StatusBand.STRONG if score is not None and score >= 70 else None,
        guard_passed=guard_passed,
    )


def _output(pillars: list[PillarScore]) -> DiagnosticOutput:
    return DiagnosticOutput(
        project_code="26018",
        campaign_type=CampaignType.CONVERSION,
        evaluation_date=date(2026, 5, 29),
        flight_day=24,
        flight_total_days=30,
        pillars=pillars,
    )


# ── 1. The 26018 regression case ────────────────────────────────────


def test_single_signal_pillar_is_insufficient():
    """Funnel with only F1 scoreable (Arch A; no GA4, no LP views, no
    form starts) — the exact 26018 shape that rendered "100 STRONG".

    AI-040: F1 carries 0.15 of Arch-A design weight → coverage 0.15 <
    MIN_PILLAR_COVERAGE → score/status withheld; coverage metadata
    populated for the INSUFFICIENT DATA card.
    """
    pillar = compute_funnel_pillar(
        _campaign(
            platform_metrics=[
                PlatformMetrics(
                    platform_id="facebook",
                    spend=7_000,
                    impressions=750_000,
                    clicks=12_375,   # CTR 1.65% vs 0.90% benchmark → F1 STRONG
                    landing_page_views=0,   # F2 guard-fails
                )
            ],
            media_plan=[_arch_a_line()],
            ga4=GA4Metrics(),   # F3/F4/F5 guard-fail (no sessions/forms)
        )
    )

    active = {s.id for s in pillar.signals if s.guard_passed}
    assert active == {"F1"}
    f1 = next(s for s in pillar.signals if s.id == "F1")
    assert f1.score is not None and f1.score >= 70  # F1 itself is healthy

    # The pillar must NOT inherit F1's score as its own.
    assert pillar.score is None
    assert pillar.status is None
    assert pillar.coverage == 0.15
    assert pillar.signals_active == 1
    assert pillar.signals_total == 5


# ── 2. Scores at/above the coverage floor ───────────────────────────


def test_pillar_scores_at_coverage_floor():
    """F1 + F2 + F4 active (Arch A: 0.15 + 0.15 + 0.35 = 0.65 ≥ 0.5):
    pillar scores the weighted average over active weights; coverage
    metadata is populated alongside the score."""
    pillar = compute_funnel_pillar(
        _campaign(
            platform_metrics=[
                PlatformMetrics(
                    platform_id="facebook",
                    spend=7_000,
                    impressions=750_000,
                    clicks=7_500,
                    landing_page_views=6_000,   # F2 healthy
                )
            ],
            media_plan=[_arch_a_line()],
            # form_starts ≥ 10 → F4 active; sessions < 20 → F3 guard-fails;
            # no key_events → F5 guard-fails.
            ga4=GA4Metrics(sessions=10, form_starts=100, form_submits=60),
        )
    )

    active = {s.id for s in pillar.signals if s.guard_passed}
    assert active == {"F1", "F2", "F4"}
    assert pillar.coverage == 0.65
    assert pillar.coverage >= MIN_PILLAR_COVERAGE
    assert pillar.score is not None
    assert pillar.status is not None
    assert pillar.signals_active == 3
    assert pillar.signals_total == 5

    # Score must equal the weighted average over ACTIVE weights.
    weights = {"F1": 0.15, "F2": 0.15, "F4": 0.35}
    by_id = {s.id: s for s in pillar.signals}
    expected = round(
        sum(by_id[i].score * w for i, w in weights.items()) / 0.65, 1
    )
    assert pillar.score == expected


# ── 3. Pure Arch-B denominator ──────────────────────────────────────


def test_pure_arch_b_coverage_denominator():
    """Pure Arch-B plan: F2/F3 are structurally absent from the blend so
    they never count against coverage. With F1 + F4 active (0.30 + 0.55
    of the 1.00 Arch-B design weight) coverage is 0.85 and the pillar
    scores; signals_total counts only the 3 applicable signals."""
    pillar = compute_funnel_pillar(
        _campaign(
            platform_metrics=[
                PlatformMetrics(
                    platform_id="facebook",
                    spend=5_000,
                    impressions=500_000,
                    clicks=5_000,
                    on_platform_leads=400,
                )
            ],
            media_plan=[_arch_b_line()],
            ga4=GA4Metrics(),   # F5 guard-fails (no key events)
        )
    )

    active = {s.id for s in pillar.signals if s.guard_passed}
    assert active == {"F1", "F4"}
    assert pillar.coverage == 0.85
    assert pillar.signals_total == 3   # F2/F3 excluded from denominator
    assert pillar.signals_active == 2
    assert pillar.score is not None


# ── 4. Acquisition all-guard-fail ───────────────────────────────────


def test_acquisition_all_guard_fail_zero_coverage():
    """No FFS (no target CPA), no conversions: every acquisition signal
    guard-fails → coverage 0.0, score None (extends the existing
    test_all_guard_fail_gives_none_score with coverage assertions)."""
    pillar = compute_acquisition_pillar(
        _campaign(
            platform_metrics=[
                PlatformMetrics(
                    platform_id="facebook",
                    spend=7_000,
                    impressions=750_000,
                    clicks=7_500,
                    conversions=0,
                )
            ],
            media_plan=[_arch_a_line(ffs=None)],   # FFS wiped → no target CPA
        )
    )

    assert pillar.score is None
    assert pillar.status is None
    assert pillar.coverage == 0.0
    assert pillar.signals_active == 0
    assert pillar.signals_total == 3


# ── 5 + 6. Health-level gating ──────────────────────────────────────


def test_health_insufficient_when_one_pillar_dead():
    """Conversion output, acquisition coverage 0 / funnel coverage 0.15
    (the 26018 2026-05-29 shape): health coverage = 0.57 × 0.15 ≈ 0.086
    < MIN_HEALTH_COVERAGE → health score/status withheld. The unscored
    acquisition pillar stays IN the denominator instead of silently
    renormalizing away."""
    acquisition = PillarScore(
        name="acquisition", weight=0.43,
        score=None, status=None,
        coverage=0.0, signals_active=0, signals_total=3,
        signals=[_signal("C1", None, guard_passed=False)],
    )
    funnel = PillarScore(
        name="funnel", weight=0.57,
        score=None, status=None,
        coverage=0.15, signals_active=1, signals_total=5,
        signals=[_signal("F1", 100.0)],
    )
    output = _output([acquisition, funnel])
    output.compute_health_score()

    assert output.health_coverage == round(0.57 * 0.15 / 1.0, 3)
    assert output.health_coverage < MIN_HEALTH_COVERAGE
    assert output.health_score is None
    assert output.health_status is None


def test_health_scores_with_full_funnel_dead_acquisition():
    """Funnel coverage 1.0 / acquisition 0: health coverage 0.57 ≥ 0.5 →
    health computed from funnel alone, but coverage stays exposed so
    the UI can caption "n of m signals reporting"."""
    acquisition = PillarScore(
        name="acquisition", weight=0.43,
        score=None, status=None,
        coverage=0.0, signals_active=0, signals_total=3,
        signals=[_signal("C1", None, guard_passed=False)],
    )
    funnel = PillarScore(
        name="funnel", weight=0.57,
        score=72.0, status=StatusBand.STRONG,
        coverage=1.0, signals_active=5, signals_total=5,
        signals=[_signal("F1", 72.0)],
    )
    output = _output([acquisition, funnel])
    output.compute_health_score()

    assert output.health_coverage == 0.57
    assert output.health_coverage >= MIN_HEALTH_COVERAGE
    assert output.health_score == 72.0   # funnel alone (renormalized)
    assert output.health_status == StatusBand.STRONG


# ── 7. Strict mode raises ───────────────────────────────────────────


def test_apply_weighted_score_strict_raises():
    """Strict mode (no default_weight) raises KeyError when a signal id
    is missing from the weight table — preserves the Resonance/Funnel
    fail-loudly contract."""
    pillar = PillarScore(
        name="resonance",
        signals=[_signal("R1", 80.0), _signal("R9", 50.0)],
    )
    with pytest.raises(KeyError, match="R9"):
        pillar.apply_weighted_score(
            {"R1": 0.45},
            min_coverage=MIN_PILLAR_COVERAGE,
        )


def test_apply_weighted_score_default_weight_is_lenient():
    """With default_weight, unknown signal ids fall back instead of
    raising (Distribution/Attention/Acquisition pattern)."""
    pillar = PillarScore(
        name="distribution",
        signals=[_signal("D1", 80.0), _signal("D9", 60.0)],
    )
    pillar.apply_weighted_score(
        {"D1": 0.35},
        min_coverage=MIN_PILLAR_COVERAGE,
        default_weight=0.25,
    )
    assert pillar.coverage == 1.0
    assert pillar.score == round((80.0 * 0.35 + 60.0 * 0.25) / 0.60, 1)


# ── 8. BQ serialization round-trip ──────────────────────────────────


def test_to_bq_row_carries_coverage():
    """to_bq_row()["pillars"][name] carries weight / coverage /
    signals_active / signals_total — additive keys inside the existing
    pillars JSON column (no BQ schema change)."""
    funnel = PillarScore(
        name="funnel", weight=0.57,
        score=None, status=None,
        coverage=0.15, signals_active=1, signals_total=5,
        signals=[_signal("F1", 100.0)],
    )
    output = _output([funnel])
    row = output.to_bq_row()

    p = row["pillars"]["funnel"]
    assert p["score"] is None
    assert p["status"] is None
    assert p["weight"] == 0.57
    assert p["coverage"] == 0.15
    assert p["signals_active"] == 1
    assert p["signals_total"] == 5


# ── API layer: legacy rows + derived health coverage ────────────────


def test_row_to_diagnostic_legacy_row_has_null_coverage():
    """Old snapshots (pre-coverage pillars JSON) must not crash and must
    return health_coverage None so the frontend renders them exactly as
    before — no INSUFFICIENT DATA retro-blanking."""
    row = {
        "id": "legacy-1",
        "project_code": "26018",
        "campaign_type": "conversion",
        "evaluation_date": date(2026, 5, 19),
        "flight_day": 14,
        "flight_total_days": 30,
        "health_score": 100.0,
        "health_status": "STRONG",
        "pillars": {
            "acquisition": {"score": None, "status": None},
            "funnel": {"score": 100.0, "status": "STRONG"},
        },
        "signals": [],
    }
    out = _row_to_diagnostic(row)
    assert out["health_coverage"] is None
    assert out["health_score"] == 100.0
    assert out["pillars"]["funnel"]["score"] == 100.0


def test_row_to_diagnostic_derives_health_coverage():
    """Post-fix rows carry per-pillar weight/coverage inside the pillars
    JSON; the API derives the campaign-level weighted coverage."""
    row = {
        "id": "new-1",
        "project_code": "26018",
        "campaign_type": "conversion",
        "evaluation_date": date(2026, 6, 3),
        "flight_day": 28,
        "flight_total_days": 30,
        "health_score": None,
        "health_status": None,
        "pillars": {
            "acquisition": {
                "score": None, "status": None,
                "weight": 0.43, "coverage": 0.0,
                "signals_active": 0, "signals_total": 3,
            },
            "funnel": {
                "score": None, "status": None,
                "weight": 0.57, "coverage": 0.3,
                "signals_active": 2, "signals_total": 5,
            },
        },
        "signals": [],
    }
    out = _row_to_diagnostic(row)
    assert out["health_coverage"] == round(0.57 * 0.3 / 1.0, 3)
    assert out["health_score"] is None
