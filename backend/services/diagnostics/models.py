"""Pydantic models for the diagnostic signal engine.

Four-layer pyramid:
    Level 1: DiagnosticOutput (health score)
    Level 2: PillarScore
        - Persuasion: distribution, attention, resonance
        - Conversion: acquisition, funnel
          (A Quality pillar was originally scoped but is deferred
          pending per-client CRM integration — see
          docs/diagnostics/quality-pillar-deferred.md.)
    Level 3: SignalResult (D1-D4, A1-A5, R1-R3 / C1-C3, F1-F5)
    Level 4: EfficiencyMetrics (CPM, CPC, CPA, CPCV, pacing %)

Plus: typed wrappers for BQ query results so signal functions
get clean, documented inputs instead of raw dicts.
"""

from __future__ import annotations

import uuid
from datetime import date, datetime, timezone
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field

from backend.config import settings


def _current_engine_version() -> str:
    """Read the engine version from settings at construction time.

    Indirected through a function (rather than using `settings.engine_version`
    directly in a default_factory) so tests can monkeypatch
    `backend.services.diagnostics.models._current_engine_version` without
    having to mutate the settings singleton.
    """
    return settings.engine_version


# ── Enums ───────────────────────────────────────────────────────────


class CampaignType(str, Enum):
    PERSUASION = "persuasion"
    CONVERSION = "conversion"


class StatusBand(str, Enum):
    STRONG = "STRONG"
    WATCH = "WATCH"
    ACTION = "ACTION"


class AlertSeverity(str, Enum):
    CRITICAL = "critical"
    WARNING = "warning"
    INFO = "info"


class AudienceType(str, Enum):
    MEMBER_LIST = "member_list"
    RETARGETING = "retargeting"
    PROSPECTING = "prospecting"


# ── Level 3: Individual Signal ──────────────────────────────────────


class SignalResult(BaseModel):
    """One diagnostic signal evaluation (e.g. D1 Reach Attainment)."""

    id: str                                 # "D1", "A1", "C1", etc.
    name: str                               # Human-readable signal name
    score: float | None = None              # 0-100, None if guard fails
    status: StatusBand | None = None        # STRONG / WATCH / ACTION / None
    raw_value: float | None = None          # Computed metric before normalization
    benchmark: float | None = None          # What we compared against
    floor: float | None = None              # Minimum threshold
    diagnostic: str = ""                    # Human-readable RA message
    guard_passed: bool = True               # Did this signal have enough data?
    guard_reason: str | None = None         # Why guard failed (if applicable)
    inputs: dict[str, Any] = Field(default_factory=dict)  # Raw calc inputs


# ── Level 2: Pillar ─────────────────────────────────────────────────


class PillarScore(BaseModel):
    """Aggregate score for one diagnostic pillar."""

    name: str                               # "distribution", "attention", etc.
    score: float | None = None              # 0-100 weighted average of signals
    status: StatusBand | None = None
    signals: list[SignalResult] = Field(default_factory=list)
    weight: float = 1.0                     # Pillar weight in health rollup
    # Coverage metadata (AI-040): fraction of the pillar's designed signal
    # weight that actually reported (guard-passed + scored). None only on
    # legacy rows read back from BQ that predate this field.
    coverage: float | None = None
    signals_active: int = 0
    signals_total: int = 0

    @property
    def active_signals(self) -> list[SignalResult]:
        """Signals that passed their guard and have a score."""
        return [s for s in self.signals if s.guard_passed and s.score is not None]

    def apply_weighted_score(
        self,
        weights: dict[str, float],
        min_coverage: float,
        default_weight: float | None = None,
    ) -> None:
        """Shared pillar rollup used by every pillar builder (AI-040).

        Computes the weighted average of guard-passed signal scores AND the
        weighted coverage (active design weight / total design weight). If
        coverage < ``min_coverage`` the score/status are withheld (None) —
        the coverage fields stay populated so the frontend can render
        "n of m signals reporting".

        ``weights`` is the pillar's design-weight table (arch-blended for
        Funnel). Signals absent from ``weights`` use ``default_weight`` when
        given (legacy leniency — Distribution/Attention/Acquisition pattern),
        or raise KeyError when None (strict mode — Resonance/Funnel pattern).
        The coverage denominator only counts signals with weight > 0, so
        signals that structurally don't apply (e.g. F2/F3 on a pure Arch-B
        campaign) never count against coverage.
        """
        def _w(sig_id: str) -> float:
            if sig_id in weights:
                return weights[sig_id]
            if default_weight is not None:
                return default_weight
            raise KeyError(
                f"Signal weight table for pillar '{self.name}' is missing an "
                f"entry for {sig_id!r}. Add it to shared.benchmarks before "
                f"scoring."
            )

        total_design_weight = sum(
            w for w in (_w(s.id) for s in self.signals) if w > 0
        )
        active = self.active_signals
        active_weight = sum(_w(s.id) for s in active)

        self.signals_total = sum(1 for s in self.signals if _w(s.id) > 0)
        self.signals_active = len(active)
        self.coverage = (
            round(active_weight / total_design_weight, 3)
            if total_design_weight > 0 else 0.0
        )

        if not active or active_weight <= 0 or self.coverage < min_coverage:
            # Either nothing reported, or too little of the pillar's designed
            # weight reported to present a score honestly.
            self.score = None
            self.status = None
            return

        weighted_sum = sum(s.score * _w(s.id) for s in active)
        self.score = round(weighted_sum / active_weight, 1)
        self.status = status_band(self.score)

    def compute_score(self) -> None:
        """Set pillar score to the mean of active signal scores."""
        active = self.active_signals
        if not active:
            self.score = None
            self.status = None
            return
        self.score = round(sum(s.score for s in active) / len(active), 1)
        self.status = status_band(self.score)


# ── Level 4: Efficiency ─────────────────────────────────────────────


class EfficiencyMetrics(BaseModel):
    """Cost-efficiency layer — context, not scored."""

    cpm: float | None = None
    cpc: float | None = None
    cpa: float | None = None
    cpcv: float | None = None               # Cost per completed view
    pacing_pct: float | None = None         # Budget pacing %


# ── Level 1: Full Diagnostic Output ─────────────────────────────────


class DiagnosticAlert(BaseModel):
    """A critical or warning alert fired during evaluation."""

    type: str
    severity: AlertSeverity
    message: str
    signal_id: str | None = None


class DiagnosticOutput(BaseModel):
    """Complete diagnostic evaluation for one project + campaign type."""

    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    project_code: str
    campaign_type: CampaignType
    evaluation_date: date
    flight_day: int
    flight_total_days: int

    health_score: float | None = None
    health_status: StatusBand | None = None
    # AI-040: fraction of total designed signal weight that reported,
    # rolled up through pillar weights. None on legacy rows only.
    health_coverage: float | None = None

    pillars: list[PillarScore] = Field(default_factory=list)
    efficiency: EfficiencyMetrics = Field(default_factory=EfficiencyMetrics)
    alerts: list[DiagnosticAlert] = Field(default_factory=list)

    platforms: list[str] = Field(default_factory=list)
    line_ids: list[str] = Field(default_factory=list)

    computed_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    spec_version: str = "1.1"
    # engine_version is an orthogonal discriminator from spec_version:
    # spec_version tracks the scoring spec (human-tagged, changes rarely),
    # engine_version tracks the code SHA (auto, changes every deploy).
    # Sourced from settings at construction time so tests can override by
    # patching settings.engine_version.
    engine_version: str = Field(
        default_factory=lambda: _current_engine_version()
    )

    def compute_health_score(self) -> None:
        """Weighted average of pillar scores → health score, gated on
        weighted coverage (AI-040).

        The coverage denominator includes ALL pillars (scored or not): a
        pillar whose every signal guard-failed contributes 0 coverage
        instead of disappearing from the average. Below
        MIN_HEALTH_COVERAGE the health score is withheld — the dashboard
        shows INSUFFICIENT DATA rather than a confident number built from
        a sliver of the design weight.
        """
        # Local import: benchmarks is a leaf constants module, but models
        # is imported by nearly everything — keep the module-level import
        # graph of models dependency-free to avoid future cycles.
        from backend.services.diagnostics.shared.benchmarks import (
            MIN_HEALTH_COVERAGE,
        )

        all_weight = sum(p.weight for p in self.pillars)
        if all_weight > 0:
            self.health_coverage = round(
                sum(p.weight * (p.coverage or 0.0) for p in self.pillars)
                / all_weight,
                3,
            )
        else:
            self.health_coverage = 0.0

        scored = [p for p in self.pillars if p.score is not None]
        total_weight = sum(p.weight for p in scored)
        if (
            not scored
            or total_weight == 0
            or self.health_coverage < MIN_HEALTH_COVERAGE
        ):
            self.health_score = None
            self.health_status = None
            return

        weighted_sum = sum(p.score * p.weight for p in scored)
        self.health_score = round(weighted_sum / total_weight, 1)
        self.health_status = status_band(self.health_score)

    def to_bq_row(self) -> dict:
        """Serialize to a dict matching fact_diagnostic_signals schema."""
        return {
            "id": self.id,
            "project_code": self.project_code,
            "campaign_type": self.campaign_type.value,
            "evaluation_date": self.evaluation_date.isoformat(),
            "flight_day": self.flight_day,
            "flight_total_days": self.flight_total_days,
            "health_score": self.health_score,
            "health_status": self.health_status.value if self.health_status else None,
            "pillars": {
                p.name: {
                    "score": p.score,
                    "status": p.status.value if p.status else None,
                    # AI-040 coverage metadata — additive keys inside the
                    # existing pillars JSON column; no BQ schema change.
                    "weight": p.weight,
                    "coverage": p.coverage,
                    "signals_active": p.signals_active,
                    "signals_total": p.signals_total,
                }
                for p in self.pillars
            },
            "signals": [
                {
                    "id": s.id,
                    "name": s.name,
                    "score": s.score,
                    "status": s.status.value if s.status else None,
                    "raw_value": s.raw_value,
                    "benchmark": s.benchmark,
                    "floor": s.floor,
                    "diagnostic": s.diagnostic,
                    "guard_passed": s.guard_passed,
                    "guard_reason": s.guard_reason,
                    "inputs": s.inputs,
                }
                for p in self.pillars
                for s in p.signals
            ],
            "efficiency": self.efficiency.model_dump(),
            "alerts": [a.model_dump() for a in self.alerts],
            "platforms": self.platforms,
            "line_ids": self.line_ids,
            "computed_at": self.computed_at.isoformat(),
            "spec_version": self.spec_version,
            "engine_version": self.engine_version,
        }


# ── Data wrappers for BQ query results ──────────────────────────────


class FlightContext(BaseModel):
    """Calendar context for the campaign evaluation."""

    flight_start: date
    flight_end: date
    evaluation_date: date

    @property
    def total_days(self) -> int:
        return max((self.flight_end - self.flight_start).days + 1, 1)

    @property
    def elapsed_days(self) -> int:
        return max((self.evaluation_date - self.flight_start).days + 1, 0)

    @property
    def elapsed_fraction(self) -> float:
        return min(self.elapsed_days / self.total_days, 1.0)

    @property
    def remaining_days(self) -> int:
        return max((self.flight_end - self.evaluation_date).days, 0)


class PlatformMetrics(BaseModel):
    """Aggregated metrics for one platform within a campaign."""

    platform_id: str
    spend: float = 0
    impressions: int = 0
    clicks: int = 0
    conversions: float = 0
    reach: int = 0                          # MAX across dates (not SUM)
    frequency: float = 0                    # AVG
    video_views_3s: int = 0
    thruplay: int = 0
    video_q25: int = 0
    video_q50: int = 0
    video_q75: int = 0
    video_q100: int = 0
    post_engagement: int = 0
    post_reactions: int = 0
    post_comments: int = 0
    outbound_clicks: int = 0
    landing_page_views: int = 0
    registrations: float = 0
    leads: float = 0
    on_platform_leads: float = 0
    contacts: float = 0
    donations: float = 0
    campaign_objective: str | None = None
    viewability_measured: int = 0
    viewability_viewed: int = 0


class DailyMetrics(BaseModel):
    """One day of aggregated metrics (for trend signals)."""

    date: date
    platform_id: str
    spend: float = 0
    impressions: int = 0
    clicks: int = 0
    conversions: float = 0
    video_views_3s: int = 0
    thruplay: int = 0
    post_engagement: int = 0


class MediaPlanLine(BaseModel):
    """One line from the media plan."""

    line_id: str
    platform_id: str | None = None
    channel_category: str | None = None
    audience_name: str | None = None
    audience_type: AudienceType | None = None
    planned_budget: float = 0
    planned_impressions: int = 0
    planned_reach: int = 0
    frequency_cap: float = 0
    flight_start: date | None = None
    flight_end: date | None = None
    ffs_score: float | None = None
    ffs_inputs: dict[str, Any] | None = None
    objective: str | None = None


class GA4Metrics(BaseModel):
    """Aggregated GA4 session data for a campaign."""

    sessions: int = 0
    scrolls: int = 0
    engaged_sessions: int = 0
    form_starts: int = 0
    form_submits: int = 0
    key_events: int = 0


class CampaignData(BaseModel):
    """All data needed to evaluate one campaign — assembled by the engine
    from multiple BQ queries before being passed to signal functions."""

    project_code: str
    campaign_type: CampaignType
    flight: FlightContext

    # Aggregated by platform (for distribution / attention signals)
    platform_metrics: list[PlatformMetrics] = Field(default_factory=list)

    # Daily breakdown (for trend signals)
    daily_metrics: list[DailyMetrics] = Field(default_factory=list)

    # Media plan lines
    media_plan: list[MediaPlanLine] = Field(default_factory=list)

    # GA4 data
    ga4: GA4Metrics = Field(default_factory=GA4Metrics)

    # Budget tracking
    budget_pacing_pct: float | None = None

    @property
    def total_spend(self) -> float:
        return sum(p.spend for p in self.platform_metrics)

    @property
    def total_impressions(self) -> int:
        return sum(p.impressions for p in self.platform_metrics)

    @property
    def total_clicks(self) -> int:
        return sum(p.clicks for p in self.platform_metrics)

    @property
    def total_conversions(self) -> float:
        return sum(p.conversions for p in self.platform_metrics)

    @property
    def total_reach(self) -> int:
        """MAX reach across platforms (not SUM — Phase 0 finding)."""
        if not self.platform_metrics:
            return 0
        return max(p.reach for p in self.platform_metrics)

    @property
    def total_video_views_3s(self) -> int:
        return sum(p.video_views_3s for p in self.platform_metrics)

    @property
    def total_thruplay(self) -> int:
        return sum(p.thruplay for p in self.platform_metrics)

    @property
    def total_post_engagement(self) -> int:
        return sum(p.post_engagement for p in self.platform_metrics)

    @property
    def planned_budget(self) -> float:
        return sum(l.planned_budget for l in self.media_plan)

    @property
    def planned_impressions(self) -> int:
        return sum(l.planned_impressions for l in self.media_plan)

    @property
    def planned_reach(self) -> int:
        return sum(l.planned_reach for l in self.media_plan)


# ── Helpers ──────────────────────────────────────────────────────────


def status_band(score: float | None) -> StatusBand | None:
    """Map a 0-100 score to STRONG / WATCH / ACTION."""
    if score is None:
        return None
    if score >= 70:
        return StatusBand.STRONG
    if score >= 40:
        return StatusBand.WATCH
    return StatusBand.ACTION
