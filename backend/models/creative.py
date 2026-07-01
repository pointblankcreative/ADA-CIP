"""Response models for the Phase 14 Creative + Audiences redesign endpoints.

Three surfaces, all read-only rollups over fact_digital_daily /
fact_adset_daily at creative-variant and ad-set grain:

  * /api/projects/{code}/creative/rotation — ranked creative list with
    KPI rollups, 8-point daily trends, and platform metric coverage.
  * /api/projects/{code}/creative/matrix   — creative × platform cells.
  * /api/projects/{code}/audiences/matrix  — ad-set (audience) rows plus
    audience × creative cells.

Rate metrics are nullable throughout: None means "not reported /
insufficient volume" and renders as an em-dash (AI-029 pattern), never 0.
"""

from datetime import date

from pydantic import BaseModel, Field


class CreativeTrend(BaseModel):
    """Last-8-daily-point sparkline series, oldest → newest.

    `primary` is completion_rate on awareness projects and CPA on
    conversion/mixed projects (the rotation response's `objective` field
    tells the frontend which). Arrays are empty when a creative has fewer
    than two usable daily points — a single point is not a trend.
    """

    ctr: list[float] = []
    frequency: list[float] = []
    primary: list[float] = []


class CreativeRotationRow(BaseModel):
    variant: str
    type: str  # "video" | "static"
    # Signed GCS URL for the stored ad still (Phase 19 asset sync), ~7-day
    # expiry like the alert charts. None until the sync finds a match.
    image_url: str | None = None
    platforms: list[str] = []
    spend: float = 0
    spend_share: float = 0  # share of total creative spend, 0-1
    impressions: int = 0
    frequency: float | None = None
    hook_rate: float | None = None
    completion_rate: float | None = None
    engagement_rate: float | None = None
    ctr: float | None = None
    clicks: int = 0
    cpm: float | None = None
    conversions: float = 0
    cpa: float | None = None
    trend: CreativeTrend = Field(default_factory=CreativeTrend)


class CreativeTotals(BaseModel):
    """Window totals: sums for volume fields, impression-weighted rollups
    for rate fields (same coverage + volume-guard rules as the rows)."""

    spend: float = 0
    impressions: int = 0
    frequency: float | None = None
    hook_rate: float | None = None
    completion_rate: float | None = None
    engagement_rate: float | None = None
    ctr: float | None = None
    clicks: int = 0
    cpm: float | None = None
    conversions: float = 0
    cpa: float | None = None


class CreativeCoverage(BaseModel):
    """platform_ids that report each derived rate's source metric in the
    window (mirrors the metric_platforms pattern in the performance router,
    but keyed by platform_id rather than display name)."""

    hook: list[str] = []
    completion: list[str] = []
    engagement: list[str] = []


class CreativeRotationResponse(BaseModel):
    project_code: str
    objective: str  # "awareness" | "conversion" | "mixed"
    window: str  # "flight" | "7d"
    as_of: date
    creatives: list[CreativeRotationRow] = []
    coverage: CreativeCoverage = Field(default_factory=CreativeCoverage)
    totals: CreativeTotals = Field(default_factory=CreativeTotals)


class MatrixPlatform(BaseModel):
    platform_id: str
    spend: float = 0
    share: float = 0  # share of total creative spend, 0-1


class CreativeMatrixCell(BaseModel):
    spend: float = 0
    impressions: int = 0
    hook_rate: float | None = None
    completion_rate: float | None = None
    engagement_rate: float | None = None
    ctr: float | None = None
    cpm: float | None = None
    conversions: float = 0
    cpa: float | None = None
    # Phase 19 / #11: raw per-platform video quartile completion counts
    # (SUM over the cell's ads). Anchor is video_q25; the frontend divides
    # by video_q25 to draw the per-platform retention curve. Non-video
    # cells stay 0. Kept off CreativeTotals — cells only.
    video_q25: int = 0
    video_q50: int = 0
    video_q75: int = 0
    video_q100: int = 0


class CreativeMatrixResponse(BaseModel):
    project_code: str
    platforms: list[MatrixPlatform] = []
    creatives: list[str] = []  # variant names, rotation rank order
    # cells[variant][platform_id] — absent where the variant doesn't run
    cells: dict[str, dict[str, CreativeMatrixCell]] = {}


class AudienceRow(BaseModel):
    id: str  # stable slug of ad_set_name + platform_id
    name: str
    platform_id: str
    role: str | None = None  # media_plan_lines.audience_type, else None
    spend: float = 0
    frequency: float | None = None
    frequency_trend: list[float] = []  # last 8 daily points, oldest → newest
    impressions: int = 0
    ctr: float | None = None
    completion_rate: float | None = None
    engagement_rate: float | None = None
    conversions: float = 0
    cpa: float | None = None
    # Phase 19 targeting sync (Meta ad sets only) — all None elsewhere.
    # persona is a deterministic plain-English render of the targeting
    # spec; pool_size comes from delivery_estimate; saturation is
    # reach / pool_size (AI-103 latest-snapshot reach), None when either
    # side is missing.
    persona: str | None = None
    pool_size: int | None = None
    saturation: float | None = None


class AudienceMatrixCell(BaseModel):
    spend: float = 0
    impressions: int = 0
    hook_rate: float | None = None
    completion_rate: float | None = None
    engagement_rate: float | None = None
    ctr: float | None = None
    conversions: float = 0
    cpa: float | None = None


class AudienceMatrixResponse(BaseModel):
    project_code: str
    audiences: list[AudienceRow] = []
    creatives: list[str] = []  # variant names, rotation rank order
    # cells[audience_id][variant] — absent where the pairing never ran
    cells: dict[str, dict[str, AudienceMatrixCell]] = {}
