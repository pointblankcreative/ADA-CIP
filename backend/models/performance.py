from datetime import date

from pydantic import BaseModel


class DailyMetric(BaseModel):
    date: date
    spend: float = 0
    impressions: int = 0
    clicks: int = 0
    conversions: float = 0
    cpm: float | None = None
    cpc: float | None = None
    ctr: float | None = None
    reach: int | None = None
    frequency: float | None = None
    video_views: int | None = None
    video_completions: int | None = None
    vcr: float | None = None
    engagements: int | None = None
    cpa: float | None = None
    conversion_rate: float | None = None


class PlatformBreakdown(BaseModel):
    platform_id: str
    platform_name: str
    spend: float = 0
    impressions: int = 0
    clicks: int = 0
    conversions: float = 0
    reach: int | None = None
    frequency: float | None = None
    video_views: int | None = None
    video_completions: int | None = None
    engagements: int | None = None


class CampaignRow(BaseModel):
    campaign_id: str
    campaign_name: str
    platform_id: str
    objective: str | None = None
    spend: float = 0
    impressions: int = 0
    clicks: int = 0
    conversions: float = 0
    cpm: float | None = None
    cpc: float | None = None
    ctr: float | None = None
    reach: int | None = None
    frequency: float | None = None
    video_views: int | None = None
    video_completions: int | None = None
    vcr: float | None = None
    engagements: int | None = None
    cpa: float | None = None
    conversion_rate: float | None = None


class PerformanceResponse(BaseModel):
    project_code: str
    objective_type: str = "mixed"
    start_date: date
    end_date: date
    total_spend: float = 0
    total_impressions: int = 0
    total_clicks: int = 0
    total_conversions: float = 0
    total_reach: int | None = None
    total_frequency: float | None = None
    total_video_views: int | None = None
    total_video_completions: int | None = None
    total_vcr: float | None = None
    total_engagements: int | None = None
    total_cpa: float | None = None
    total_conversion_rate: float | None = None
    available_metrics: list[str] = []
    metric_platforms: dict[str, list[str]] = {}
    daily: list[DailyMetric] = []
    by_platform: list[PlatformBreakdown] = []
    campaigns: list[CampaignRow] = []
