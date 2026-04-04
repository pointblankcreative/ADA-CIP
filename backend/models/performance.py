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
    reach_adset: int | None = None
    frequency_adset: float | None = None
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
    total_reach_adset: int | None = None
    avg_frequency_adset: float | None = None
    reach_platforms: list[str] = []
    reach_note: str | None = None
    high_frequency_warning: str | None = None
    available_metrics: list[str] = []
    metric_platforms: dict[str, list[str]] = {}
    daily: list[DailyMetric] = []
    by_platform: list[PlatformBreakdown] = []
    campaigns: list[CampaignRow] = []


class AdSetRow(BaseModel):
    ad_set_id: str | None = None
    ad_set_name: str | None = None
    platform_id: str
    campaign_name: str | None = None
    spend: float = 0
    impressions: int = 0
    clicks: int = 0
    conversions: float = 0
    engagements: int = 0
    video_views: int = 0
    video_completions: int = 0
    cpm: float | None = None
    cpc: float | None = None
    ctr: float | None = None
    vcr: float | None = None
    engagement_rate: float | None = None
    reach: int | None = None
    frequency: float | None = None
    reach_window: str | None = None
    cost_per_reach: float | None = None
    ad_count: int = 0


class AdSetPerformanceResponse(BaseModel):
    project_code: str
    start_date: date | None = None
    end_date: date | None = None
    ad_sets: list[AdSetRow] = []
    total_reach_note: str | None = None


class AdRow(BaseModel):
    ad_id: str | None = None
    ad_name: str | None = None
    ad_set_name: str | None = None
    platform_id: str
    campaign_name: str | None = None
    spend: float = 0
    impressions: int = 0
    clicks: int = 0
    conversions: float = 0
    engagements: int = 0
    video_views: int = 0
    video_completions: int = 0
    cpm: float | None = None
    cpc: float | None = None
    ctr: float | None = None
    vcr: float | None = None
    engagement_rate: float | None = None


class AdPerformanceResponse(BaseModel):
    project_code: str
    start_date: date | None = None
    end_date: date | None = None
    ads: list[AdRow] = []
