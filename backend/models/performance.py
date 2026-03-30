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


class PlatformBreakdown(BaseModel):
    platform_id: str
    platform_name: str
    spend: float = 0
    impressions: int = 0
    clicks: int = 0
    conversions: float = 0


class CampaignRow(BaseModel):
    campaign_id: str
    campaign_name: str
    platform_id: str
    spend: float = 0
    impressions: int = 0
    clicks: int = 0
    conversions: float = 0
    cpm: float | None = None
    cpc: float | None = None
    ctr: float | None = None


class PerformanceResponse(BaseModel):
    project_code: str
    start_date: date
    end_date: date
    total_spend: float = 0
    total_impressions: int = 0
    total_clicks: int = 0
    total_conversions: float = 0
    daily: list[DailyMetric] = []
    by_platform: list[PlatformBreakdown] = []
    campaigns: list[CampaignRow] = []
