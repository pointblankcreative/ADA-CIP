const API_BASE = process.env.NEXT_PUBLIC_API_URL ?? "http://localhost:8000";

export async function apiFetch<T>(
  path: string,
  options?: RequestInit
): Promise<T> {
  const res = await fetch(`${API_BASE}${path}`, {
    headers: { "Content-Type": "application/json", ...options?.headers },
    ...options,
  });
  if (!res.ok) {
    throw new Error(`API error ${res.status}: ${res.statusText}`);
  }
  return res.json();
}

/* ── Type definitions matching backend responses ── */

export interface Project {
  project_code: string;
  project_name: string;
  client_name: string | null;
  status: string;
  start_date: string;
  end_date: string;
  net_budget: number;
  total_spend: number;
  pacing_percentage: number | null;
  days_remaining: number;
  recently_ended?: boolean;
  updated_at: string;
}

export interface PacingLine {
  line_id: string;
  line_code: string | null;
  platform_id: string;
  channel_category: string;
  planned_budget: number;
  planned_spend_to_date: number;
  actual_spend_to_date: number;
  remaining_budget: number;
  remaining_days: number;
  pacing_percentage: number;
  daily_budget_required: number | null;
  is_over_pacing: boolean;
  is_under_pacing: boolean;
}

export interface PacingResponse {
  project_code: string;
  as_of_date: string;
  net_budget: number;
  total_planned_to_date: number;
  total_actual_to_date: number;
  overall_pacing_percentage: number;
  lines: PacingLine[];
}

export interface DailyPerformance {
  date: string;
  spend: number;
  impressions: number;
  clicks: number;
  conversions: number;
  cpm: number;
  cpc: number;
  ctr: number;
  reach?: number | null;
  frequency?: number | null;
  video_views?: number | null;
  video_completions?: number | null;
  vcr?: number | null;
  engagements?: number | null;
  cpa?: number | null;
  conversion_rate?: number | null;
}

export interface PlatformBreakdown {
  platform_id: string;
  platform_name?: string;
  spend: number;
  impressions: number;
  clicks: number;
  conversions: number;
  reach?: number | null;
  frequency?: number | null;
  video_views?: number | null;
  video_completions?: number | null;
  engagements?: number | null;
}

export interface CampaignRow {
  campaign_name: string;
  campaign_id: string;
  platform_id: string;
  objective?: string | null;
  spend: number;
  impressions: number;
  clicks: number;
  conversions: number;
  ctr: number;
  cpc: number;
  cpm?: number;
  reach?: number | null;
  frequency?: number | null;
  video_views?: number | null;
  video_completions?: number | null;
  vcr?: number | null;
  engagements?: number | null;
  cpa?: number | null;
  conversion_rate?: number | null;
}

export type ObjectiveType = "awareness" | "conversion" | "mixed";

export interface PerformanceResponse {
  project_code: string;
  objective_type: ObjectiveType;
  start_date: string;
  end_date: string;
  total_spend: number;
  total_impressions: number;
  total_clicks: number;
  total_conversions: number;
  total_reach?: number | null;
  total_frequency?: number | null;
  total_video_views?: number | null;
  total_video_completions?: number | null;
  total_vcr?: number | null;
  total_engagements?: number | null;
  total_cpa?: number | null;
  total_conversion_rate?: number | null;
  available_metrics: string[];
  metric_platforms: Record<string, string[]>;
  daily: DailyPerformance[];
  by_platform?: PlatformBreakdown[];
  campaigns?: CampaignRow[];
}

export interface Alert {
  alert_id: string;
  project_code: string;
  alert_type: string;
  severity: "critical" | "warning" | "info";
  title: string;
  message: string;
  metadata: Record<string, unknown> | null;
  created_at: string;
  acknowledged_at: string | null;
  acknowledged_by: string | null;
  resolved_at: string | null;
  slack_sent: boolean;
}

export interface AdminProject extends Project {
  client_id: string | null;
  campaign_type: string | null;
  currency: string;
  platforms_active: number;
  first_data_date: string | null;
  last_data_date: string | null;
  media_plan_sheet_id: string | null;
  media_plan_synced: boolean;
  slack_channel_id: string | null;
  alert_count: number;
  created_at: string | null;
}

export interface ProjectCreatePayload {
  project_code: string;
  client_name: string;
  project_name: string;
  start_date: string;
  end_date: string;
  net_budget: number;
  media_plan_sheet_url?: string;
  slack_channel_id?: string;
}

export interface ProjectCreateResponse {
  project: Record<string, unknown>;
  media_plan_sync?: { status: string; message?: string };
}

export interface PlatformFreshness {
  platform_id: string;
  latest_data_date: string | null;
  latest_loaded_at: string | null;
  total_days: number;
  total_rows: number;
}

export interface IngestionRun {
  run_id: string;
  pipeline_name: string;
  mode: string;
  status: string;
  rows_processed: number;
  started_at: string;
  completed_at: string | null;
  error_message: string | null;
}

/* ── GA4 types ── */

export interface GA4Property {
  property_id: string;
  property_name: string | null;
}

export interface GA4Url {
  id: string;
  project_code: string;
  ga4_property_id: string;
  url_pattern: string;
  label: string | null;
  created_at?: string | null;
}

export interface GA4DailyAnalytics {
  date: string;
  sessions: number;
  conversions: number;
  bounce_rate?: number | null;
  avg_session_duration?: number | null;
  pages_per_session?: number | null;
}

export interface GA4PerformanceResponse {
  has_ga4: boolean;
  urls: GA4Url[];
  daily: GA4DailyAnalytics[];
  total_sessions: number;
  total_conversions: number;
  avg_bounce_rate?: number | null;
  avg_session_duration?: number | null;
}

/* ── Benchmark types ── */

export interface BenchmarkValue {
  benchmark_id: string;
  scope: string;
  platform_id: string | null;
  metric_name: string;
  metric_unit: string;
  p25: number | null;
  p50: number | null;
  p75: number | null;
  sample_size: number | null;
  source: string | null;
  notes: string | null;
}

export interface BenchmarkResponse {
  project_code: string;
  objective_type: string;
  benchmarks: Record<string, BenchmarkValue>;
  platform_benchmarks: Record<string, Record<string, BenchmarkValue>>;
}

/* ── API functions ── */

export const api = {
  projects: {
    list: () => apiFetch<Project[]>("/api/projects/"),
    get: (code: string) => apiFetch<Project>(`/api/projects/${code}`),
  },
  pacing: {
    get: (code: string) => apiFetch<PacingResponse>(`/api/pacing/${code}`),
    run: (code: string) =>
      apiFetch(`/api/pacing/${code}/run`, { method: "POST" }),
  },
  performance: {
    get: (code: string, days?: number) =>
      apiFetch<PerformanceResponse>(
        `/api/performance/${code}${days ? `?days=${days}` : ""}`
      ),
  },
  alerts: {
    list: (params?: { project_code?: string; severity?: string; limit?: number }) => {
      const qs = new URLSearchParams();
      if (params?.project_code) qs.set("project_code", params.project_code);
      if (params?.severity) qs.set("severity", params.severity);
      if (params?.limit) qs.set("limit", String(params.limit));
      return apiFetch<Alert[]>(`/api/alerts/?${qs}`);
    },
    acknowledge: (id: string) =>
      apiFetch(`/api/alerts/${id}/acknowledge`, { method: "POST" }),
    dispatch: () => apiFetch<Record<string, unknown>>("/api/alerts/dispatch", { method: "POST" }),
  },
  benchmarks: {
    get: (code: string) => apiFetch<BenchmarkResponse>(`/api/benchmarks/${code}`),
  },
  ga4: {
    properties: () => apiFetch<GA4Property[]>("/api/ga4/properties"),
    urls: (code: string) => apiFetch<GA4Url[]>(`/api/ga4/${code}/urls`),
    addUrl: (code: string, data: { ga4_property_id: string; url_pattern: string; label?: string }) =>
      apiFetch<GA4Url>(`/api/ga4/${code}/urls`, {
        method: "POST",
        body: JSON.stringify(data),
      }),
    deleteUrl: (code: string, urlId: string) =>
      apiFetch(`/api/ga4/${code}/urls/${urlId}`, { method: "DELETE" }),
    analytics: (code: string, days?: number) => {
      const qs = days ? `?start_date=${new Date(Date.now() - days * 86400000).toISOString().slice(0, 10)}` : "";
      return apiFetch<GA4PerformanceResponse>(`/api/ga4/${code}/analytics${qs}`);
    },
  },
  admin: {
    projects: {
      list: () => apiFetch<AdminProject[]>("/api/admin/projects"),
      create: (data: ProjectCreatePayload) =>
        apiFetch<ProjectCreateResponse>("/api/admin/projects", {
          method: "POST",
          body: JSON.stringify(data),
        }),
      update: (code: string, data: Record<string, unknown>) =>
        apiFetch<Record<string, unknown>>(`/api/admin/projects/${code}`, {
          method: "PUT",
          body: JSON.stringify(data),
        }),
    },
    syncMediaPlan: (sheetId: string, projectCode: string) =>
      apiFetch<Record<string, unknown>>(
        `/api/admin/sync-media-plan?sheet_id=${encodeURIComponent(sheetId)}&project_code=${encodeURIComponent(projectCode)}`,
        { method: "POST" }
      ),
    runTransformation: (mode = "daily") =>
      apiFetch<Record<string, unknown>>(
        `/api/admin/run-transformation?mode=${mode}`,
        { method: "POST" }
      ),
    dailyRun: () =>
      apiFetch<Record<string, unknown>>("/api/admin/daily-run", { method: "POST" }),
    dataFreshness: () =>
      apiFetch<{ platforms: PlatformFreshness[] }>("/api/admin/data-freshness"),
    ingestionLog: (limit = 20) =>
      apiFetch<{ runs: IngestionRun[] }>(`/api/admin/ingestion-log?limit=${limit}`),
    runPacing: (code: string) =>
      apiFetch<Record<string, unknown>>(`/api/pacing/${code}/run`, { method: "POST" }),
  },
};
