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

export type BundleRole =
  | "suggested_parent"
  | "suggested_child"
  | "confirmed_parent"
  | "confirmed_child"
  | "rejected";

export interface BundleMember {
  line_id: string;
  line_code: string | null;
  audience_name: string | null;
}

export interface PacingLine {
  line_id: string;
  line_code: string | null;
  platform_id: string;
  channel_category: string;
  audience_name: string | null;
  flight_start: string | null;
  flight_end: string | null;
  line_status?: "not_started" | "pending" | "active" | "completed";
  planned_budget: number;
  planned_spend_to_date: number;
  actual_spend_to_date: number;
  remaining_budget: number;
  remaining_days: number;
  pacing_percentage: number;
  daily_budget_required: number | null;
  is_over_pacing: boolean;
  is_under_pacing: boolean;
  // Bundled-optimization support (PR 5). NULL for standalone lines.
  bundle_id: string | null;
  bundle_role: BundleRole | null;
  bundle_members: BundleMember[];
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

export interface PacingHistoryPoint {
  date: string;
  line_id: string;
  pacing_percentage: number;
}

export interface PacingHistoryResponse {
  project_code: string;
  history: PacingHistoryPoint[];
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
  reach_adset?: number | null;
  frequency_adset?: number | null;
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
  total_reach_adset?: number | null;
  avg_frequency_adset?: number | null;
  reach_platforms?: string[];
  reach_note?: string | null;
  high_frequency_warning?: string | null;
  available_metrics: string[];
  metric_platforms: Record<string, string[]>;
  daily: DailyPerformance[];
  by_platform?: PlatformBreakdown[];
  campaigns?: CampaignRow[];
}

export interface AdSetRow {
  ad_set_id: string | null;
  ad_set_name: string | null;
  platform_id: string;
  campaign_name: string | null;
  spend: number;
  impressions: number;
  clicks: number;
  conversions: number;
  engagements: number;
  video_views: number;
  video_completions: number;
  cpm: number | null;
  cpc: number | null;
  ctr: number | null;
  vcr: number | null;
  engagement_rate: number | null;
  reach: number | null;
  frequency: number | null;
  reach_window: string | null;
  cost_per_reach: number | null;
  ad_count: number;
}

export interface AdSetPerformanceResponse {
  project_code: string;
  start_date: string | null;
  end_date: string | null;
  ad_sets: AdSetRow[];
  total_reach_note: string | null;
}

export interface AdRow {
  ad_id: string | null;
  ad_name: string | null;
  ad_set_name: string | null;
  platform_id: string;
  campaign_name: string | null;
  spend: number;
  impressions: number;
  clicks: number;
  conversions: number;
  engagements: number;
  video_views: number;
  video_completions: number;
  cpm: number | null;
  cpc: number | null;
  ctr: number | null;
  vcr: number | null;
  engagement_rate: number | null;
}

export interface AdPerformanceResponse {
  project_code: string;
  start_date: string | null;
  end_date: string | null;
  ads: AdRow[];
}

export interface CreativeVariantRow {
  creative_variant: string;
  ad_names: string[];
  platforms: string[];
  ad_set_names: string[];
  ad_count: number;
  spend: number;
  impressions: number;
  clicks: number;
  conversions: number;
  engagements: number;
  video_views: number;
  video_completions: number;
  cpm: number | null;
  cpc: number | null;
  ctr: number | null;
  vcr: number | null;
  engagement_rate: number | null;
}

export interface CreativeVariantResponse {
  project_code: string;
  start_date: string | null;
  end_date: string | null;
  creatives: CreativeVariantRow[];
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

/**
 * One row in project_media_plans, optionally enriched with the most-recent
 * media_plans sync status. Used by the admin Plans section.
 */
export interface ProjectPlan {
  sheet_id: string;
  phase_label: string | null;
  display_order: number | null;
  is_active: boolean;
  created_at: string | null;
  last_synced_at: string | null;
  line_count: number;
}

export interface ProjectPlansResponse {
  project_code: string;
  plans: ProjectPlan[];
}

export interface ProjectPlanMutationResponse {
  status: string;
  project_code: string;
  sheet_id: string;
  plans: ProjectPlan[];
  sync_result?: { status: string; message?: string; lines_created?: number } | null;
}

export interface SyncAllResponse {
  project_code: string;
  sheets_attempted: number;
  sheets_succeeded: number;
  sheets_failed: number;
  results: Array<{
    sheet_id: string;
    phase_label?: string | null;
    status: string;
    message?: string;
    lines_created?: number;
  }>;
}

export interface AdminProject extends Project {
  client_id: string | null;
  campaign_type: string | null;
  currency: string;
  platforms_active: number;
  first_data_date: string | null;
  last_data_date: string | null;
  media_plan_sheet_id: string | null;
  media_plan_tab_name: string | null;
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
  media_plan_tab_name?: string;
  slack_channel_id?: string;
}

export interface ProjectCreateResponse {
  status: string;
  project_code: string;
  client_id: string;
  media_plan_sync: {
    status: "success" | "error" | "skipped";
    message?: string;
    lines_created?: number;
  };
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

/* ── FFS (Form Friction Score) types ── */

export interface FFSInputs {
  field_count: number;
  required_fields: number;
  field_types: string[];
  clicks_to_submit: number;
  below_fold_mobile: boolean;
  has_autofill: boolean;
  is_platform_form: boolean;
}

export interface FFSEntry {
  entry_id: string;
  project_code: string;
  label: string | null;
  lp_url: string | null;
  is_platform_form: boolean;
  platform_id: string | null;
  ffs_inputs: FFSInputs;
  ffs_score: number;
  created_at: string | null;
  updated_at: string | null;
  created_by: string | null;
  linked_line_count: number;
  linked_line_ids: string[];
}

export interface FFSEntryCreatePayload {
  label?: string | null;
  lp_url?: string | null;
  is_platform_form: boolean;
  platform_id?: string | null;
  ffs_inputs: FFSInputs;
  applied_line_ids?: string[];
}

export interface FFSEntryUpdatePayload {
  label?: string | null;
  lp_url?: string | null;
  is_platform_form?: boolean | null;
  platform_id?: string | null;
  ffs_inputs?: FFSInputs | null;
}

export interface FFSApplyResponse {
  entry_id: string;
  linked_line_ids: string[];
  added: string[];
  removed: string[];
}

export interface OrphanPlatformSpend {
  platform_id: string;
  spend: number;
  row_count: number;
}

export interface OrphanProject {
  project_code: string;
  total_spend: number;
  total_rows: number;
  first_date: string | null;
  last_date: string | null;
  by_platform: OrphanPlatformSpend[];
  dismissed: boolean;
  dismissed_at: string | null;
  dismissed_by: string | null;
  dismissed_reason: string | null;
}

export interface OrphanListResponse {
  orphans: OrphanProject[];
  count: number;
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

/* ── Diagnostics ── */

export type DiagnosticStatus = "STRONG" | "WATCH" | "ACTION" | null;

export interface DiagnosticSignal {
  id: string;
  name: string;
  score: number | null;
  status: DiagnosticStatus;
  raw_value: number | null;
  benchmark: number | null;
  floor: number | null;
  diagnostic: string;
  guard_passed: boolean;
  guard_reason: string | null;
  inputs: Record<string, unknown>;
}

export interface DiagnosticPillar {
  score: number | null;
  status: DiagnosticStatus;
}

export interface DiagnosticEfficiency {
  cpm: number | null;
  cpc: number | null;
  cpa: number | null;
  cpcv: number | null;
  pacing_pct: number | null;
}

export interface DiagnosticAlert {
  type: string;
  severity: "critical" | "warning" | "info";
  message: string;
  signal_id: string | null;
}

export interface DiagnosticOutput {
  id: string;
  project_code: string;
  campaign_type: "persuasion" | "conversion";
  evaluation_date: string;
  flight_day: number;
  flight_total_days: number;
  health_score: number | null;
  health_status: DiagnosticStatus;
  pillars: Record<string, DiagnosticPillar>;
  signals: DiagnosticSignal[];
  efficiency: DiagnosticEfficiency;
  alerts: DiagnosticAlert[];
  platforms: string[];
  line_ids: string[];
  computed_at: string;
  spec_version: string;
}

export interface DiagnosticHistoryPoint {
  evaluation_date: string;
  campaign_type: string;
  health_score: number | null;
  health_status: DiagnosticStatus;
  pillars: Record<string, DiagnosticPillar>;
}

export interface DiagnosticRunResponse {
  project_code: string;
  status: "success" | "skipped";
  message?: string;
  results: Array<{
    campaign_type: string;
    evaluation_date: string;
    health_score: number | null;
    health_status: string | null;
    alerts: number;
  }>;
}

/* ── Retrospective Mode (ADAC-51) ── */

export interface RetrospectivePacingSummary {
  project_code: string;
  lines_processed: number;
  alerts: number;
}

export interface RetrospectiveResponse {
  project_code: string;
  as_of_date: string; // YYYY-MM-DD
  engine_version: string;
  cached: boolean;
  diagnostics: DiagnosticOutput[];
  pacing: RetrospectivePacingSummary;
}

export const api = {
  projects: {
    list: () => apiFetch<Project[]>("/api/projects/"),
    get: (code: string) => apiFetch<Project>(`/api/projects/${code}`),
  },
  pacing: {
    get: (code: string, asOfDate?: string) =>
      apiFetch<PacingResponse>(
        `/api/pacing/${code}${asOfDate ? `?as_of_date=${asOfDate}` : ""}`
      ),
    history: (code: string, days = 60, asOfDate?: string) => {
      const qs = new URLSearchParams({ days: String(days) });
      if (asOfDate) qs.set("as_of_date", asOfDate);
      return apiFetch<PacingHistoryResponse>(
        `/api/pacing/${code}/history?${qs}`
      );
    },
    run: (code: string) =>
      apiFetch(`/api/pacing/${code}/run`, { method: "POST" }),
  },
  performance: {
    get: (code: string, days?: number) =>
      apiFetch<PerformanceResponse>(
        `/api/performance/${code}${days ? `?days=${days}` : ""}`
      ),
    adsets: (code: string, days?: number) =>
      apiFetch<AdSetPerformanceResponse>(
        `/api/performance/${code}/adsets${days ? `?days=${days}` : ""}`
      ),
    ads: (code: string, days?: number) =>
      apiFetch<AdPerformanceResponse>(
        `/api/performance/${code}/ads${days ? `?days=${days}` : ""}`
      ),
    creatives: (code: string, days?: number) =>
      apiFetch<CreativeVariantResponse>(
        `/api/performance/${code}/creatives${days ? `?days=${days}` : ""}`
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
  diagnostics: {
    get: (code: string, date?: string) =>
      apiFetch<DiagnosticOutput[]>(
        `/api/diagnostics/${code}${date ? `?date=${date}` : ""}`
      ),
    history: (code: string, days = 30, campaignType?: string, asOfDate?: string) => {
      const qs = new URLSearchParams({ days: String(days) });
      if (campaignType) qs.set("campaign_type", campaignType);
      if (asOfDate) qs.set("as_of_date", asOfDate);
      return apiFetch<DiagnosticHistoryPoint[]>(
        `/api/diagnostics/${code}/history?${qs}`
      );
    },
    run: (code: string) =>
      apiFetch<DiagnosticRunResponse>(`/api/diagnostics/${code}/run`, {
        method: "POST",
      }),
  },
  retrospective: {
    /**
     * Replay diagnostics + pacing for a past date.
     * `asOfDate` must be ISO YYYY-MM-DD (the format Next.js extracts from
     * the URL path segment on the matching frontend route).
     */
    get: (code: string, asOfDate: string) =>
      apiFetch<RetrospectiveResponse>(
        `/api/diagnostics/as-of/${asOfDate}/project/${code}`
      ),
  },
  ffs: {
    list: (code: string) => apiFetch<FFSEntry[]>(`/api/ffs/${code}`),
    create: (code: string, data: FFSEntryCreatePayload) =>
      apiFetch<FFSEntry>(`/api/ffs/${code}`, {
        method: "POST",
        body: JSON.stringify(data),
      }),
    update: (code: string, entryId: string, data: FFSEntryUpdatePayload) =>
      apiFetch<FFSEntry>(`/api/ffs/${code}/${entryId}`, {
        method: "PATCH",
        body: JSON.stringify(data),
      }),
    delete: (code: string, entryId: string) =>
      apiFetch<{ status: string; entry_id: string }>(`/api/ffs/${code}/${entryId}`, {
        method: "DELETE",
      }),
    apply: (code: string, entryId: string, lineIds: string[]) =>
      apiFetch<FFSApplyResponse>(`/api/ffs/${code}/${entryId}/apply`, {
        method: "POST",
        body: JSON.stringify({ line_ids: lineIds }),
      }),
    setLineOverride: (code: string, lineId: string, ffsInputs: FFSInputs) =>
      apiFetch<Record<string, unknown>>(`/api/ffs/${code}/lines/${lineId}/override`, {
        method: "POST",
        body: JSON.stringify({ ffs_inputs: ffsInputs, clear: false }),
      }),
    clearLineOverride: (code: string, lineId: string) =>
      apiFetch<Record<string, unknown>>(`/api/ffs/${code}/lines/${lineId}/override`, {
        method: "POST",
        body: JSON.stringify({ clear: true }),
      }),
  },
  bundles: {
    /**
     * Lock in a parser-suggested bundle as user-confirmed (ADAC-54 follow-up).
     * Survives subsequent media plan re-syncs via media_plan_bundle_overrides.
     */
    confirm: (projectCode: string, bundleId: string) =>
      apiFetch<{ status: string; project_code: string; bundle_id: string; members_updated: number }>(
        `/api/admin/bundles/${encodeURIComponent(bundleId)}/confirm?project_code=${projectCode}`,
        { method: "POST" }
      ),
    /**
     * Clear any saved override for this bundle, reverting live lines to the
     * parser's "suggested" state. Next sync re-decides from the spreadsheet.
     */
    clearOverride: (projectCode: string, bundleId: string) =>
      apiFetch<{ status: string; project_code: string; bundle_id: string }>(
        `/api/admin/bundles/${encodeURIComponent(bundleId)}/override?project_code=${projectCode}`,
        { method: "DELETE" }
      ),
    /**
     * Mark a parser-suggested bundle as user-rejected. The former parent
     * shows up as a standalone with the pool budget, while children whose
     * budgets were zeroed by the parser fall through pacing's budget<=0
     * skip and disappear from the dashboard. Re-syncs preserve the
     * rejection via media_plan_bundle_overrides.
     */
    reject: (projectCode: string, bundleId: string) =>
      apiFetch<{ status: string; project_code: string; bundle_id: string; members_updated: number }>(
        `/api/admin/bundles/${encodeURIComponent(bundleId)}/reject?project_code=${projectCode}`,
        { method: "POST" }
      ),
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
    syncMediaPlan: (sheetId: string, projectCode: string, tabName?: string) => {
      let url = `/api/admin/sync-media-plan?sheet_id=${encodeURIComponent(sheetId)}&project_code=${encodeURIComponent(projectCode)}`;
      if (tabName) url += `&tab_name=${encodeURIComponent(tabName)}`;
      return apiFetch<Record<string, unknown>>(url, { method: "POST" });
    },
    /**
     * Multi-plan support: list/add/update/remove the media plan sheets
     * registered against a project. The dedup guard joins through
     * project_media_plans (is_active=TRUE) so retired phases drop out
     * of pacing/diagnostics without losing their data for retrospective
     * replay. Backed by /api/admin/projects/{code}/plans.
     */
    plans: {
      list: (projectCode: string) =>
        apiFetch<ProjectPlansResponse>(
          `/api/admin/projects/${encodeURIComponent(projectCode)}/plans`,
        ),
      add: (
        projectCode: string,
        data: {
          sheet_url_or_id: string;
          phase_label?: string | null;
          display_order?: number | null;
          auto_sync?: boolean;
        },
      ) =>
        apiFetch<ProjectPlanMutationResponse>(
          `/api/admin/projects/${encodeURIComponent(projectCode)}/plans`,
          { method: "POST", body: JSON.stringify(data) },
        ),
      update: (
        projectCode: string,
        sheetId: string,
        data: { phase_label?: string | null; display_order?: number | null; is_active?: boolean },
      ) =>
        apiFetch<ProjectPlanMutationResponse>(
          `/api/admin/projects/${encodeURIComponent(projectCode)}/plans/${encodeURIComponent(sheetId)}`,
          { method: "PUT", body: JSON.stringify(data) },
        ),
      remove: (projectCode: string, sheetId: string, hard = false) =>
        apiFetch<ProjectPlanMutationResponse>(
          `/api/admin/projects/${encodeURIComponent(projectCode)}/plans/${encodeURIComponent(sheetId)}${hard ? "?hard=true" : ""}`,
          { method: "DELETE" },
        ),
      syncAll: (projectCode: string) =>
        apiFetch<SyncAllResponse>(
          `/api/admin/projects/${encodeURIComponent(projectCode)}/sync-all`,
          { method: "POST" },
        ),
    },
    runTransformation: (mode = "daily") =>
      apiFetch<Record<string, unknown>>(
        `/api/admin/run-transformation?mode=${mode}`,
        { method: "POST" }
      ),
    runAdsetTransformation: (mode = "daily") =>
      apiFetch<Record<string, unknown>>(
        `/api/admin/run-adset-transformation?mode=${mode}`,
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
    updateMediaPlanLine: (lineId: string, data: { audience_name: string }) =>
      apiFetch<Record<string, unknown>>(`/api/admin/media-plan-lines/${encodeURIComponent(lineId)}`, {
        method: "PUT",
        body: JSON.stringify(data),
      }),
    createCreativeAlias: (data: {
      project_code: string;
      ad_name_pattern: string;
      creative_variant: string;
      platform_id?: string;
    }) =>
      apiFetch<Record<string, unknown>>("/api/admin/creative-aliases", {
        method: "POST",
        body: JSON.stringify(data),
      }),
    deleteCreativeAlias: (aliasId: string) =>
      apiFetch<Record<string, unknown>>(`/api/admin/creative-aliases/${encodeURIComponent(aliasId)}`, {
        method: "DELETE",
      }),
  },
  orphans: {
    list: (includeDismissed = false) =>
      apiFetch<OrphanListResponse>(
        `/api/orphan-projects${includeDismissed ? "?include_dismissed=true" : ""}`
      ),
    dismiss: (projectCode: string, reason?: string) =>
      apiFetch<OrphanProject>(`/api/orphan-projects/${projectCode}/dismiss`, {
        method: "POST",
        body: JSON.stringify({ reason: reason ?? null }),
      }),
    undismiss: (projectCode: string) =>
      apiFetch<{ status: string; project_code: string }>(
        `/api/orphan-projects/${projectCode}/undismiss`,
        { method: "POST" }
      ),
  },
};
