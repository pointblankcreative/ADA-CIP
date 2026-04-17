"use client";

import { useEffect, useRef, useState } from "react";
import {
  AreaChart,
  Area,
  LineChart,
  Line,
  BarChart,
  Bar,
  ComposedChart,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Cell,
} from "recharts";
import {
  api,
  type PerformanceResponse,
  type ObjectiveType,
  type GA4PerformanceResponse,
  type BenchmarkResponse,
  type BenchmarkValue,
  type AdSetPerformanceResponse,
  type AdPerformanceResponse,
  type CreativeVariantResponse,
  type CreativeVariantRow,
} from "@/lib/api";
import { Card, KpiCard, type BenchmarkIndicator } from "@/components/card";
import { CampaignTable } from "@/components/campaign-table";
import { AdSetDrillDown } from "@/components/adset-drilldown";
import { AdDrillDown } from "@/components/ad-drilldown";
import {
  formatCurrency,
  formatNumber,
  formatPercent,
  platformLabel,
  cn,
} from "@/lib/utils";

const RANGE_OPTIONS = [
  { label: "7d", days: 7 },
  { label: "14d", days: 14 },
  { label: "30d", days: 30 },
  { label: "All", days: 0 },
];

const PLATFORM_COLORS: Record<string, string> = {
  meta: "#3b82f6",
  google_ads: "#22c55e",
  stackadapt: "#a855f7",
  linkedin: "#0ea5e9",
  tiktok: "#f43f5e",
  snapchat: "#eab308",
  perion: "#f97316",
  reddit: "#ff4500",
  pinterest: "#e60023",
};

const OBJECTIVE_BADGE: Record<ObjectiveType, { label: string; cls: string }> = {
  awareness: { label: "Awareness", cls: "bg-purple-500/20 text-purple-400 border-purple-500/30" },
  conversion: { label: "Conversion", cls: "bg-emerald-500/20 text-emerald-400 border-emerald-500/30" },
  mixed: { label: "Mixed", cls: "bg-blue-500/20 text-blue-400 border-blue-500/30" },
};

const TOOLTIP_STYLE = {
  background: "#1e293b",
  border: "1px solid #334155",
  borderRadius: "0.5rem",
  fontSize: "0.75rem",
  color: "#e2e8f0",
};

function has(data: PerformanceResponse, metric: string): boolean {
  return data.available_metrics.includes(metric);
}

function metricNote(data: PerformanceResponse, metric: string): string | undefined {
  const platforms = data.metric_platforms[metric];
  if (!platforms || platforms.length === 0) return undefined;
  if (platforms.length === data.by_platform?.length) return undefined;
  return `Based on ${platforms.join(", ")} data`;
}

function toBenchmark(
  bv: BenchmarkValue | undefined,
  current: number,
  opts?: { lowerIsBetter?: boolean; format?: (v: number) => string },
): BenchmarkIndicator | undefined {
  if (!bv || bv.p25 == null || bv.p50 == null || bv.p75 == null) return undefined;
  return {
    p25: bv.p25,
    p50: bv.p50,
    p75: bv.p75,
    current,
    lowerIsBetter: opts?.lowerIsBetter,
    format: opts?.format,
  };
}

const fmtPct = (v: number | null): string | null => v != null ? `${(v * 100).toFixed(1)}%` : null;
const fmtCad = (v: number | null): string | null => v != null ? `$${v.toFixed(2)}` : null;
const safeFix = (v: number | null, d = 0): string | null => v != null ? v.toFixed(d) : null;

function freqHealthDot(f: number | null | undefined): string | null {
  if (f == null || f <= 0) return null;
  if (f <= 3) return "bg-emerald-500";
  if (f <= 5) return "bg-amber-400";
  return "bg-red-500";
}

export function PerformanceTab({ code }: { code: string }) {
  const [data, setData] = useState<PerformanceResponse | null>(null);
  const [ga4Data, setGa4Data] = useState<GA4PerformanceResponse | null>(null);
  const [benchData, setBenchData] = useState<BenchmarkResponse | null>(null);
  const [adsetsData, setAdsetsData] = useState<AdSetPerformanceResponse | null>(null);
  const [adsData, setAdsData] = useState<AdPerformanceResponse | null>(null);
  const [creativesData, setCreativesData] = useState<CreativeVariantResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [days, setDays] = useState(0);

  // Simple request cache: keyed by `${code}-${days}`
  const cacheRef = useRef<Record<string, {
    perf: PerformanceResponse | null;
    ga4: GA4PerformanceResponse | null;
    bench: BenchmarkResponse | null;
    adsets: AdSetPerformanceResponse | null;
    ads: AdPerformanceResponse | null;
    creatives: CreativeVariantResponse | null;
  }>>({});

  useEffect(() => {
    // Clear cache when project changes
    cacheRef.current = {};
  }, [code]);

  useEffect(() => {
    const cacheKey = `${code}-${days}`;
    const cached = cacheRef.current[cacheKey];

    if (cached) {
      // Use cached data
      setData(cached.perf);
      setGa4Data(cached.ga4);
      setBenchData(cached.bench);
      setAdsetsData(cached.adsets);
      setAdsData(cached.ads);
      setCreativesData(cached.creatives);
      setLoading(false);
      return;
    }

    setLoading(true);
    Promise.all([
      api.performance.get(code, days).catch(() => null),
      api.ga4.analytics(code, days).catch(() => null),
      api.benchmarks.get(code).catch(() => null),
      api.performance.adsets(code, days).catch(() => null),
      api.performance.ads(code, days).catch(() => null),
      api.performance.creatives(code, days).catch(() => null),
    ]).then(([perf, ga4, bench, adsets, ads, creatives]) => {
      // Cache the results
      cacheRef.current[cacheKey] = { perf, ga4, bench, adsets, ads, creatives };

      setData(perf);
      setGa4Data(ga4);
      setBenchData(bench);
      setAdsetsData(adsets);
      setAdsData(ads);
      setCreativesData(creatives);
    }).finally(() => setLoading(false));
  }, [code, days]);

  if (loading) {
    return (
      <div className="grid grid-cols-2 gap-4 lg:grid-cols-4">
        {Array.from({ length: 5 }).map((_, i) => (
          <Card key={i} className="animate-pulse">
            <div className="h-3 w-20 rounded bg-slate-700" />
            <div className="mt-3 h-7 w-28 rounded bg-slate-700" />
          </Card>
        ))}
      </div>
    );
  }

  if (!data) {
    return (
      <Card>
        <p className="text-slate-400">No performance data available.</p>
      </Card>
    );
  }

  const objective = data.objective_type ?? "mixed";
  const badge = OBJECTIVE_BADGE[objective];
  const showAwareness = objective === "awareness" || objective === "mixed";
  const showConversion = objective === "conversion" || objective === "mixed";

  const avgCPM =
    data.total_impressions > 0
      ? (data.total_spend / data.total_impressions) * 1000
      : 0;
  const avgCTR =
    data.total_impressions > 0
      ? (data.total_clicks / data.total_impressions) * 100
      : 0;
  const engagementRate =
    has(data, "engagements") && data.total_impressions > 0 && data.total_engagements
      ? (data.total_engagements / data.total_impressions) * 100
      : null;

  const bm = benchData?.benchmarks ?? {};

  const hasAdsetReachSeries = data.daily.some(
    (d) => d.reach_adset != null && d.reach_adset > 0,
  );

  const chartData = data.daily.map((d) => ({
    ...d,
    dateLabel: d.date.slice(5),
    ctrPct: d.ctr != null ? d.ctr * 100 : null,
    convRatePct: d.conversion_rate != null ? d.conversion_rate * 100 : null,
    vcrPct: d.vcr != null ? d.vcr * 100 : null,
    reach: d.reach_adset ?? d.reach ?? null,
    frequency: d.frequency_adset ?? d.frequency ?? null,
  }));

  return (
    <div className="space-y-6">
      {/* Header: date range + objective badge */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-3">
          <h3 className="text-sm font-semibold uppercase tracking-wider text-slate-500">
            {data.start_date} — {data.end_date}
          </h3>
          <span
            className={cn(
              "rounded-full border px-2.5 py-0.5 text-xs font-medium",
              badge.cls
            )}
          >
            {badge.label}
          </span>
        </div>
        <div className="flex gap-1 rounded-md border border-slate-800 bg-surface-raised p-0.5">
          {RANGE_OPTIONS.map((opt) => (
            <button
              key={opt.days}
              onClick={() => setDays(opt.days)}
              className={cn(
                "rounded px-3 py-1 text-xs font-medium transition-colors",
                days === opt.days
                  ? "bg-brand-600/20 text-brand-400"
                  : "text-slate-500 hover:text-slate-300"
              )}
            >
              {opt.label}
            </button>
          ))}
        </div>
      </div>

      {data.high_frequency_warning && (
        <div className="rounded-md border border-amber-500/40 bg-amber-500/10 px-4 py-3 text-sm text-amber-200">
          <span className="font-medium">High frequency — </span>
          {data.high_frequency_warning}
        </div>
      )}

      {/* ── KPI Cards ─────────────────────────────────────────────── */}

      {showAwareness && (
        <div>
          {objective === "mixed" && (
            <h4 className="mb-3 text-xs font-semibold uppercase tracking-wider text-purple-400">
              Awareness Metrics
            </h4>
          )}
          <div className="grid grid-cols-2 gap-4 lg:grid-cols-5">
            <KpiCard label="Spend" value={formatCurrency(data.total_spend)} />
            {data.total_reach_adset != null && data.total_reach_adset > 0 ? (
              <KpiCard
                label="Reach"
                value={formatNumber(data.total_reach_adset)}
                sub={data.reach_note ?? "From fact_adset_daily"}
              />
            ) : has(data, "reach") && data.total_reach ? (
              <KpiCard
                label="Reach"
                value={formatNumber(data.total_reach)}
                sub={metricNote(data, "reach")}
              />
            ) : (
              <KpiCard
                label="Impressions"
                value={formatNumber(data.total_impressions)}
              />
            )}
            {data.avg_frequency_adset != null && data.avg_frequency_adset > 0 ? (
              <KpiCard
                label="Frequency"
                value={data.avg_frequency_adset.toFixed(1)}
                sub={data.reach_note ?? undefined}
                benchmark={toBenchmark(bm.frequency, data.avg_frequency_adset, { format: (v) => v.toFixed(1) })}
              />
            ) : has(data, "frequency") && data.total_frequency ? (
              <KpiCard
                label="Frequency"
                value={data.total_frequency.toFixed(1)}
                sub={metricNote(data, "frequency")}
                benchmark={toBenchmark(bm.frequency, data.total_frequency, { format: (v) => v.toFixed(1) })}
              />
            ) : (
              <KpiCard
                label="CPM"
                value={`$${avgCPM.toFixed(2)}`}
                benchmark={toBenchmark(bm.cpm, avgCPM, { lowerIsBetter: true, format: (v) => fmtCad(v) ?? "—" })}
              />
            )}
            {has(data, "vcr") && data.total_vcr != null ? (
              <KpiCard
                label="Video Completion Rate"
                value={formatPercent(data.total_vcr * 100)}
                sub={metricNote(data, "video_views")}
                benchmark={toBenchmark(bm.vcr, data.total_vcr, { format: (v) => fmtPct(v) ?? "—" })}
              />
            ) : (
              <KpiCard
                label="CTR"
                value={formatPercent(avgCTR)}
                benchmark={toBenchmark(bm.ctr, avgCTR / 100, { format: (v) => fmtPct(v) ?? "—" })}
              />
            )}
            {engagementRate != null ? (
              <KpiCard
                label="Engagement Rate"
                value={formatPercent(engagementRate)}
                sub={metricNote(data, "engagements")}
              />
            ) : (
              <KpiCard
                label="Clicks"
                value={formatNumber(data.total_clicks)}
              />
            )}
          </div>
        </div>
      )}

      {showConversion && (
        <div>
          {objective === "mixed" && (
            <h4 className="mb-3 text-xs font-semibold uppercase tracking-wider text-emerald-400">
              Conversion Metrics
            </h4>
          )}
          <div className="grid grid-cols-2 gap-4 lg:grid-cols-5">
            {objective !== "mixed" && (
              <KpiCard label="Spend" value={formatCurrency(data.total_spend)} />
            )}
            {has(data, "conversions") && (
              <KpiCard
                label="Conversions"
                value={formatNumber(Math.round(data.total_conversions))}
                sub={metricNote(data, "conversions")}
              />
            )}
            {has(data, "cpa") && data.total_cpa != null ? (
              <KpiCard
                label="CPA"
                value={formatCurrency(data.total_cpa)}
                sub="Cost per acquisition"
                benchmark={toBenchmark(bm.cpa, data.total_cpa, { lowerIsBetter: true, format: (v) => fmtCad(v) ?? "—" })}
              />
            ) : null}
            <KpiCard
              label="CTR"
              value={formatPercent(avgCTR)}
              benchmark={toBenchmark(bm.ctr, avgCTR / 100, { format: (v) => fmtPct(v) ?? "—" })}
            />
            {has(data, "conversion_rate") && data.total_conversion_rate != null ? (
              <KpiCard
                label="Conv. Rate"
                value={formatPercent(data.total_conversion_rate * 100)}
                benchmark={toBenchmark(bm.conversion_rate, data.total_conversion_rate, { format: (v) => fmtPct(v) ?? "—" })}
              />
            ) : (
              <KpiCard
                label="CPC"
                value={`$${data.total_clicks > 0 ? (data.total_spend / data.total_clicks).toFixed(2) : "0.00"}`}
                benchmark={toBenchmark(bm.cpc, data.total_clicks > 0 ? data.total_spend / data.total_clicks : 0, { lowerIsBetter: true, format: (v) => fmtCad(v) ?? "—" })}
              />
            )}
          </div>
        </div>
      )}

      {/* ── Daily Spend Chart (always shown) ──────────────────────── */}
      <Card>
        <h4 className="mb-4 text-sm font-medium text-slate-400">Daily Spend</h4>
        <div className="h-64">
          <ResponsiveContainer width="100%" height="100%">
            <AreaChart data={chartData}>
              <defs>
                <linearGradient id="spendGrad" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%" stopColor="#3b82f6" stopOpacity={0.3} />
                  <stop offset="95%" stopColor="#3b82f6" stopOpacity={0} />
                </linearGradient>
              </defs>
              <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" vertical={false} />
              <XAxis dataKey="dateLabel" stroke="#475569" fontSize={11} tickLine={false} />
              <YAxis stroke="#475569" fontSize={11} tickLine={false} axisLine={false} tickFormatter={(v) => `$${((v ?? 0) / 1000).toFixed(0)}k`} />
              <Tooltip contentStyle={TOOLTIP_STYLE} formatter={(v: number) => [formatCurrency(v), "Spend"]} />
              <Area type="monotone" dataKey="spend" stroke="#3b82f6" strokeWidth={2} fill="url(#spendGrad)" />
            </AreaChart>
          </ResponsiveContainer>
        </div>
      </Card>

      {/* ── Awareness Charts ──────────────────────────────────────── */}
      {showAwareness && (has(data, "reach") || hasAdsetReachSeries) && (
        <Card>
          <h4 className="mb-4 text-sm font-medium text-slate-400">
            Reach &amp; Frequency
            {(data.reach_note || metricNote(data, "reach")) && (
              <span className="ml-2 text-xs font-normal text-slate-600">
                {data.reach_note ?? metricNote(data, "reach")}
              </span>
            )}
          </h4>
          <div className="h-64">
            <ResponsiveContainer width="100%" height="100%">
              <ComposedChart data={chartData}>
                <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" vertical={false} />
                <XAxis dataKey="dateLabel" stroke="#475569" fontSize={11} tickLine={false} />
                <YAxis yAxisId="left" stroke="#475569" fontSize={11} tickLine={false} axisLine={false} tickFormatter={(v) => formatNumber(v)} />
                <YAxis yAxisId="right" orientation="right" stroke="#475569" fontSize={11} tickLine={false} axisLine={false} domain={[0, "auto"]} />
                <Tooltip contentStyle={TOOLTIP_STYLE} />
                <Bar yAxisId="left" dataKey="reach" fill="#a855f7" opacity={0.4} radius={[2, 2, 0, 0]} name="Reach" />
                <Line yAxisId="right" type="monotone" dataKey="frequency" stroke="#f59e0b" strokeWidth={2} dot={false} name="Frequency" />
              </ComposedChart>
            </ResponsiveContainer>
          </div>
        </Card>
      )}

      {showAwareness && has(data, "vcr") && (
        <Card>
          <h4 className="mb-4 text-sm font-medium text-slate-400">
            Video Completion Rate
            {metricNote(data, "video_views") && (
              <span className="ml-2 text-xs font-normal text-slate-600">{metricNote(data, "video_views")}</span>
            )}
          </h4>
          <div className="h-56">
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={chartData}>
                <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" vertical={false} />
                <XAxis dataKey="dateLabel" stroke="#475569" fontSize={11} tickLine={false} />
                <YAxis stroke="#475569" fontSize={11} tickLine={false} axisLine={false} tickFormatter={(v) => `${safeFix(v) ?? "0"}%`} domain={[0, 100]} />
                <Tooltip contentStyle={TOOLTIP_STYLE} formatter={(v: number) => [`${(v ?? 0).toFixed(1)}%`, "VCR"]} />
                <Line type="monotone" dataKey="vcrPct" stroke="#a855f7" strokeWidth={2} dot={false} name="VCR" />
              </LineChart>
            </ResponsiveContainer>
          </div>
        </Card>
      )}

      {/* ── Conversion Charts ─────────────────────────────────────── */}
      {showConversion && has(data, "cpa") && (
        <Card>
          <h4 className="mb-4 text-sm font-medium text-slate-400">CPA Trend</h4>
          <div className="h-56">
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={chartData}>
                <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" vertical={false} />
                <XAxis dataKey="dateLabel" stroke="#475569" fontSize={11} tickLine={false} />
                <YAxis stroke="#475569" fontSize={11} tickLine={false} axisLine={false} tickFormatter={(v) => `$${safeFix(v) ?? "0"}`} />
                <Tooltip contentStyle={TOOLTIP_STYLE} formatter={(v: number) => [formatCurrency(v), "CPA"]} />
                <Line type="monotone" dataKey="cpa" stroke="#22c55e" strokeWidth={2} dot={false} name="CPA" />
              </LineChart>
            </ResponsiveContainer>
          </div>
        </Card>
      )}

      {showConversion && has(data, "conversions") && (
        <Card>
          <h4 className="mb-4 text-sm font-medium text-slate-400">Conversion Volume &amp; Rate</h4>
          <div className="h-56">
            <ResponsiveContainer width="100%" height="100%">
              <ComposedChart data={chartData}>
                <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" vertical={false} />
                <XAxis dataKey="dateLabel" stroke="#475569" fontSize={11} tickLine={false} />
                <YAxis yAxisId="left" stroke="#475569" fontSize={11} tickLine={false} axisLine={false} />
                <YAxis yAxisId="right" orientation="right" stroke="#475569" fontSize={11} tickLine={false} axisLine={false} tickFormatter={(v) => `${safeFix(v) ?? "0"}%`} />
                <Tooltip contentStyle={TOOLTIP_STYLE} />
                <Bar yAxisId="left" dataKey="conversions" fill="#22c55e" opacity={0.5} radius={[2, 2, 0, 0]} name="Conversions" />
                {has(data, "conversion_rate") && (
                  <Line yAxisId="right" type="monotone" dataKey="convRatePct" stroke="#10b981" strokeWidth={2} dot={false} name="Conv. Rate %" />
                )}
              </ComposedChart>
            </ResponsiveContainer>
          </div>
        </Card>
      )}

      {/* ── Platform Breakdown (always shown) ─────────────────────── */}
      {data.by_platform && data.by_platform.length > 0 && (
        <Card>
          <h4 className="mb-4 text-sm font-medium text-slate-400">Platform Breakdown</h4>
          <div className="h-48">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart
                data={data.by_platform.map((p) => ({
                  ...p,
                  name: platformLabel(p.platform_id),
                }))}
                layout="vertical"
              >
                <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" horizontal={false} />
                <XAxis type="number" stroke="#475569" fontSize={11} tickFormatter={(v) => `$${((v ?? 0) / 1000).toFixed(0)}k`} />
                <YAxis type="category" dataKey="name" stroke="#475569" fontSize={11} width={80} />
                <Tooltip contentStyle={TOOLTIP_STYLE} formatter={(v: number) => [formatCurrency(v), "Spend"]} />
                <Bar dataKey="spend" radius={[0, 4, 4, 0]}>
                  {(data.by_platform ?? []).map((entry) => (
                    <Cell key={entry.platform_id} fill={PLATFORM_COLORS[entry.platform_id] ?? "#64748b"} />
                  ))}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </div>
        </Card>
      )}

      {/* ── Campaign Table ────────────────────────────────────────── */}
      <CampaignTable
        data={data}
        showAwareness={showAwareness}
        showConversion={showConversion}
      />

      {/* ── Audience (ad set) drill-down ─────────────────────────── */}
      {adsetsData && <AdSetDrillDown data={adsetsData} />}

      {/* ── Creative variants (cross-platform) ───────────────────── */}
      {creativesData && creativesData.creatives.length > 0 && (
        <CreativeVariantsTable
          data={creativesData}
          projectCode={code}
          onRename={(oldVariant: string, newVariant: string) => {
            setCreativesData((prev) => {
              if (!prev) return prev;
              return {
                ...prev,
                creatives: prev.creatives.map((row) =>
                  row.creative_variant === oldVariant
                    ? { ...row, creative_variant: newVariant }
                    : row
                ),
              };
            });
          }}
        />
      )}

      {/* ── Creative (ad) drill-down ──────────────────────────────── */}
      {adsData && <AdDrillDown data={adsData} />}

      {/* ── GA4 Web Analytics Section ─────────────────────────────── */}
      {ga4Data?.has_ga4 && ga4Data.daily.length > 0 && (
        <>
          <div className="border-t border-slate-800 pt-6">
            <h3 className="text-sm font-semibold uppercase tracking-wider text-slate-500 mb-4">
              Web Analytics (GA4)
            </h3>
          </div>

          <div className="grid grid-cols-2 gap-4 lg:grid-cols-4">
            <KpiCard label="Sessions" value={formatNumber(ga4Data.total_sessions)} />
            <KpiCard label="GA4 Conversions" value={formatNumber(ga4Data.total_conversions)} />
            {ga4Data.avg_bounce_rate != null && (
              <KpiCard label="Bounce Rate" value={formatPercent(ga4Data.avg_bounce_rate * 100)} />
            )}
            {ga4Data.avg_session_duration != null && (
              <KpiCard label="Avg Session" value={`${Math.round(ga4Data.avg_session_duration)}s`} />
            )}
          </div>

          <Card>
            <h4 className="mb-4 text-sm font-medium text-slate-400">Sessions &amp; GA4 Conversions</h4>
            <div className="h-56">
              <ResponsiveContainer width="100%" height="100%">
                <ComposedChart data={ga4Data.daily.map((d) => ({ ...d, dateLabel: d.date.slice(5) }))}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" vertical={false} />
                  <XAxis dataKey="dateLabel" stroke="#475569" fontSize={11} tickLine={false} />
                  <YAxis yAxisId="left" stroke="#475569" fontSize={11} tickLine={false} axisLine={false} />
                  <YAxis yAxisId="right" orientation="right" stroke="#475569" fontSize={11} tickLine={false} axisLine={false} />
                  <Tooltip contentStyle={TOOLTIP_STYLE} />
                  <Bar yAxisId="left" dataKey="sessions" fill="#6366f1" opacity={0.4} radius={[2, 2, 0, 0]} name="Sessions" />
                  <Line yAxisId="right" type="monotone" dataKey="conversions" stroke="#22c55e" strokeWidth={2} dot={false} name="GA4 Conversions" />
                </ComposedChart>
              </ResponsiveContainer>
            </div>
          </Card>
        </>
      )}
    </div>
  );
}


/* ── Creative Variants Table Component ────────────────────────────── */

function CreativeVariantsTable({
  data,
  projectCode,
  onRename,
}: {
  data: CreativeVariantResponse;
  projectCode: string;
  onRename: (oldVariant: string, newVariant: string) => void;
}) {
  const [expanded, setExpanded] = useState<Set<string>>(new Set());
  const [collapsed, setCollapsed] = useState(false);
  const [renaming, setRenaming] = useState<string | null>(null);
  const [renameValue, setRenameValue] = useState("");

  const toggleExpand = (variant: string) => {
    setExpanded((prev) => {
      const next = new Set(prev);
      if (next.has(variant)) next.delete(variant);
      else next.add(variant);
      return next;
    });
  };

  const handleRename = async (row: CreativeVariantRow) => {
    const trimmed = renameValue.trim();
    if (!trimmed || trimmed === row.creative_variant) {
      setRenaming(null);
      return;
    }
    try {
      for (const adName of row.ad_names) {
        await api.admin.createCreativeAlias({
          project_code: projectCode,
          ad_name_pattern: adName,
          creative_variant: trimmed,
        });
      }
      // Optimistic update — reflect new name immediately
      const oldVariant = row.creative_variant;
      onRename(oldVariant, trimmed);
      // Track the new variant name in expanded state
      setExpanded((prev) => {
        if (!prev.has(oldVariant)) return prev;
        const next = new Set(prev);
        next.delete(oldVariant);
        next.add(trimmed);
        return next;
      });
      setRenaming(null);
    } catch {
      setRenaming(null);
    }
  };

  return (
    <Card className="overflow-hidden !p-0">
      <button
        onClick={() => setCollapsed(!collapsed)}
        className="flex w-full items-center justify-between px-5 py-4 border-b border-slate-800 hover:bg-slate-800/30 transition-colors"
      >
        <h4 className="text-sm font-medium text-slate-400">
          Creative variants
          <span className="ml-2 text-slate-600">
            ({data.creatives.length})
          </span>
        </h4>
        <svg
          xmlns="http://www.w3.org/2000/svg"
          viewBox="0 0 16 16"
          fill="currentColor"
          className={cn(
            "h-4 w-4 text-slate-600 transition-transform",
            collapsed && "-rotate-90"
          )}
        >
          <path
            fillRule="evenodd"
            d="M4.22 6.22a.75.75 0 0 1 1.06 0L8 8.94l2.72-2.72a.75.75 0 1 1 1.06 1.06l-3.25 3.25a.75.75 0 0 1-1.06 0L4.22 7.28a.75.75 0 0 1 0-1.06Z"
          />
        </svg>
      </button>

      {!collapsed && (
        <div className="overflow-x-auto">
          <table className="w-full text-left text-sm">
            <thead>
              <tr className="border-t border-slate-800 text-xs uppercase tracking-wider text-slate-500">
                <th className="px-5 py-3 font-medium">Creative Variant</th>
                <th className="px-5 py-3 font-medium">Platforms</th>
                <th className="px-5 py-3 font-medium text-right">Ad Sets</th>
                <th className="px-5 py-3 font-medium text-right">Ads</th>
                <th className="px-5 py-3 font-medium text-right">Spend</th>
                <th className="px-5 py-3 font-medium text-right">CTR</th>
                <th className="px-5 py-3 font-medium text-right">Eng. rate</th>
                <th className="px-5 py-3 font-medium text-right">VCR</th>
              </tr>
            </thead>
            <tbody>
              {data.creatives.map((row) => {
                const isExpanded = expanded.has(row.creative_variant);
                const isRenaming = renaming === row.creative_variant;

                return (
                  <VariantRowGroup
                    key={row.creative_variant}
                    row={row}
                    isExpanded={isExpanded}
                    isRenaming={isRenaming}
                    renameValue={renameValue}
                    onToggle={() => toggleExpand(row.creative_variant)}
                    onStartRename={() => {
                      setRenaming(row.creative_variant);
                      setRenameValue(row.creative_variant);
                    }}
                    onRenameChange={setRenameValue}
                    onRenameSubmit={() => handleRename(row)}
                    onRenameCancel={() => setRenaming(null)}
                  />
                );
              })}
            </tbody>
          </table>
        </div>
      )}
    </Card>
  );
}

function VariantRowGroup({
  row,
  isExpanded,
  isRenaming,
  renameValue,
  onToggle,
  onStartRename,
  onRenameChange,
  onRenameSubmit,
  onRenameCancel,
}: {
  row: CreativeVariantRow;
  isExpanded: boolean;
  isRenaming: boolean;
  renameValue: string;
  onToggle: () => void;
  onStartRename: () => void;
  onRenameChange: (v: string) => void;
  onRenameSubmit: () => void;
  onRenameCancel: () => void;
}) {
  return (
    <>
      <tr
        className="border-t border-slate-800/50 hover:bg-slate-800/30 cursor-pointer"
        onClick={onToggle}
      >
        <td className="px-5 py-3">
          <div className="flex items-center gap-2">
            <svg
              xmlns="http://www.w3.org/2000/svg"
              viewBox="0 0 16 16"
              fill="currentColor"
              className={cn(
                "h-3 w-3 text-slate-600 transition-transform flex-shrink-0",
                isExpanded && "rotate-90"
              )}
            >
              <path d="M6.22 4.22a.75.75 0 0 1 1.06 0l3.25 3.25a.75.75 0 0 1 0 1.06l-3.25 3.25a.75.75 0 0 1-1.06-1.06L8.94 8 6.22 5.28a.75.75 0 0 1 0-1.06Z" />
            </svg>
            {isRenaming ? (
              <input
                autoFocus
                value={renameValue}
                onChange={(e) => onRenameChange(e.target.value)}
                onBlur={onRenameSubmit}
                onKeyDown={(e) => {
                  if (e.key === "Enter") onRenameSubmit();
                  if (e.key === "Escape") onRenameCancel();
                }}
                onClick={(e) => e.stopPropagation()}
                className="rounded border border-slate-600 bg-slate-800 px-2 py-0.5 text-sm text-blue-300 outline-none focus:border-blue-500 w-64"
              />
            ) : (
              <>
                <span className="text-slate-200 truncate max-w-[250px]">
                  {row.creative_variant}
                </span>
                <button
                  onClick={(e) => {
                    e.stopPropagation();
                    onStartRename();
                  }}
                  className="text-slate-600 hover:text-slate-400 transition-colors flex-shrink-0"
                  title="Rename variant"
                >
                  <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 16 16" fill="currentColor" className="h-3 w-3">
                    <path d="M13.488 2.513a1.75 1.75 0 0 0-2.475 0L3.22 10.306a1 1 0 0 0-.258.42l-.97 3.232a.5.5 0 0 0 .616.617l3.232-.97a1 1 0 0 0 .42-.258l7.793-7.793a1.75 1.75 0 0 0 0-2.475l-.565-.566Z" />
                  </svg>
                </button>
              </>
            )}
          </div>
        </td>
        <td className="px-5 py-3">
          <div className="flex flex-wrap gap-1">
            {row.platforms.map((p) => (
              <span
                key={p}
                className="inline-block rounded-full px-2 py-0.5 text-[10px] font-medium"
                style={{
                  backgroundColor: `${PLATFORM_COLORS[p] || "#64748b"}20`,
                  color: PLATFORM_COLORS[p] || "#94a3b8",
                }}
              >
                {platformLabel(p)}
              </span>
            ))}
          </div>
        </td>
        <td className="px-5 py-3 text-right tabular-nums text-slate-400">
          {row.ad_set_names.length}
        </td>
        <td className="px-5 py-3 text-right tabular-nums text-slate-400">
          {row.ad_count}
        </td>
        <td className="px-5 py-3 text-right tabular-nums text-slate-300">
          {formatCurrency(row.spend)}
        </td>
        <td className="px-5 py-3 text-right tabular-nums text-slate-400">
          {row.ctr != null ? formatPercent(row.ctr * 100) : "—"}
        </td>
        <td className="px-5 py-3 text-right tabular-nums text-slate-400">
          {row.engagement_rate != null ? formatPercent(row.engagement_rate * 100) : "—"}
        </td>
        <td className="px-5 py-3 text-right tabular-nums text-slate-400">
          {row.vcr != null ? formatPercent(row.vcr * 100) : "—"}
        </td>
      </tr>

      {/* Expanded detail: show original ad names, ad sets, and platforms */}
      {isExpanded && (
        <>
          {row.ad_names.map((adName, i) => (
            <tr
              key={`${row.creative_variant}-detail-${i}`}
              className="bg-slate-900/50 border-t border-slate-800/30"
            >
              <td className="pl-12 pr-5 py-2 text-xs text-slate-500 truncate max-w-[300px]" colSpan={2}>
                {adName}
              </td>
              <td colSpan={6} />
            </tr>
          ))}
          {row.ad_set_names.length > 0 && (
            <tr className="bg-slate-900/50 border-t border-slate-800/30">
              <td className="pl-12 pr-5 py-2 text-xs text-slate-600" colSpan={8}>
                Ad sets: {row.ad_set_names.join(", ")}
              </td>
            </tr>
          )}
        </>
      )}
    </>
  );
}
