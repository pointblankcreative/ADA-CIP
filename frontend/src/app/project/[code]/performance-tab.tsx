"use client";

import { useEffect, useState } from "react";
import {
  AreaChart,
  Area,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  BarChart,
  Bar,
  Cell,
} from "recharts";
import { api, type PerformanceResponse } from "@/lib/api";
import { Card, KpiCard } from "@/components/card";
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
  { label: "All", days: 365 },
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

export function PerformanceTab({ code }: { code: string }) {
  const [data, setData] = useState<PerformanceResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [days, setDays] = useState(7);

  useEffect(() => {
    setLoading(true);
    api.performance
      .get(code, days)
      .then(setData)
      .catch(() => setData(null))
      .finally(() => setLoading(false));
  }, [code, days]);

  if (loading) {
    return (
      <div className="grid grid-cols-4 gap-4">
        {Array.from({ length: 4 }).map((_, i) => (
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

  const avgCPM =
    data.total_impressions > 0
      ? (data.total_spend / data.total_impressions) * 1000
      : 0;
  const avgCPC =
    data.total_clicks > 0 ? data.total_spend / data.total_clicks : 0;
  const ctr =
    data.total_impressions > 0
      ? (data.total_clicks / data.total_impressions) * 100
      : 0;

  const chartData = data.daily.map((d) => ({
    ...d,
    date: d.date.slice(5), // "03-22"
  }));

  return (
    <div className="space-y-6">
      {/* Range selector */}
      <div className="flex items-center justify-between">
        <h3 className="text-sm font-semibold uppercase tracking-wider text-slate-500">
          {data.start_date} — {data.end_date}
        </h3>
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

      {/* KPIs */}
      <div className="grid grid-cols-2 gap-4 lg:grid-cols-5">
        <KpiCard label="Spend" value={formatCurrency(data.total_spend)} />
        <KpiCard
          label="Impressions"
          value={formatNumber(data.total_impressions)}
        />
        <KpiCard label="Clicks" value={formatNumber(data.total_clicks)} />
        <KpiCard label="CPM" value={`$${avgCPM.toFixed(2)}`} />
        <KpiCard label="CTR" value={formatPercent(ctr)} />
      </div>

      {/* Spend chart */}
      <Card>
        <h4 className="mb-4 text-sm font-medium text-slate-400">
          Daily Spend
        </h4>
        <div className="h-64">
          <ResponsiveContainer width="100%" height="100%">
            <AreaChart data={chartData}>
              <defs>
                <linearGradient id="spendGrad" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%" stopColor="#3b82f6" stopOpacity={0.3} />
                  <stop offset="95%" stopColor="#3b82f6" stopOpacity={0} />
                </linearGradient>
              </defs>
              <CartesianGrid
                strokeDasharray="3 3"
                stroke="#1e293b"
                vertical={false}
              />
              <XAxis
                dataKey="date"
                stroke="#475569"
                fontSize={11}
                tickLine={false}
              />
              <YAxis
                stroke="#475569"
                fontSize={11}
                tickLine={false}
                axisLine={false}
                tickFormatter={(v) => `$${(v / 1000).toFixed(0)}k`}
              />
              <Tooltip
                contentStyle={{
                  background: "#1e293b",
                  border: "1px solid #334155",
                  borderRadius: "0.5rem",
                  fontSize: "0.75rem",
                  color: "#e2e8f0",
                }}
                formatter={(v: number) => [formatCurrency(v), "Spend"]}
              />
              <Area
                type="monotone"
                dataKey="spend"
                stroke="#3b82f6"
                strokeWidth={2}
                fill="url(#spendGrad)"
              />
            </AreaChart>
          </ResponsiveContainer>
        </div>
      </Card>

      {/* Platform breakdown */}
      {data.by_platform && data.by_platform.length > 0 && (
        <Card>
          <h4 className="mb-4 text-sm font-medium text-slate-400">
            Platform Breakdown
          </h4>
          <div className="h-48">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart
                data={data.by_platform.map((p) => ({
                  ...p,
                  name: platformLabel(p.platform_id),
                }))}
                layout="vertical"
              >
                <CartesianGrid
                  strokeDasharray="3 3"
                  stroke="#1e293b"
                  horizontal={false}
                />
                <XAxis
                  type="number"
                  stroke="#475569"
                  fontSize={11}
                  tickFormatter={(v) => `$${(v / 1000).toFixed(0)}k`}
                />
                <YAxis
                  type="category"
                  dataKey="name"
                  stroke="#475569"
                  fontSize={11}
                  width={80}
                />
                <Tooltip
                  contentStyle={{
                    background: "#1e293b",
                    border: "1px solid #334155",
                    borderRadius: "0.5rem",
                    fontSize: "0.75rem",
                    color: "#e2e8f0",
                  }}
                  formatter={(v: number) => [formatCurrency(v), "Spend"]}
                />
                <Bar dataKey="spend" radius={[0, 4, 4, 0]}>
                  {(data.by_platform ?? []).map((entry) => (
                    <Cell
                      key={entry.platform_id}
                      fill={
                        PLATFORM_COLORS[entry.platform_id] ?? "#64748b"
                      }
                    />
                  ))}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </div>
        </Card>
      )}

      {/* Campaign table */}
      {data.campaigns && data.campaigns.length > 0 && (
        <Card className="overflow-hidden !p-0">
          <div className="px-5 py-4">
            <h4 className="text-sm font-medium text-slate-400">Campaigns</h4>
          </div>
          <div className="overflow-x-auto">
            <table className="w-full text-left text-sm">
              <thead>
                <tr className="border-t border-slate-800 text-xs uppercase tracking-wider text-slate-500">
                  <th className="px-5 py-3 font-medium">Campaign</th>
                  <th className="px-5 py-3 font-medium">Platform</th>
                  <th className="px-5 py-3 font-medium text-right">Spend</th>
                  <th className="px-5 py-3 font-medium text-right">Impr.</th>
                  <th className="px-5 py-3 font-medium text-right">Clicks</th>
                  <th className="px-5 py-3 font-medium text-right">CTR</th>
                  <th className="px-5 py-3 font-medium text-right">CPC</th>
                </tr>
              </thead>
              <tbody>
                {data.campaigns.map((c, i) => (
                  <tr
                    key={`${c.campaign_id}-${i}`}
                    className="border-t border-slate-800/50 transition-colors hover:bg-slate-800/30"
                  >
                    <td className="max-w-[300px] truncate px-5 py-3 text-slate-200">
                      {c.campaign_name}
                    </td>
                    <td className="px-5 py-3 text-slate-400">
                      {platformLabel(c.platform_id)}
                    </td>
                    <td className="px-5 py-3 text-right tabular-nums text-slate-200">
                      {formatCurrency(c.spend)}
                    </td>
                    <td className="px-5 py-3 text-right tabular-nums text-slate-400">
                      {formatNumber(c.impressions)}
                    </td>
                    <td className="px-5 py-3 text-right tabular-nums text-slate-400">
                      {formatNumber(c.clicks)}
                    </td>
                    <td className="px-5 py-3 text-right tabular-nums text-slate-400">
                      {formatPercent(c.ctr * 100)}
                    </td>
                    <td className="px-5 py-3 text-right tabular-nums text-slate-400">
                      ${c.cpc.toFixed(2)}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </Card>
      )}
    </div>
  );
}
