"use client";

import {
  type AdSetPerformanceResponse,
} from "@/lib/api";
import { Card } from "@/components/card";
import {
  formatCurrency,
  formatNumber,
  formatPercent,
  platformLabel,
} from "@/lib/utils";

function freqHealthDot(f: number | null | undefined): string | null {
  if (f == null || f <= 0) return null;
  if (f <= 3) return "bg-emerald-500";
  if (f <= 5) return "bg-amber-400";
  return "bg-red-500";
}

export function AdSetDrillDown({
  data,
}: {
  data: AdSetPerformanceResponse;
}) {
  if (!data.ad_sets || data.ad_sets.length === 0) {
    return null;
  }

  return (
    <Card className="overflow-hidden !p-0">
      <div className="px-5 py-4 border-b border-slate-800">
        <h4 className="text-sm font-medium text-slate-400">Audience performance</h4>
        {data.total_reach_note && (
          <p className="mt-1 text-xs text-slate-500">{data.total_reach_note}</p>
        )}
      </div>
      <div className="overflow-x-auto">
        <table className="w-full text-left text-sm">
          <thead>
            <tr className="border-t border-slate-800 text-xs uppercase tracking-wider text-slate-500">
              <th className="px-5 py-3 font-medium">Audience</th>
              <th className="px-5 py-3 font-medium">Platform</th>
              <th className="px-5 py-3 font-medium text-right">Reach</th>
              <th className="px-5 py-3 font-medium text-right">Freq.</th>
              <th className="px-5 py-3 font-medium text-right">Spend</th>
              <th className="px-5 py-3 font-medium text-right">Eng. rate</th>
              <th className="px-5 py-3 font-medium text-right">Ads</th>
            </tr>
          </thead>
          <tbody>
            {data.ad_sets.map((row, i) => (
              <tr
                key={`${row.ad_set_id}-${row.platform_id}-${i}`}
                className="border-t border-slate-800/50 hover:bg-slate-800/30"
              >
                <td className="max-w-[220px] truncate px-5 py-3 text-slate-200">
                  {row.ad_set_name ?? "—"}
                </td>
                <td className="px-5 py-3 text-slate-400">{platformLabel(row.platform_id)}</td>
                <td className="px-5 py-3 text-right tabular-nums text-slate-300">
                  {row.reach != null ? formatNumber(row.reach) : "—"}
                </td>
                <td className="px-5 py-3 text-right tabular-nums text-slate-300">
                  <span className="inline-flex items-center justify-end gap-1.5">
                    {row.frequency != null ? row.frequency.toFixed(1) : "—"}
                    {freqHealthDot(row.frequency) && (
                      <span className={`inline-block h-2 w-2 rounded-full ${freqHealthDot(row.frequency)}`} />
                    )}
                  </span>
                </td>
                <td className="px-5 py-3 text-right tabular-nums text-slate-300">
                  {formatCurrency(row.spend)}
                </td>
                <td className="px-5 py-3 text-right tabular-nums text-slate-400">
                  {row.engagement_rate != null ? formatPercent(row.engagement_rate * 100) : "—"}
                </td>
                <td className="px-5 py-3 text-right tabular-nums text-slate-400">
                  {row.ad_count}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </Card>
  );
}
