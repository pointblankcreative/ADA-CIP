"use client";

import {
  type AdPerformanceResponse,
} from "@/lib/api";
import { Card } from "@/components/card";
import {
  formatCurrency,
  formatPercent,
  platformLabel,
} from "@/lib/utils";

export function AdDrillDown({
  data,
}: {
  data: AdPerformanceResponse;
}) {
  if (!data.ads || data.ads.length === 0) {
    return null;
  }

  return (
    <Card className="overflow-hidden !p-0">
      <div className="px-5 py-4 border-b border-slate-800">
        <h4 className="text-sm font-medium text-slate-400">Creative performance</h4>
      </div>
      <div className="overflow-x-auto">
        <table className="w-full text-left text-sm">
          <thead>
            <tr className="border-t border-slate-800 text-xs uppercase tracking-wider text-slate-500">
              <th className="px-5 py-3 font-medium">Ad</th>
              <th className="px-5 py-3 font-medium">Audience</th>
              <th className="px-5 py-3 font-medium">Platform</th>
              <th className="px-5 py-3 font-medium text-right">Spend</th>
              <th className="px-5 py-3 font-medium text-right">CTR</th>
              <th className="px-5 py-3 font-medium text-right">Eng. rate</th>
              <th className="px-5 py-3 font-medium text-right">VCR</th>
            </tr>
          </thead>
          <tbody>
            {data.ads.map((row, i) => (
              <tr
                key={`${row.ad_id}-${row.platform_id}-${i}`}
                className="border-t border-slate-800/50 hover:bg-slate-800/30"
              >
                <td className="max-w-[200px] truncate px-5 py-3 text-slate-200">
                  {row.ad_name ?? "—"}
                </td>
                <td className="max-w-[160px] truncate px-5 py-3 text-slate-500">
                  {row.ad_set_name ?? "—"}
                </td>
                <td className="px-5 py-3 text-slate-400">{platformLabel(row.platform_id)}</td>
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
            ))}
          </tbody>
        </table>
      </div>
    </Card>
  );
}
