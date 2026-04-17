"use client";

import {
  ObjectiveType,
  type PerformanceResponse,
} from "@/lib/api";
import { Card } from "@/components/card";
import {
  formatCurrency,
  formatNumber,
  formatPercent,
  platformLabel,
  cn,
} from "@/lib/utils";

const OBJECTIVE_BADGE: Record<ObjectiveType, { label: string; cls: string }> = {
  awareness: { label: "Awareness", cls: "bg-purple-500/20 text-purple-400 border-purple-500/30" },
  conversion: { label: "Conversion", cls: "bg-emerald-500/20 text-emerald-400 border-emerald-500/30" },
  mixed: { label: "Mixed", cls: "bg-blue-500/20 text-blue-400 border-blue-500/30" },
};

function has(data: PerformanceResponse, metric: string): boolean {
  return data.available_metrics.includes(metric);
}

export function CampaignTable({
  data,
  showAwareness,
  showConversion,
}: {
  data: PerformanceResponse;
  showAwareness: boolean;
  showConversion: boolean;
}) {
  if (!data.campaigns || data.campaigns.length === 0) {
    return null;
  }

  return (
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
              <th className="px-5 py-3 font-medium">Objective</th>
              <th className="px-5 py-3 font-medium text-right">Spend</th>
              <th className="px-5 py-3 font-medium text-right">Impr.</th>
              <th className="px-5 py-3 font-medium text-right">Clicks</th>
              <th className="px-5 py-3 font-medium text-right">CTR</th>
              {showAwareness && has(data, "reach") && (
                <th className="px-5 py-3 font-medium text-right">Reach</th>
              )}
              {showAwareness && has(data, "vcr") && (
                <th className="px-5 py-3 font-medium text-right">VCR</th>
              )}
              {showConversion && has(data, "conversions") && (
                <>
                  <th className="px-5 py-3 font-medium text-right">Conv.</th>
                  <th className="px-5 py-3 font-medium text-right">CPA</th>
                </>
              )}
            </tr>
          </thead>
          <tbody>
            {data.campaigns.map((c, i) => {
              const objBadge = c.objective ? OBJECTIVE_BADGE[c.objective as ObjectiveType] : null;
              return (
                <tr
                  key={`${c.campaign_id}-${i}`}
                  className="border-t border-slate-800/50 transition-colors hover:bg-slate-800/30"
                >
                  <td className="max-w-[260px] truncate px-5 py-3 text-slate-200">
                    {c.campaign_name}
                  </td>
                  <td className="px-5 py-3 text-slate-400">
                    {platformLabel(c.platform_id)}
                  </td>
                  <td className="px-5 py-3">
                    {objBadge ? (
                      <span className={cn("rounded-full border px-2 py-0.5 text-[10px] font-medium", objBadge.cls)}>
                        {objBadge.label}
                      </span>
                    ) : (
                      <span className="text-slate-600">—</span>
                    )}
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
                    {c.ctr != null ? formatPercent(c.ctr * 100) : "—"}
                  </td>
                  {showAwareness && has(data, "reach") && (
                    <td className="px-5 py-3 text-right tabular-nums text-slate-400">
                      {c.reach ? formatNumber(c.reach) : "—"}
                    </td>
                  )}
                  {showAwareness && has(data, "vcr") && (
                    <td className="px-5 py-3 text-right tabular-nums text-slate-400">
                      {c.vcr != null ? formatPercent(c.vcr * 100) : "—"}
                    </td>
                  )}
                  {showConversion && has(data, "conversions") && (
                    <>
                      <td className="px-5 py-3 text-right tabular-nums text-slate-400">
                        {c.conversions ? formatNumber(Math.round(c.conversions)) : "—"}
                      </td>
                      <td className="px-5 py-3 text-right tabular-nums text-slate-400">
                        {c.cpa != null ? formatCurrency(c.cpa) : "—"}
                      </td>
                    </>
                  )}
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </Card>
  );
}
