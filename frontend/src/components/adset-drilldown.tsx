"use client";

import {
  type AdSetPerformanceResponse,
} from "@/lib/api";
import { Card } from "@/components/card";
import { Label } from "@/components/ui";
import { TH_CLS } from "@/lib/chart-theme";
import {
  cn,
  formatCurrency,
  formatNumber,
  platformLabel,
  renderEngagementRate,
} from "@/lib/utils";

function freqHealthDot(f: number | null | undefined): string | null {
  if (f == null || f <= 0) return null;
  if (f <= 3) return "bg-ok";
  if (f <= 5) return "bg-warn";
  return "bg-danger";
}

export function AdSetDrillDown({
  data,
  engagementSupport,
}: {
  data: AdSetPerformanceResponse;
  /**
   * Platforms that the backend says report engagements for this project, taken
   * from PerformanceResponse.metric_platforms.engagements. When undefined we
   * fall through to the old behaviour (render "—" for every row, since
   * nothing supports the metric).
   */
  engagementSupport?: string[];
}) {
  if (!data.ad_sets || data.ad_sets.length === 0) {
    return null;
  }

  return (
    <Card className="overflow-hidden !p-0">
      <div className="border-b border-line-soft px-5 py-4">
        <Label className="text-fg-secondary">Audience performance</Label>
        {data.total_reach_note && (
          <p className="mt-1 text-xs text-fg-muted">{data.total_reach_note}</p>
        )}
      </div>
      <div className="overflow-x-auto">
        <table className="w-full text-left text-sm">
          <thead>
            <tr>
              <th className={TH_CLS}>Audience</th>
              <th className={TH_CLS}>Platform</th>
              <th className={cn(TH_CLS, "text-right")}>Reach</th>
              <th className={cn(TH_CLS, "text-right")}>Freq.</th>
              <th className={cn(TH_CLS, "text-right")}>Spend</th>
              <th className={cn(TH_CLS, "text-right")}>Eng. rate</th>
              <th className={cn(TH_CLS, "text-right")}>Ads</th>
            </tr>
          </thead>
          <tbody>
            {data.ad_sets.map((row, i) => (
              <tr
                key={`${row.ad_set_id}-${row.platform_id}-${i}`}
                className="border-t border-line-soft hover:bg-surface-sunken"
              >
                <td className="max-w-[220px] truncate px-5 py-3 font-medium text-fg">
                  {row.ad_set_name ?? "—"}
                </td>
                <td className="px-5 py-3 text-fg-muted">{platformLabel(row.platform_id)}</td>
                <td className="tnum px-5 py-3 text-right font-mono text-fg-secondary">
                  {row.reach != null ? formatNumber(row.reach) : "—"}
                </td>
                <td className="tnum px-5 py-3 text-right font-mono text-fg-secondary">
                  <span className="inline-flex items-center justify-end gap-1.5">
                    {row.frequency != null ? row.frequency.toFixed(1) : "—"}
                    {freqHealthDot(row.frequency) && (
                      <span className={`inline-block h-2 w-2 rounded-full ${freqHealthDot(row.frequency)}`} />
                    )}
                  </span>
                </td>
                <td className="tnum px-5 py-3 text-right font-mono text-fg-secondary">
                  {formatCurrency(row.spend)}
                </td>
                <td className="tnum px-5 py-3 text-right font-mono text-fg-secondary">
                  {renderEngagementRate(row.engagement_rate, row.platform_id, engagementSupport)}
                </td>
                <td className="tnum px-5 py-3 text-right font-mono text-fg-secondary">
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
