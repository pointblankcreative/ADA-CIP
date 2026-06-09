"use client";

import {
  type AdPerformanceResponse,
} from "@/lib/api";
import { Card } from "@/components/card";
import { Label } from "@/components/ui";
import { TH_CLS } from "@/lib/chart-theme";
import {
  cn,
  formatCurrency,
  formatNumber,
  formatPercent,
  platformLabel,
  renderEngagementRate,
} from "@/lib/utils";

export function AdDrillDown({
  data,
  engagementSupport,
}: {
  data: AdPerformanceResponse;
  /**
   * Platforms that the backend says report engagements for this project, taken
   * from PerformanceResponse.metric_platforms.engagements. Used to render "—"
   * for rows whose platform doesn't report the metric (rather than the
   * backend's 0.0, which is the AI-029 / AI-115 bug).
   */
  engagementSupport?: string[];
}) {
  if (!data.ads || data.ads.length === 0) {
    return null;
  }

  return (
    <Card className="overflow-hidden !p-0">
      <div className="border-b border-line-soft px-5 py-4">
        <Label className="text-fg-secondary">Creative performance</Label>
      </div>
      <div className="overflow-x-auto">
        <table className="w-full text-left text-sm">
          <thead>
            <tr>
              <th className={TH_CLS}>Ad</th>
              <th className={TH_CLS}>Audience</th>
              <th className={TH_CLS}>Platform</th>
              <th className={cn(TH_CLS, "text-right")}>Spend</th>
              <th className={cn(TH_CLS, "text-right")}>CTR</th>
              <th className={cn(TH_CLS, "text-right")}>Conv.</th>
              <th className={cn(TH_CLS, "text-right")}>CPA</th>
              <th className={cn(TH_CLS, "text-right")}>Eng. rate</th>
              <th className={cn(TH_CLS, "text-right")}>VCR</th>
            </tr>
          </thead>
          <tbody>
            {data.ads.map((row, i) => (
              <tr
                key={`${row.ad_id}-${row.platform_id}-${i}`}
                className="border-t border-line-soft hover:bg-surface-sunken"
              >
                <td className="max-w-[200px] truncate px-5 py-3 font-medium text-fg">
                  {row.ad_name ?? "—"}
                </td>
                <td className="max-w-[160px] truncate px-5 py-3 text-fg-muted">
                  {row.ad_set_name ?? "—"}
                </td>
                <td className="px-5 py-3 text-fg-muted">{platformLabel(row.platform_id)}</td>
                <td className="tnum px-5 py-3 text-right font-mono text-fg-secondary">
                  {formatCurrency(row.spend)}
                </td>
                <td className="tnum px-5 py-3 text-right font-mono text-fg-secondary">
                  {row.ctr != null ? formatPercent(row.ctr * 100) : "—"}
                </td>
                <td className="tnum px-5 py-3 text-right font-mono text-fg-secondary">
                  {row.conversions > 0 ? formatNumber(Math.round(row.conversions)) : "—"}
                </td>
                <td className="tnum px-5 py-3 text-right font-mono text-fg-secondary">
                  {row.conversions > 0 ? formatCurrency(row.spend / row.conversions) : "—"}
                </td>
                <td className="tnum px-5 py-3 text-right font-mono text-fg-secondary">
                  {renderEngagementRate(row.engagement_rate, row.platform_id, engagementSupport)}
                </td>
                <td className="tnum px-5 py-3 text-right font-mono text-fg-secondary">
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
