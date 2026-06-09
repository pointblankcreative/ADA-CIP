"use client";

import {
  ObjectiveType,
  type PerformanceResponse,
} from "@/lib/api";
import { Card } from "@/components/card";
import { Label } from "@/components/ui";
import { OBJECTIVE_BADGE, TH_CLS } from "@/lib/chart-theme";
import {
  formatCurrency,
  formatNumber,
  formatPercent,
  platformLabel,
  cn,
} from "@/lib/utils";

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
        <Label className="text-fg-secondary">Campaigns</Label>
      </div>
      <div className="overflow-x-auto">
        <table className="w-full text-left text-sm">
          <thead>
            <tr className="border-t border-line-soft">
              <th className={TH_CLS}>Campaign</th>
              <th className={TH_CLS}>Platform</th>
              <th className={TH_CLS}>Objective</th>
              <th className={cn(TH_CLS, "text-right")}>Spend</th>
              <th className={cn(TH_CLS, "text-right")}>Impr.</th>
              <th className={cn(TH_CLS, "text-right")}>Clicks</th>
              <th className={cn(TH_CLS, "text-right")}>CTR</th>
              {/* No Reach column: reach/frequency only exist at audience
                  (ad set) grain — campaign-grain reach never populates.
                  See the Audience performance table for R&F. */}
              {showAwareness && has(data, "vcr") && (
                <th className={cn(TH_CLS, "text-right")}>VCR</th>
              )}
              {showConversion && has(data, "conversions") && (
                <>
                  <th className={cn(TH_CLS, "text-right")}>Conv.</th>
                  <th className={cn(TH_CLS, "text-right")}>CPA</th>
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
                  className="border-t border-line-soft transition-colors hover:bg-surface-sunken"
                >
                  <td className="max-w-[260px] truncate px-5 py-3 font-medium text-fg">
                    {c.campaign_name}
                  </td>
                  <td className="px-5 py-3 text-fg-muted">
                    {platformLabel(c.platform_id)}
                  </td>
                  <td className="px-5 py-3">
                    {objBadge ? (
                      <span className={cn("rounded-pill border px-2 py-0.5 font-mono text-[10px] font-medium uppercase tracking-[0.06em]", objBadge.cls)}>
                        {objBadge.label}
                      </span>
                    ) : (
                      <span className="text-fg-faint">—</span>
                    )}
                  </td>
                  <td className="tnum px-5 py-3 text-right font-mono text-fg">
                    {formatCurrency(c.spend)}
                  </td>
                  <td className="tnum px-5 py-3 text-right font-mono text-fg-secondary">
                    {formatNumber(c.impressions)}
                  </td>
                  <td className="tnum px-5 py-3 text-right font-mono text-fg-secondary">
                    {formatNumber(c.clicks)}
                  </td>
                  <td className="tnum px-5 py-3 text-right font-mono text-fg-secondary">
                    {c.ctr != null ? formatPercent(c.ctr * 100) : "—"}
                  </td>
                  {showAwareness && has(data, "vcr") && (
                    <td className="tnum px-5 py-3 text-right font-mono text-fg-secondary">
                      {c.vcr != null ? formatPercent(c.vcr * 100) : "—"}
                    </td>
                  )}
                  {showConversion && has(data, "conversions") && (
                    <>
                      <td className="tnum px-5 py-3 text-right font-mono text-fg-secondary">
                        {c.conversions ? formatNumber(Math.round(c.conversions)) : "—"}
                      </td>
                      <td className="tnum px-5 py-3 text-right font-mono text-fg-secondary">
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
