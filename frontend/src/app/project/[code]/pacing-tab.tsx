"use client";

import { useEffect, useState } from "react";
import { api, type PacingResponse, type PacingLine } from "@/lib/api";
import { Card, KpiCard } from "@/components/card";
import { PacingBadge } from "@/components/pacing-badge";
import {
  formatCurrency,
  formatPercent,
  pacingStatus,
  pacingBarColor,
  pacingColor,
  platformLabel,
  cn,
} from "@/lib/utils";

export function PacingTab({ code }: { code: string }) {
  const [data, setData] = useState<PacingResponse | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    api.pacing
      .get(code)
      .then(setData)
      .catch(() => setData(null))
      .finally(() => setLoading(false));
  }, [code]);

  if (loading) {
    return (
      <div className="grid grid-cols-2 gap-4 lg:grid-cols-4">
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
        <p className="text-slate-400">
          No pacing data available. Run the pacing engine first.
        </p>
      </Card>
    );
  }

  const overallStatus = pacingStatus(data.overall_pacing_percentage);

  return (
    <div className="space-y-6">
      {/* KPI cards */}
      <div className="grid grid-cols-2 gap-4 lg:grid-cols-4">
        <KpiCard
          label="Total Budget"
          value={formatCurrency(data.net_budget)}
        />
        <KpiCard
          label="Spent to Date"
          value={formatCurrency(data.total_actual_to_date)}
          sub={`of ${formatCurrency(data.total_planned_to_date)} planned`}
        />
        <KpiCard
          label="Remaining"
          value={formatCurrency(
            data.net_budget - data.total_actual_to_date
          )}
        />
        <KpiCard
          label="Overall Pacing"
          value={formatPercent(data.overall_pacing_percentage)}
          accent={pacingColor(overallStatus)}
        />
      </div>

      {/* Per-line pacing */}
      <div>
        <h3 className="text-sm font-semibold uppercase tracking-wider text-slate-500">
          Line-by-Line Pacing
        </h3>
        <div className="mt-3 space-y-3">
          {data.lines.map((line) => (
            <LineRow key={line.line_id} line={line} />
          ))}
        </div>
      </div>

      {/* Blocking chart visualization */}
      <div>
        <h3 className="text-sm font-semibold uppercase tracking-wider text-slate-500">
          As of {data.as_of_date}
        </h3>
      </div>
    </div>
  );
}

function LineRow({ line }: { line: PacingLine }) {
  const status = pacingStatus(line.pacing_percentage);
  const barPct = Math.min(line.pacing_percentage, 150);
  const budgetPct =
    line.planned_budget > 0
      ? (line.actual_spend_to_date / line.planned_budget) * 100
      : 0;

  return (
    <Card className="!p-3 sm:!p-4">
      <div className="flex flex-col gap-2 sm:flex-row sm:items-center sm:justify-between">
        <div className="flex items-center gap-3 min-w-0">
          <div className="flex h-8 w-8 flex-shrink-0 items-center justify-center rounded-md bg-slate-800 text-xs font-bold text-slate-300">
            {platformLabel(line.platform_id).charAt(0)}
          </div>
          <div className="min-w-0">
            <div className="flex flex-wrap items-center gap-1.5 sm:gap-2">
              <span className="text-sm font-medium text-white">
                {platformLabel(line.platform_id)}
              </span>
              {line.line_code && (
                <span className="rounded bg-slate-800 px-1.5 py-0.5 font-mono text-[10px] text-slate-400">
                  {line.line_code}
                </span>
              )}
              <span className="text-xs text-slate-500">
                {line.channel_category}
              </span>
            </div>
            <div className="mt-0.5 flex flex-wrap items-center gap-x-3 gap-y-0.5 text-xs text-slate-500">
              <span>{formatCurrency(line.actual_spend_to_date)} spent</span>
              <span>of {formatCurrency(line.planned_budget)} budget</span>
              {line.remaining_days > 0 && (
                <span>{line.remaining_days}d remaining</span>
              )}
              {line.daily_budget_required != null &&
                line.daily_budget_required > 0 && (
                  <span>
                    {formatCurrency(line.daily_budget_required)}/day needed
                  </span>
                )}
            </div>
          </div>
        </div>
        <div className="flex-shrink-0 self-end sm:self-auto">
          <PacingBadge percentage={line.pacing_percentage} />
        </div>
      </div>

      {/* Progress bar */}
      <div className="mt-3">
        <div className="relative h-3 w-full overflow-hidden rounded-full bg-slate-800">
          {/* Planned marker at planned % */}
          <div
            className="absolute top-0 bottom-0 w-0.5 bg-slate-500 z-10"
            style={{
              left: `${Math.min(
                (line.planned_spend_to_date / Math.max(line.planned_budget, 1)) * 100,
                100
              )}%`,
            }}
          />
          {/* Actual bar */}
          <div
            className={cn(
              "h-full rounded-full transition-all duration-700",
              pacingBarColor(status)
            )}
            style={{ width: `${Math.min(budgetPct, 100)}%` }}
          />
        </div>
        <div className="mt-1 flex justify-between text-[10px] text-slate-600">
          <span>0%</span>
          <span>Budget: {formatCurrency(line.planned_budget)}</span>
        </div>
      </div>
    </Card>
  );
}
