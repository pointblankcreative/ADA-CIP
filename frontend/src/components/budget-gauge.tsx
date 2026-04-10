"use client";

import { cn } from "@/lib/utils";

interface BudgetGaugeProps {
  spent: number;
  budget: number;
  className?: string;
}

export function BudgetGauge({ spent, budget, className }: BudgetGaugeProps) {
  const pct = budget > 0 ? (spent / budget) * 100 : 0;
  const capped = Math.min(pct, 100);

  // Color based on budget utilization, not pacing:
  // green for normal spend, amber when >90%, red when over budget
  const barColor =
    pct > 100 ? "bg-red-500" : pct > 90 ? "bg-amber-500" : "bg-emerald-500";

  return (
    <div className={cn("w-full", className)}>
      <div className="h-2 w-full overflow-hidden rounded-full bg-slate-800">
        <div
          className={cn(
            "h-full rounded-full transition-all duration-500",
            barColor
          )}
          style={{ width: `${capped}%` }}
        />
      </div>
    </div>
  );
}
