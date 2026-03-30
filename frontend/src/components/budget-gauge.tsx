"use client";

import { cn, pacingStatus, pacingBarColor } from "@/lib/utils";

interface BudgetGaugeProps {
  spent: number;
  budget: number;
  className?: string;
}

export function BudgetGauge({ spent, budget, className }: BudgetGaugeProps) {
  const pct = budget > 0 ? (spent / budget) * 100 : 0;
  const capped = Math.min(pct, 100);
  const status = pacingStatus(pct);

  return (
    <div className={cn("w-full", className)}>
      <div className="h-2 w-full overflow-hidden rounded-full bg-slate-800">
        <div
          className={cn(
            "h-full rounded-full transition-all duration-500",
            pacingBarColor(status)
          )}
          style={{ width: `${capped}%` }}
        />
      </div>
    </div>
  );
}
