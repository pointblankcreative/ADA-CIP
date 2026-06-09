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
  // healthy for normal spend, warn when >90%, danger when over budget
  const barColor = pct > 100 ? "bg-danger" : pct > 90 ? "bg-warn" : "bg-ok";

  return (
    <div className={cn("w-full", className)}>
      <div className="h-2 w-full overflow-hidden rounded-pill bg-surface-sunken">
        <div
          className={cn(
            "h-full rounded-pill transition-all duration-700 ease-snap",
            barColor
          )}
          style={{ width: `${capped}%` }}
        />
      </div>
    </div>
  );
}
