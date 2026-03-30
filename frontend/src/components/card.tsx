import { cn } from "@/lib/utils";
import type { ReactNode } from "react";

interface CardProps {
  children: ReactNode;
  className?: string;
}

export function Card({ children, className }: CardProps) {
  return (
    <div
      className={cn(
        "rounded-lg border border-slate-800 bg-surface-raised p-5",
        className
      )}
    >
      {children}
    </div>
  );
}

interface KpiCardProps {
  label: string;
  value: string;
  sub?: string;
  accent?: string;
}

export function KpiCard({ label, value, sub, accent }: KpiCardProps) {
  return (
    <Card>
      <p className="text-xs font-medium uppercase tracking-wider text-slate-500">
        {label}
      </p>
      <p className={cn("mt-1.5 text-2xl font-semibold tabular-nums", accent ?? "text-white")}>
        {value}
      </p>
      {sub && <p className="mt-0.5 text-xs text-slate-500">{sub}</p>}
    </Card>
  );
}
