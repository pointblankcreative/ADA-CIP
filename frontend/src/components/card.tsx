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

export interface BenchmarkIndicator {
  p25: number;
  p50: number;
  p75: number;
  current: number;
  lowerIsBetter?: boolean;
  format?: (v: number) => string;
}

interface KpiCardProps {
  label: string;
  value: string;
  sub?: string;
  accent?: string;
  benchmark?: BenchmarkIndicator;
}

function benchmarkColor(bm: BenchmarkIndicator): string {
  const { current, p25, p75, lowerIsBetter } = bm;
  if (lowerIsBetter) {
    if (current <= p25) return "text-emerald-400";
    if (current >= p75) return "text-red-400";
    return "text-slate-400";
  }
  if (current >= p75) return "text-emerald-400";
  if (current <= p25) return "text-red-400";
  return "text-slate-400";
}

function benchmarkBarPosition(bm: BenchmarkIndicator): number {
  const range = bm.p75 - bm.p25;
  if (range <= 0) return 50;
  const pos = ((bm.current - bm.p25) / range) * 100;
  return Math.max(0, Math.min(100, pos));
}

export function KpiCard({ label, value, sub, accent, benchmark }: KpiCardProps) {
  const fmt = benchmark?.format ?? ((v: number) => v.toFixed(2));

  return (
    <Card>
      <p className="text-xs font-medium uppercase tracking-wider text-slate-500">
        {label}
      </p>
      <p className={cn("mt-1.5 text-2xl font-semibold tabular-nums", accent ?? "text-white")}>
        {value}
      </p>
      {sub && <p className="mt-0.5 text-xs text-slate-500">{sub}</p>}
      {benchmark && (
        <div className="mt-2 space-y-1">
          <div className="flex items-center justify-between text-[10px]">
            <span className={cn("font-medium", benchmarkColor(benchmark))}>
              Benchmark: {fmt(benchmark.p50)}
            </span>
          </div>
          <div className="relative h-1.5 rounded-full bg-slate-800">
            <div
              className="absolute top-0 h-full rounded-full bg-slate-600"
              style={{
                left: "0%",
                width: "100%",
              }}
            />
            <div
              className={cn(
                "absolute top-[-1px] h-2 w-2 rounded-full border border-slate-900",
                benchmarkColor(benchmark).replace("text-", "bg-"),
              )}
              style={{
                left: `${benchmarkBarPosition(benchmark)}%`,
                transform: "translateX(-50%)",
              }}
            />
          </div>
          <div className="flex justify-between text-[9px] text-slate-600">
            <span>{fmt(benchmark.p25)}</span>
            <span>{fmt(benchmark.p75)}</span>
          </div>
        </div>
      )}
    </Card>
  );
}
