import { cn } from "@/lib/utils";
import type { ReactNode } from "react";

interface CardProps {
  children: ReactNode;
  className?: string;
  /** Native browser tooltip (AI-102: used for per-platform metric definitions). */
  title?: string;
}

/**
 * Card — flat fill, 2px border, tight radius. Point Blank cards are not
 * floaty drop-shadow cards; the border is the structuring device.
 */
export function Card({ children, className, title }: CardProps) {
  return (
    <div
      className={cn(
        "rounded-md border-2 border-line-soft bg-surface-card p-[18px]",
        className
      )}
      title={title}
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
  /** Native browser tooltip on hover (AI-102: per-platform clicks definitions). */
  title?: string;
  /** Folsom display treatment for hero numbers (design: Kpi `big`). */
  big?: boolean;
}

function benchmarkColor(bm: BenchmarkIndicator): string {
  const { current, p25, p75, lowerIsBetter } = bm;
  if (current == null || p25 == null || p75 == null) return "text-fg-muted";
  if (lowerIsBetter) {
    if (current <= p25) return "text-ok";
    if (current >= p75) return "text-danger";
    return "text-fg-muted";
  }
  if (current >= p75) return "text-ok";
  if (current <= p25) return "text-danger";
  return "text-fg-muted";
}

function benchmarkBarPosition(bm: BenchmarkIndicator): number {
  const range = bm.p75 - bm.p25;
  if (range <= 0) return 50;
  const pos = ((bm.current - bm.p25) / range) * 100;
  return Math.max(0, Math.min(100, pos));
}

export function KpiCard({
  label,
  value,
  sub,
  accent,
  benchmark,
  title,
  big = false,
}: KpiCardProps) {
  const fmt = benchmark?.format ?? ((v: number) => (v ?? 0).toFixed(2));

  return (
    <Card title={title} className="p-4">
      <p className="label text-[10.5px]">{label}</p>
      <p
        className={cn(
          "mt-2 tnum break-all leading-none sm:break-normal",
          big
            ? "font-display text-3xl uppercase tracking-[0.01em] sm:text-4xl"
            : "text-xl font-extrabold tracking-tight sm:text-[27px]",
          accent ?? "text-fg"
        )}
      >
        {value}
      </p>
      {sub && <p className="mt-[7px] text-xs text-fg-faint">{sub}</p>}
      {benchmark && (
        <div className="mt-[11px] space-y-[5px]">
          <div
            className={cn(
              "font-mono text-[10px] uppercase tracking-[0.06em]",
              benchmarkColor(benchmark)
            )}
          >
            Bench {fmt(benchmark.p50)}
          </div>
          <div className="relative h-1 rounded-pill bg-surface-sunken">
            <div
              className={cn(
                "absolute top-[-2px] h-2 w-2 rounded-full border-[1.5px] border-surface-card",
                benchmarkColor(benchmark).replace("text-", "bg-")
              )}
              style={{
                left: `${benchmarkBarPosition(benchmark)}%`,
                transform: "translateX(-50%)",
              }}
            />
          </div>
          <div className="flex justify-between font-mono text-[9px] text-fg-faint">
            <span>{fmt(benchmark.p25)}</span>
            <span>{fmt(benchmark.p75)}</span>
          </div>
        </div>
      )}
    </Card>
  );
}
