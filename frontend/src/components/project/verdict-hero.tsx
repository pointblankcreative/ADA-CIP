"use client";

/**
 * VerdictHero — the verdict-first landing block on the Summary tab.
 * "On track? What's wrong? What do I do?" answered before any depth.
 * Ported from the prototype's summary.jsx.
 */
import { Gauge, TrendingUp, Activity, type LucideIcon } from "lucide-react";
import type { Project } from "@/lib/api";
import type { FlightMath, Verdict, VerdictIcon } from "@/lib/flight";
import { Card } from "@/components/card";
import { Btn } from "@/components/ui";
import { formatCurrency, formatCurrencyCompact } from "@/lib/utils";

const VERDICT_ICONS: Record<VerdictIcon, LucideIcon> = {
  gauge: Gauge,
  "trending-up": TrendingUp,
  activity: Activity,
};

export function DeltaChip({ f }: { f: FlightMath }) {
  if (f.deltaAmount == null) return null;
  const over = f.deltaAmount > 0.5;
  const under = f.deltaAmount < -0.5;
  const onPlan = !over && !under;
  const color = onPlan
    ? "var(--ok)"
    : Math.abs(f.deltaPct ?? 0) > 15
      ? "var(--danger)"
      : "var(--warn)";
  return (
    <span
      className="inline-flex items-center gap-1.5 rounded-pill px-[11px] py-1 font-mono text-xs font-bold"
      style={{
        color,
        backgroundColor: `color-mix(in srgb, ${color} 14%, transparent)`,
        border: `1.5px solid color-mix(in srgb, ${color} 34%, transparent)`,
      }}
    >
      {onPlan
        ? "ON BUDGET"
        : `${f.deltaAmount > 0 ? "+" : "−"}${formatCurrencyCompact(
            Math.abs(f.deltaAmount)
          )} ${over ? "OVER" : "UNDER"}`}
    </span>
  );
}

export function VerdictHero({
  p,
  f,
  v,
  asOf,
  onTab,
}: {
  p: Project;
  f: FlightMath;
  v: Verdict;
  /** ISO date the read is current as of (pacing.as_of_date). */
  asOf: string | null;
  onTab: (tab: string) => void;
}) {
  const ActionIcon = v.action ? VERDICT_ICONS[v.action.icon] : null;

  return (
    <Card
      className="overflow-hidden p-0"
      style={{
        borderColor: `color-mix(in srgb, ${v.tone} 40%, transparent)`,
        background: `linear-gradient(160deg, color-mix(in srgb, ${v.tone} 8%, var(--surface-card)) 0%, var(--surface-card) 60%)`,
      }}
    >
      <div className="grid lg:grid-cols-[minmax(0,1.5fr)_minmax(240px,1fr)]">
        {/* left: the verdict */}
        <div className="p-[26px] sm:p-7">
          <div className="flex items-center gap-2.5">
            <span
              className="h-2 w-2 rounded-full"
              style={{ backgroundColor: v.tone }}
            />
            <span className="eyebrow" style={{ color: v.tone }}>
              Verdict · as of {f.ended ? p.end_date : (asOf ?? "today")}
            </span>
          </div>
          <div
            className="mt-3.5 font-display text-[40px] uppercase leading-[0.92] tracking-[0.01em] sm:text-5xl"
            style={{ color: v.tone }}
          >
            {v.word}
          </div>
          <h2 className="mt-3 text-[22px] font-extrabold leading-[1.15] tracking-tight text-fg">
            {v.headline}
          </h2>
          <p className="mt-2.5 max-w-[520px] text-[14.5px] leading-relaxed text-fg-secondary">
            {v.detail}
          </p>
          {v.action && ActionIcon && (
            <div className="mt-[18px] flex flex-wrap gap-2.5">
              <Btn
                variant="primary"
                size="md"
                icon={<ActionIcon className="h-4 w-4" />}
              >
                {v.action.label}
              </Btn>
              <Btn
                variant="ghost"
                size="md"
                icon={<Gauge className="h-4 w-4" />}
                onClick={() => onTab("pacing")}
              >
                See line pacing
              </Btn>
            </div>
          )}
        </div>

        {/* right: projected finish */}
        <div
          className="flex flex-col justify-center border-t-2 border-line-soft p-6 lg:border-l-2 lg:border-t-0"
          style={{
            background: "color-mix(in srgb, var(--surface-sunken) 60%, transparent)",
          }}
        >
          <div className="label text-[9.5px]">If the pace holds</div>
          {f.noData ? (
            <div className="mt-3.5 text-[13px] text-fg-faint">
              Projection available once data arrives.
            </div>
          ) : (
            <>
              <div className="mt-2.5 flex items-baseline gap-2">
                <span className="tnum font-display text-[38px] uppercase leading-[0.9] text-fg">
                  {formatCurrencyCompact(f.projectedFinal)}
                </span>
                <span className="font-mono text-[11px] text-fg-faint">
                  projected final
                </span>
              </div>
              <div className="mt-3">
                <DeltaChip f={f} />
              </div>
              <div className="mt-[18px] flex flex-col gap-[11px]">
                {(
                  [
                    ["Budget", formatCurrency(f.budget)],
                    ["Spent to date", formatCurrency(f.spend)],
                    f.exhaustEarlyDays > 0
                      ? ["Budget runs out", `${Math.abs(f.exhaustEarlyDays)}d early`]
                      : ["Days remaining", `${f.remaining}d`],
                    ["To land on budget", `${formatCurrency(Math.round(f.dailyNeeded))}/day`],
                  ] as Array<[string, string]>
                ).map(([k, val]) => (
                  <div
                    key={k}
                    className="flex items-baseline justify-between gap-3 text-[12.5px]"
                  >
                    <span className="text-fg-muted">{k}</span>
                    <span className="tnum font-mono font-semibold text-fg">
                      {val}
                    </span>
                  </div>
                ))}
              </div>
            </>
          )}
        </div>
      </div>
    </Card>
  );
}
