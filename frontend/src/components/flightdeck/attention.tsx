"use client";

/**
 * Needs Attention — the triage cards at the top of the Flightdeck.
 * The worst offender gets the full-width feature card; the next three
 * get compact tiles. Ported from the prototype's flightdeck.jsx.
 */
import { Gauge, TrendingUp, Activity, type LucideIcon } from "lucide-react";
import type { Project } from "@/lib/api";
import type { FlightMath, Verdict, VerdictIcon } from "@/lib/flight";
import { Card } from "@/components/card";
import { CodeChip, Btn } from "@/components/ui";
import { PacingTrack } from "@/components/flightdeck/pacing-track";
import { formatCurrency, formatPercent } from "@/lib/utils";

const VERDICT_ICONS: Record<VerdictIcon, LucideIcon> = {
  gauge: Gauge,
  "trending-up": TrendingUp,
  activity: Activity,
};

export function AttentionFeature({
  p,
  f,
  v,
  onOpen,
}: {
  p: Project;
  f: FlightMath;
  v: Verdict;
  onOpen: (code: string) => void;
}) {
  const ActionIcon = v.action ? VERDICT_ICONS[v.action.icon] : null;
  return (
    <Card
      className="relative col-span-full cursor-pointer overflow-hidden p-[22px] transition-transform duration-base ease-snap hover:-translate-y-0.5"
      // status-tinted border + wash — inline because the tone is dynamic
      // (color-mix over CSS vars; tokens only, no raw hex)
      style={{
        borderColor: `color-mix(in srgb, ${v.tone} 45%, transparent)`,
        background: `linear-gradient(180deg, color-mix(in srgb, ${v.tone} 7%, var(--surface-card)), var(--surface-card))`,
      }}
      onClick={() => onOpen(p.project_code)}
    >
      <div className="grid items-center gap-[26px] lg:grid-cols-[minmax(0,1.4fr)_minmax(220px,1fr)]">
        <div className="min-w-0">
          <div className="flex flex-wrap items-center gap-2.5">
            <span
              className="font-display text-3xl uppercase leading-[0.9] tracking-[0.01em]"
              style={{ color: v.tone }}
            >
              {v.word}
            </span>
            <CodeChip>{p.project_code}</CodeChip>
          </div>
          <h3 className="mt-3 text-[21px] font-extrabold leading-[1.15] tracking-tight text-fg">
            {p.project_name}
          </h3>
          {p.client_name && (
            <div className="mt-[5px] font-mono text-[11px] uppercase tracking-[0.06em] text-fg-meta">
              {p.client_name}
            </div>
          )}
          <p className="mt-3.5 max-w-[560px] text-sm leading-normal text-fg-secondary">
            {v.detail}
          </p>
          {v.action && ActionIcon && (
            <div className="mt-4">
              <Btn
                variant="primary"
                size="sm"
                icon={<ActionIcon className="h-3.5 w-3.5" />}
              >
                {v.action.label}
              </Btn>
            </div>
          )}
        </div>
        <div className="border-line-soft lg:border-l lg:pl-6">
          <div className="flex items-baseline gap-2">
            <span
              className="tnum font-display text-[44px] leading-[0.9]"
              style={{ color: v.tone }}
            >
              {formatPercent(p.pacing_percentage)}
            </span>
            <span className="font-mono text-[11px] text-fg-faint">paced</span>
          </div>
          <div className="mt-4">
            <PacingTrack p={p} f={f} height={12} />
          </div>
          <div className="mt-4 flex flex-wrap gap-x-4 gap-y-1 font-mono text-[11px] text-fg-muted">
            <span>{f.remaining}d left</span>
            <span className="font-semibold" style={{ color: v.tone }}>
              {formatCurrency(Math.round(f.dailyNeeded))}/day to land
            </span>
          </div>
        </div>
      </div>
    </Card>
  );
}

export function AttentionTile({
  p,
  f,
  v,
  onOpen,
}: {
  p: Project;
  f: FlightMath;
  v: Verdict;
  onOpen: (code: string) => void;
}) {
  return (
    <Card
      className="flex cursor-pointer flex-col gap-3 p-[17px] transition-transform duration-base ease-snap hover:-translate-y-0.5"
      style={{
        borderColor: `color-mix(in srgb, ${v.tone} 38%, transparent)`,
      }}
      onClick={() => onOpen(p.project_code)}
    >
      <div className="flex items-start justify-between gap-2.5">
        <span
          className="font-display text-lg uppercase tracking-[0.01em]"
          style={{ color: v.tone }}
        >
          {v.word}
        </span>
        <span
          className="tnum font-mono text-[15px] font-bold"
          style={{ color: v.tone }}
        >
          {formatPercent(p.pacing_percentage)}
        </span>
      </div>
      <div>
        <div className="text-[15px] font-bold leading-tight text-fg">
          {p.project_name}
        </div>
        <div className="mt-1 font-mono text-[10.5px] uppercase tracking-[0.06em] text-fg-meta">
          {p.project_code}
          {p.client_name ? ` · ${p.client_name}` : ""}
        </div>
      </div>
      <p className="m-0 flex-1 text-[12.5px] leading-snug text-fg-muted">
        {v.detail}
      </p>
      <PacingTrack p={p} f={f} height={8} showCaps={false} />
    </Card>
  );
}
