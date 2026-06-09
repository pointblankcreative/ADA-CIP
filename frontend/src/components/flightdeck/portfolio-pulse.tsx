"use client";

/**
 * PortfolioPulse — the Flightdeck command bar: live portfolio numbers on
 * the left, the under↔over pacing-spread strip on the right. Ported from
 * the prototype's flightdeck.jsx + flight.jsx (PortfolioPacingStrip).
 */
import { useEffect, useMemo, useRef, useState } from "react";
import { TrendingDown, TrendingUp } from "lucide-react";
import type { Project } from "@/lib/api";
import { computeFlight } from "@/lib/flight";
import { Card } from "@/components/card";
import {
  formatCurrencyCompact,
  formatPercent,
  pacingStatus,
  pacingVar,
} from "@/lib/utils";

function PulseStat({
  label,
  value,
  sub,
  color,
}: {
  label: string;
  value: string;
  sub?: string;
  color?: string;
}) {
  return (
    <div className="min-w-0">
      <div className="label text-[9.5px]">{label}</div>
      <div
        className="tnum mt-1.5 font-display text-[34px] uppercase leading-none tracking-[0.01em]"
        style={{ color: color ?? "var(--text-primary)" }}
      >
        {value}
      </div>
      {sub && <div className="mt-[5px] text-[11.5px] text-fg-faint">{sub}</div>}
    </div>
  );
}

export function PortfolioPulse({
  active,
  onOpen,
}: {
  active: Project[];
  onOpen: (code: string) => void;
}) {
  const m = useMemo(() => {
    let budget = 0;
    let spend = 0;
    let burn = 0;
    let attention = 0;
    let live = 0;
    let liveProjected = 0;
    let liveBudget = 0;
    active.forEach((p) => {
      const f = computeFlight(p);
      budget += f.budget;
      spend += f.spend;
      if (!f.noData) {
        burn += f.dailyRate;
        live += 1;
        liveProjected += f.projectedFinal ?? 0;
        liveBudget += f.budget;
        if (f.status.includes("critical") || f.status.includes("warning")) {
          attention += 1;
        }
      }
    });
    return {
      budget,
      spend,
      burn,
      attention,
      live,
      deployed: budget > 0 ? (spend / budget) * 100 : 0,
      netDelta: liveProjected - liveBudget,
    };
  }, [active]);

  const over = m.netDelta > 0;

  return (
    <Card className="p-[22px] sm:p-6">
      <div className="grid items-stretch gap-7 lg:grid-cols-[minmax(0,1.05fr)_minmax(280px,1fr)]">
        {/* left: numbers */}
        <div>
          <div className="flex items-center gap-2.5">
            <span
              className="h-2 w-2 rounded-full bg-ok"
              style={{
                boxShadow: "0 0 0 4px color-mix(in srgb, var(--ok) 22%, transparent)",
              }}
            />
            <span className="eyebrow">Portfolio · live now</span>
          </div>
          <div className="mt-[18px] grid grid-cols-2 gap-x-[18px] gap-y-5 sm:grid-cols-4">
            <PulseStat
              label="Flights live"
              value={String(m.live)}
              sub={`${active.length} active total`}
            />
            <PulseStat
              label="Deployed"
              value={formatCurrencyCompact(m.spend)}
              sub={`${m.deployed.toFixed(0)}% of ${formatCurrencyCompact(m.budget)}`}
            />
            <PulseStat
              label="Daily burn"
              value={formatCurrencyCompact(m.burn)}
              sub="across live flights"
            />
            <PulseStat
              label="Need eyes"
              value={String(m.attention)}
              sub={m.attention ? "off-plan now" : "all on plan"}
              color={m.attention ? "var(--danger)" : "var(--ok)"}
            />
          </div>
          <div className="mt-5 flex items-center gap-2.5 border-t border-line-soft pt-4 text-[12.5px] text-fg-muted">
            {over ? (
              <TrendingUp className="h-[15px] w-[15px] text-warn" />
            ) : (
              <TrendingDown className="h-[15px] w-[15px] text-ok" />
            )}
            <span>
              On current pace the book finishes{" "}
              <strong style={{ color: over ? "var(--warn)" : "var(--ok)" }}>
                {over ? "+" : "−"}
                {formatCurrencyCompact(Math.abs(m.netDelta))}
              </strong>{" "}
              {over ? "over" : "under"} budget.
            </span>
          </div>
        </div>
        {/* right: distribution strip */}
        <div className="flex flex-col justify-center border-line-soft lg:border-l lg:pl-6">
          <div className="mb-1 flex items-baseline justify-between">
            <span className="label text-[9.5px]">Pacing spread</span>
            <span className="font-mono text-[9.5px] text-fg-faint">
              UNDER ← → OVER
            </span>
          </div>
          <PortfolioPacingStrip projects={active} onOpen={onOpen} />
          <div className="mt-1.5 text-[11px] text-fg-faint">
            Each dot is a live flight. The green band is on-plan. Click to open.
          </div>
        </div>
      </div>
    </Card>
  );
}

/* ── PortfolioPacingStrip — every live flight on one under↔over axis ── */

export function PortfolioPacingStrip({
  projects,
  onOpen,
  height = 84,
}: {
  projects: Project[];
  onOpen: (code: string) => void;
  height?: number;
}) {
  const ref = useRef<HTMLDivElement>(null);
  const [w, setW] = useState(720);
  const [hover, setHover] = useState<string | null>(null);

  useEffect(() => {
    if (!ref.current) return;
    const ro = new ResizeObserver((e) => setW(e[0].contentRect.width));
    ro.observe(ref.current);
    return () => ro.disconnect();
  }, []);

  const lo = 50;
  const hi = 150;
  const padL = 6;
  const padR = 6;
  const iw = Math.max(40, w - padL - padR);
  const x = (v: number) =>
    padL + ((Math.max(lo, Math.min(hi, v)) - lo) / (hi - lo)) * iw;
  const live = projects.filter((p) => p.pacing_percentage != null);
  const baseY = height - 22;
  const hovered = hover != null ? live.find((p) => p.project_code === hover) : null;

  return (
    <div ref={ref} className="relative w-full">
      <svg
        viewBox={`0 0 ${w} ${height}`}
        width="100%"
        height={height}
        className="block overflow-visible"
      >
        {/* zone bands */}
        <rect x={x(85)} y={6} width={x(115) - x(85)} height={baseY - 6} fill="var(--ok)" opacity="0.10" />
        <rect x={x(70)} y={6} width={x(85) - x(70)} height={baseY - 6} fill="var(--warn)" opacity="0.07" />
        <rect x={x(115)} y={6} width={x(130) - x(115)} height={baseY - 6} fill="var(--warn)" opacity="0.07" />
        <rect x={x(50)} y={6} width={x(70) - x(50)} height={baseY - 6} fill="var(--danger)" opacity="0.06" />
        <rect x={x(130)} y={6} width={x(150) - x(130)} height={baseY - 6} fill="var(--danger)" opacity="0.06" />
        {/* baseline */}
        <line x1={padL} y1={baseY} x2={w - padR} y2={baseY} stroke="var(--border-soft)" strokeWidth="1.5" />
        {/* 100% target */}
        <line x1={x(100)} y1={6} x2={x(100)} y2={baseY + 4} stroke="var(--ok)" strokeWidth="1.5" strokeDasharray="2 3" opacity="0.7" />
        {/* axis labels */}
        {(
          [
            [70, "70"],
            [85, "85"],
            [100, "100%"],
            [115, "115"],
            [130, "130"],
          ] as const
        ).map(([v, l]) => (
          <text
            key={v}
            x={x(v)}
            y={height - 4}
            textAnchor="middle"
            fontSize="9"
            fontFamily="var(--font-mono)"
            fill={v === 100 ? "var(--ok)" : "var(--text-faint)"}
          >
            {l}
          </text>
        ))}
        {/* campaign dots — jitter vertically by index to avoid overlap */}
        {live.map((p, i) => {
          const cy = 16 + (i % 4) * ((baseY - 26) / 3);
          const c = pacingVar(pacingStatus(p.pacing_percentage));
          const on = hover === p.project_code;
          const px = x(p.pacing_percentage as number);
          return (
            <g
              key={p.project_code}
              className="cursor-pointer"
              onMouseEnter={() => setHover(p.project_code)}
              onMouseLeave={() => setHover(null)}
              onClick={() => onOpen(p.project_code)}
            >
              <line x1={px} y1={cy} x2={px} y2={baseY} stroke={c} strokeWidth="1" opacity={on ? 0.5 : 0.22} />
              <circle cx={px} cy={cy} r={on ? 6 : 4.5} fill={c} stroke="var(--surface-card)" strokeWidth="1.5" />
            </g>
          );
        })}
      </svg>
      {hovered && (
        <div
          className="pointer-events-none absolute -top-2 z-[5] w-[168px] rounded-sm border-[1.5px] border-line bg-surface-up px-[11px] py-2 shadow-soft"
          style={{
            left: Math.min(
              Math.max(x(hovered.pacing_percentage as number) - 80, 0),
              w - 168
            ),
          }}
        >
          <div className="font-mono text-[10px] text-accent-ink">
            {hovered.project_code}
          </div>
          <div className="mt-0.5 text-[12.5px] font-bold leading-tight text-fg">
            {hovered.project_name}
          </div>
          <div
            className="mt-1 font-mono text-[11px] font-semibold"
            style={{ color: pacingVar(pacingStatus(hovered.pacing_percentage)) }}
          >
            {formatPercent(hovered.pacing_percentage)} paced
          </div>
        </div>
      )}
    </div>
  );
}
