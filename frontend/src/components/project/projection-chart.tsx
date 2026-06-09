"use client";

/**
 * ProjectionChart — cumulative spend vs plan, projected to flight end.
 * Bespoke SVG (no chart lib): plan line, actual area, dashed projection,
 * budget ceiling, TODAY marker, hover crosshair. Ported from flight.jsx.
 */
import { useEffect, useMemo, useRef, useState } from "react";
import type { DailyPerformance, Project } from "@/lib/api";
import type { FlightMath } from "@/lib/flight";
import { formatCurrencyCompact, pacingVar } from "@/lib/utils";

export function ProjectionChart({
  p,
  f,
  daily,
  height = 240,
}: {
  p: Project;
  f: FlightMath;
  daily: DailyPerformance[] | null;
  height?: number;
}) {
  const ref = useRef<HTMLDivElement>(null);
  const [w, setW] = useState(680);
  const [hover, setHover] = useState<number | null>(null);

  useEffect(() => {
    if (!ref.current) return;
    const ro = new ResizeObserver((e) => setW(e[0].contentRect.width));
    ro.observe(ref.current);
    return () => ro.disconnect();
  }, []);

  // Cumulative actual, scaled so the last point equals total spend (the
  // daily series may not cover the whole flight or include untracked spend).
  const series = useMemo(() => {
    const arr = daily && daily.length ? daily : [{ spend: f.spend }];
    let cum = 0;
    const raw = arr.map((d) => {
      cum += d.spend ?? 0;
      return cum;
    });
    const last = raw[raw.length - 1];
    const scale = last > 0 ? f.spend / last : 0;
    return raw.map((v, i) => ({
      t: (i / (raw.length - 1 || 1)) * f.elapsed,
      y: v * scale,
    }));
  }, [daily, f.spend, f.elapsed]);

  const padL = 46;
  const padR = 14;
  const padT = 14;
  const padB = 26;
  const iw = Math.max(40, w - padL - padR);
  const ih = height - padT - padB;
  const xMax = f.flightTotal;
  const yMax = Math.max(f.budget, f.projectedFinal ?? 0) * 1.08 || 1;
  const X = (t: number) => padL + (t / xMax) * iw;
  const Y = (v: number) => padT + (1 - v / yMax) * ih;

  const statusColor = pacingVar(f.status);
  const planPath = `M${X(0)} ${Y(0)} L${X(xMax)} ${Y(f.budget)}`;
  const actualPath = series
    .map((d, i) => (i ? "L" : "M") + X(d.t).toFixed(1) + " " + Y(d.y).toFixed(1))
    .join(" ");
  const projPath =
    f.projectedFinal == null
      ? ""
      : `M${X(f.elapsed)} ${Y(f.spend)} L${X(xMax)} ${Y(f.projectedFinal)}`;
  const todayX = X(f.elapsed);
  const yTicks = [0.25, 0.5, 0.75, 1].map((fr) => fr * f.budget);

  return (
    <div ref={ref} className="relative">
      <svg
        viewBox={`0 0 ${w} ${height}`}
        width="100%"
        height={height}
        className="block"
        onMouseMove={(e) => {
          const r = e.currentTarget.getBoundingClientRect();
          const t = Math.max(
            0,
            Math.min(f.elapsed, ((e.clientX - r.left - padL) / iw) * xMax)
          );
          setHover(t);
        }}
        onMouseLeave={() => setHover(null)}
      >
        {/* budget ceiling */}
        <line x1={padL} y1={Y(f.budget)} x2={w - padR} y2={Y(f.budget)} stroke="var(--danger)" strokeWidth="1.2" strokeDasharray="4 4" opacity="0.55" />
        <text x={w - padR} y={Y(f.budget) - 6} textAnchor="end" fontSize="9.5" fontFamily="var(--font-mono)" fill="var(--danger)">
          BUDGET {formatCurrencyCompact(f.budget)}
        </text>
        {/* y grid */}
        {yTicks.map((v, i) => (
          <g key={i}>
            <line x1={padL} y1={Y(v)} x2={w - padR} y2={Y(v)} stroke="var(--border-soft)" strokeWidth="1" opacity="0.4" />
            <text x={padL - 8} y={Y(v) + 3} textAnchor="end" fontSize="9" fontFamily="var(--font-mono)" fill="var(--text-faint)">
              {formatCurrencyCompact(v)}
            </text>
          </g>
        ))}
        {/* plan line */}
        <path d={planPath} fill="none" stroke="var(--text-faint)" strokeWidth="1.5" strokeDasharray="5 4" />
        {/* projection */}
        {projPath && (
          <path d={projPath} fill="none" stroke={statusColor} strokeWidth="2" strokeDasharray="2 4" opacity="0.85" />
        )}
        {/* today marker */}
        <line x1={todayX} y1={padT} x2={todayX} y2={padT + ih} stroke="var(--text-muted)" strokeWidth="1" opacity="0.45" />
        <text x={todayX} y={padT + 2} textAnchor="middle" fontSize="8.5" fontFamily="var(--font-mono)" fill="var(--text-muted)">
          TODAY
        </text>
        {/* actual area + line */}
        <path
          d={`${actualPath} L${X(f.elapsed)} ${Y(0)} L${X(0)} ${Y(0)} Z`}
          fill="var(--accent)"
          opacity="0.10"
        />
        <path d={actualPath} fill="none" stroke="var(--accent-ink)" strokeWidth="2.4" strokeLinejoin="round" vectorEffect="non-scaling-stroke" />
        {/* projected-final dot */}
        {f.projectedFinal != null && (
          <circle cx={X(xMax)} cy={Y(f.projectedFinal)} r="4" fill={statusColor} stroke="var(--surface-card)" strokeWidth="1.5" />
        )}
        {/* x labels */}
        <text x={X(0)} y={height - 6} textAnchor="start" fontSize="9" fontFamily="var(--font-mono)" fill="var(--text-faint)">
          {p.start_date?.slice(5)}
        </text>
        <text x={X(xMax)} y={height - 6} textAnchor="end" fontSize="9" fontFamily="var(--font-mono)" fill="var(--text-faint)">
          {p.end_date?.slice(5)}
        </text>
        {hover != null &&
          (() => {
            // interpolate actual at hovered t
            let yv = 0;
            for (let i = 1; i < series.length; i++) {
              if (series[i].t >= hover) {
                const a = series[i - 1];
                const b = series[i];
                const r = (hover - a.t) / (b.t - a.t || 1);
                yv = a.y + (b.y - a.y) * r;
                break;
              }
              yv = series[i].y;
            }
            const planV = f.budget * (hover / xMax);
            return (
              <g>
                <line x1={X(hover)} y1={padT} x2={X(hover)} y2={padT + ih} stroke="var(--text-secondary)" strokeWidth="1" />
                <circle cx={X(hover)} cy={Y(yv)} r="3.5" fill="var(--accent-ink)" stroke="var(--surface-card)" strokeWidth="1.5" />
                <circle cx={X(hover)} cy={Y(planV)} r="3" fill="var(--text-faint)" />
              </g>
            );
          })()}
      </svg>
      {/* legend */}
      <div className="mt-2 flex flex-wrap gap-x-4 gap-y-1">
        {(
          [
            ["Actual spend", "var(--accent-ink)", false],
            ["Plan", "var(--text-faint)", true],
            ["Projection", statusColor, true],
          ] as Array<[string, string, boolean]>
        ).map(([k, c, dash]) => (
          <span
            key={k}
            className="inline-flex items-center gap-1.5 font-mono text-[10px] text-fg-muted"
          >
            <span
              className="inline-block h-0 w-4"
              style={{ borderTop: `2px ${dash ? "dashed" : "solid"} ${c}` }}
            />
            {k}
          </span>
        ))}
      </div>
    </div>
  );
}
