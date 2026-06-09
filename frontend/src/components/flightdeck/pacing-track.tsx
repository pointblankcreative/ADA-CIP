"use client";

/**
 * PacingTrack — the Flightdeck row hero. Budget = right edge.
 * Spent fill vs planned tick = over/under now; hatched ghost = projected
 * end-state. Ported from the prototype's flight.jsx.
 */
import type { Project } from "@/lib/api";
import type { FlightMath } from "@/lib/flight";
import { formatCurrencyCompact } from "@/lib/utils";

function trackColor(f: FlightMath): string {
  if (f.ended) return "var(--done)";
  if (f.noData) return "var(--info)";
  if (f.status === "on-track") return "var(--ok)";
  if (f.status.includes("critical")) return "var(--danger)";
  return "var(--warn)";
}

export function PacingTrack({
  p,
  f,
  height = 10,
  showCaps = true,
}: {
  p: Project;
  f: FlightMath;
  height?: number;
  showCaps?: boolean;
}) {
  const color = trackColor(f);
  const spent = Math.max(0, Math.min(100, f.spentPct));
  const planned = Math.max(0, Math.min(100, f.plannedPct));
  const proj = f.projPct == null ? null : Math.max(0, Math.min(100, f.projPct));
  const overBudget = f.projPct != null && f.projPct > 100.5;
  const ghostFrom = Math.min(spent, proj ?? spent);
  const ghostTo = proj == null ? spent : Math.max(spent, proj);

  return (
    <div className="w-full">
      <div
        className="relative w-full overflow-hidden rounded-pill bg-surface-sunken"
        style={{ height }}
      >
        {/* projection ghost (only the forward portion) */}
        {!f.ended && !f.noData && ghostTo > ghostFrom && (
          <div
            className="absolute bottom-0 top-0"
            style={{
              left: `${ghostFrom}%`,
              width: `${ghostTo - ghostFrom}%`,
              background: `repeating-linear-gradient(135deg, color-mix(in srgb, ${color} 34%, transparent) 0 4px, transparent 4px 8px)`,
            }}
          />
        )}
        {/* spent fill */}
        <div
          className="absolute bottom-0 left-0 top-0 rounded-pill transition-[width] duration-700 ease-snap"
          style={{ width: `${spent}%`, background: color }}
        />
        {/* planned-to-date tick */}
        {!f.ended && !f.noData && (
          <div
            title="Where spend should be today"
            className="absolute -bottom-px -top-px z-[3] w-0.5 bg-fg-secondary"
            style={{ left: `${planned}%` }}
          />
        )}
      </div>
      {showCaps && (
        <div className="mt-[5px] flex items-center justify-between font-mono text-[9.5px] text-fg-faint">
          <span
            className="font-semibold"
            style={{ color: f.ended ? "var(--text-faint)" : color }}
          >
            {formatCurrencyCompact(f.spend)} spent
          </span>
          {overBudget ? (
            <span className="font-bold text-danger">
              ↑ {formatCurrencyCompact(Math.abs(f.deltaAmount ?? 0))} over budget
            </span>
          ) : f.projPct != null && f.projPct < 96 ? (
            <span className="text-warn">
              ↓ {formatCurrencyCompact(Math.abs(f.deltaAmount ?? 0))} unspent risk
            </span>
          ) : (
            <span>budget {formatCurrencyCompact(f.budget)}</span>
          )}
        </div>
      )}
    </div>
  );
}
