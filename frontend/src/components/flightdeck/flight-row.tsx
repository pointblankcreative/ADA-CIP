"use client";

/**
 * FlightRow — dense one-line campaign row for the Flightdeck board.
 * Ported from the prototype's flightdeck.jsx, minus the hide-from-board
 * affordance (visibility is managed in BigQuery via dismiss/archive flags).
 */
import { useState } from "react";
import { ChevronRight } from "lucide-react";
import type { Project } from "@/lib/api";
import { computeFlight, flightDotColor } from "@/lib/flight";
import { PacingTrack } from "@/components/flightdeck/pacing-track";
import { StatusPill } from "@/components/ui";
import { cn, formatCurrencyCompact, formatPercent } from "@/lib/utils";

const ROW_GRID =
  "grid-cols-[10px_minmax(0,1fr)_110px_24px] md:grid-cols-[10px_minmax(190px,1.3fr)_minmax(190px,2fr)_132px_24px]";

export function FlightRowHeader({ ended }: { ended: boolean }) {
  return (
    <div
      className={cn(
        "grid items-end gap-[18px] px-[18px] pb-[9px]",
        ROW_GRID,
        "hidden md:grid"
      )}
    >
      <span />
      <span className="font-mono text-[9.5px] uppercase tracking-[0.12em] text-fg-faint">
        Campaign
      </span>
      <span className="flex justify-between font-mono text-[9.5px] uppercase tracking-[0.12em] text-fg-faint">
        <span>Spent</span>
        <span className="inline-flex items-center gap-[5px]">
          <span className="h-0.5 w-[11px] bg-fg-secondary" />
          plan today
        </span>
      </span>
      <span className="text-right font-mono text-[9.5px] uppercase tracking-[0.12em] text-fg-faint">
        {ended ? "Final" : "Pace / run-rate"}
      </span>
      <span />
    </div>
  );
}

export function FlightRow({
  p,
  onOpen,
  delay = 0,
  glow = false,
}: {
  p: Project;
  onOpen: (code: string) => void;
  delay?: number;
  /** This campaign is hovered in the Signals orbit — light the row in its
   *  status colour. */
  glow?: boolean;
}) {
  const [hovered, setHovered] = useState(false);
  const f = computeFlight(p);
  const color = flightDotColor(p, f);

  return (
    <div
      onClick={() => onOpen(p.project_code)}
      onMouseEnter={() => setHovered(true)}
      onMouseLeave={() => setHovered(false)}
      className={cn(
        "grid cursor-pointer items-center gap-3 rounded-md border-2 px-[18px] py-[15px] md:gap-[18px]",
        ROW_GRID,
        "transition-colors duration-fast",
        hovered ? "border-line bg-surface-up" : "border-line-soft bg-surface-card"
      )}
      style={{
        animation: `fade-up 0.36s cubic-bezier(0.2,0,0,1) ${delay * 0.025}s both`,
        ...(glow
          ? {
              borderColor: color,
              boxShadow: `0 0 0 1.5px ${color}, 0 0 26px color-mix(in srgb, ${color} 28%, transparent)`,
            }
          : {}),
      }}
    >
      {/* status dot */}
      <span
        className="h-[9px] w-[9px] rounded-full transition-shadow duration-fast"
        style={{
          backgroundColor: color,
          boxShadow: hovered
            ? `0 0 0 4px color-mix(in srgb, ${color} 20%, transparent)`
            : "none",
        }}
      />
      {/* identity */}
      <div className="min-w-0">
        <div className="flex items-center gap-2">
          <span className="font-mono text-[10.5px] tracking-[0.02em] text-accent-ink">
            {p.project_code}
          </span>
          {f.noData && !f.ended && (
            <StatusPill label="Awaiting" color="var(--info)" size="sm" />
          )}
        </div>
        <div className="mt-[3px] truncate text-[14.5px] font-bold leading-tight tracking-tight text-fg">
          {p.project_name}
        </div>
        {p.client_name && (
          <div className="mt-0.5 truncate font-mono text-[10px] uppercase tracking-[0.06em] text-fg-meta">
            {p.client_name}
          </div>
        )}
      </div>
      {/* pacing track — hidden on small screens */}
      <div className="hidden min-w-0 md:block">
        <PacingTrack p={p} f={f} height={9} />
      </div>
      {/* numbers */}
      <div className="text-right">
        {f.ended ? (
          <>
            <div className="tnum text-[17px] font-bold" style={{ color }}>
              {formatPercent(p.pacing_percentage)}
            </div>
            <div className="mt-0.5 font-mono text-[10px] text-fg-faint">
              final · ended {p.end_date?.slice(5)}
            </div>
          </>
        ) : f.noData ? (
          <>
            <div className="tnum text-[17px] font-bold text-info">—</div>
            <div className="mt-0.5 font-mono text-[10px] text-fg-faint">
              opens {p.start_date?.slice(5)}
            </div>
          </>
        ) : (
          <>
            <div className="tnum text-[17px] font-bold" style={{ color }}>
              {formatPercent(p.pacing_percentage)}
            </div>
            <div className="mt-0.5 font-mono text-[10px] text-fg-faint">
              {f.remaining}d · {formatCurrencyCompact(Math.round(f.dailyNeeded))}
              /day
            </div>
          </>
        )}
      </div>
      {/* chevron */}
      <div className="flex items-center justify-end">
        <ChevronRight
          className={cn(
            "h-[17px] w-[17px] transition-colors duration-fast",
            hovered ? "text-accent-ink" : "text-fg-faint"
          )}
        />
      </div>
    </div>
  );
}
