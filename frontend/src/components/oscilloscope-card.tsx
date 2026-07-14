"use client";

/**
 * The pacing instrument — the Signal Lab's Orbit, replacing the old
 * oscilloscope: this campaign is the core, and its line items orbit it.
 * On-pace lines hold their shell; drifters wobble; broken lines judder
 * off-orbit. Hover a body to read it; click the stage for the 60-day
 * trend + line table.
 */
import { useEffect, useMemo, useRef, useState } from "react";
import {
  Area,
  ComposedChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  ReferenceLine,
  ReferenceArea,
} from "recharts";
import { ChevronDown } from "lucide-react";
import type { PacingResponse, PacingLine, PacingHistoryResponse } from "@/lib/api";
import { api } from "@/lib/api";
import { computeHealthScore, isLinePending } from "@/lib/oscilloscope";
import { lineSignalItems } from "@/lib/viz/health-core";
import {
  SignalTooltip,
  useOrbitInstrument,
} from "@/components/signals/instrument";
import { formatPercent, platformLabel, cn } from "@/lib/utils";
import { PacingBadge } from "@/components/pacing-badge";
import { Label } from "@/components/ui";

// ── Expanded: Historical Pacing Chart ───────────────────────────────

const TOOLTIP_STYLE = {
  background: "var(--surface-raised)",
  border: "1.5px solid var(--border)",
  borderRadius: "4px",
  fontSize: "0.75rem",
  color: "var(--text-primary)",
};

const ZONE_LABEL = {
  fontSize: 8.5,
  fontFamily: "var(--font-mono)",
  letterSpacing: "0.12em",
} as const;

function HistoryChart({
  code,
  asOfDate,
}: {
  code: string;
  /** Retrospective Mode (AI-070 bonus fix): anchor the trailing window at
   *  the replay date instead of today, so the expanded history chart never
   *  peeks past the as-of date. Undefined = live mode = anchored at today. */
  asOfDate?: string;
}) {
  const [history, setHistory] = useState<PacingHistoryResponse | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    api.pacing
      .history(code, 60, asOfDate)
      .then(setHistory)
      .catch(() => setHistory(null))
      .finally(() => setLoading(false));
  }, [code, asOfDate]);

  if (loading) {
    return (
      <div className="flex h-48 items-center justify-center text-sm text-fg-faint">
        Loading history...
      </div>
    );
  }

  if (!history || history.history.length === 0) {
    return (
      <div className="flex h-48 items-center justify-center text-sm text-fg-faint">
        No historical data yet. Pacing history builds daily.
      </div>
    );
  }

  // Aggregate by date: compute max, overall avg, min pacing
  const byDate = new Map<
    string,
    { max: number; sum: number; min: number; count: number }
  >();
  for (const row of history.history) {
    const d = byDate.get(row.date) || {
      max: -Infinity,
      sum: 0,
      min: Infinity,
      count: 0,
    };
    d.max = Math.max(d.max, row.pacing_percentage);
    d.min = Math.min(d.min, row.pacing_percentage);
    d.sum += row.pacing_percentage;
    d.count++;
    byDate.set(row.date, d);
  }

  const chartData = Array.from(byDate.entries())
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([date, d]) => ({
      date: date.slice(5), // "MM-DD"
      high: Math.round(d.max * 10) / 10,
      overall: Math.round((d.sum / d.count) * 10) / 10,
      low: Math.round(d.min * 10) / 10,
      // range series for the neutral high–low envelope
      band: [Math.round(d.min * 10) / 10, Math.round(d.max * 10) / 10] as [number, number],
    }));

  return (
    <div className="h-64">
      <ResponsiveContainer width="100%" height="100%">
        <ComposedChart data={chartData}>
          <CartesianGrid strokeDasharray="3 3" stroke="var(--border-soft)" />
          <XAxis
            dataKey="date"
            tick={{ fontSize: 10, fill: "var(--text-faint)", fontFamily: "var(--font-mono)" }}
            interval="preserveStartEnd"
          />
          <YAxis
            domain={[50, 150]}
            tick={{ fontSize: 10, fill: "var(--text-faint)", fontFamily: "var(--font-mono)" }}
            tickFormatter={(v: number) => `${v}%`}
          />
          <Tooltip contentStyle={TOOLTIP_STYLE} formatter={(v: number) => `${v}%`} />

          {/* Threshold bands — the zone edges read at a glance */}
          <ReferenceArea
            y1={130}
            y2={150}
            fill="var(--danger)"
            fillOpacity={0.11}
            label={{ value: "OVER PACE", position: "insideRight", fill: "var(--danger)", opacity: 0.7, ...ZONE_LABEL }}
          />
          <ReferenceArea y1={115} y2={130} fill="var(--warn)" fillOpacity={0.13} />
          <ReferenceArea
            y1={85}
            y2={115}
            fill="var(--ok)"
            fillOpacity={0.15}
            label={{ value: "ON PACE", position: "insideRight", fill: "var(--ok)", opacity: 0.7, ...ZONE_LABEL }}
          />
          <ReferenceArea y1={70} y2={85} fill="var(--warn)" fillOpacity={0.13} />
          <ReferenceArea
            y1={50}
            y2={70}
            fill="var(--danger)"
            fillOpacity={0.11}
            label={{ value: "UNDER PACE", position: "insideRight", fill: "var(--danger)", opacity: 0.7, ...ZONE_LABEL }}
          />
          <ReferenceLine y={85} stroke="var(--ok)" strokeWidth={1} opacity={0.35} />
          <ReferenceLine y={115} stroke="var(--ok)" strokeWidth={1} opacity={0.35} />

          {/* 100% reference line — the exact-to-plan mark */}
          <ReferenceLine
            y={100}
            stroke="var(--text-faint)"
            strokeDasharray="4 4"
            strokeWidth={1}
            label={{ value: "100% = ON PLAN", position: "insideBottomLeft", fill: "var(--text-muted)", ...ZONE_LABEL }}
          />

          {/* High–low envelope — deliberately NEUTRAL: the spread is a
              range, not a status, so it never borrows green/amber/red. */}
          <Area
            type="monotone"
            dataKey="band"
            stroke="none"
            fill="var(--text-faint)"
            fillOpacity={0.13}
            name="Spread"
            tooltipType="none"
          />
          <Line type="monotone" dataKey="high" stroke="var(--text-faint)" strokeWidth={1} strokeDasharray="2 4" dot={false} name="Highest line" />
          <Line type="monotone" dataKey="low" stroke="var(--text-faint)" strokeWidth={1} strokeDasharray="2 4" dot={false} name="Lowest line" />
          <Line type="monotone" dataKey="overall" stroke="var(--text-primary)" strokeWidth={2.2} dot={false} name="Overall" />
        </ComposedChart>
      </ResponsiveContainer>
      <div className="mt-2 flex gap-[18px]">
        <span className="inline-flex items-center gap-1.5 font-mono text-[10.5px] text-fg-muted">
          <span className="h-[2.5px] w-3.5 bg-fg" />
          Overall pacing
        </span>
        <span className="inline-flex items-center gap-1.5 font-mono text-[10.5px] text-fg-muted">
          <span
            className="h-2 w-3.5 border border-dashed border-fg-faint"
            style={{ background: "color-mix(in srgb, var(--text-faint) 18%, transparent)" }}
          />
          Spread across lines (high–low)
        </span>
      </div>
    </div>
  );
}

// ── Main card: the Pacing Signal orbit ─────────────────────────────

export function OscilloscopeCard({
  pacing,
  code,
  asOfDate,
  onHover,
}: {
  pacing: PacingResponse;
  code: string;
  /** Set in Retrospective Mode — passed through to the history chart so its
   *  trailing window anchors at the replay date, not today. */
  asOfDate?: string;
  /** Bubbles the hovered line_id so the pacing list can glow its row. */
  onHover?: (lineId: string | null) => void;
}) {
  const [expanded, setExpanded] = useState(false);
  const health = computeHealthScore(pacing.lines);
  const allPending =
    pacing.lines.length > 0 && pacing.lines.every(isLinePending);

  const healthLabel = allPending
    ? "No signal"
    : health > 0.85
      ? "On pace"
      : health > 0.5
        ? "Drifting"
        : "Off pace";
  const healthColor = allPending
    ? "var(--info)"
    : health > 0.85
      ? "var(--ok)"
      : health > 0.5
        ? "var(--warn)"
        : "var(--danger)";

  const items = useMemo(() => lineSignalItems(pacing.lines), [pacing.lines]);
  const itemsRef = useRef(items);
  itemsRef.current = items;
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const [hoverId, setHoverId] = useState<string | null>(null);
  const [pos, setPos] = useState<{ x: number; y: number } | null>(null);
  const [bounds, setBounds] = useState({ w: 0, h: 0 });
  const hoverRef = useRef<string | null>(null);
  hoverRef.current = hoverId;
  const vizRef = useOrbitInstrument(canvasRef, itemsRef, hoverRef, true);

  useEffect(() => {
    onHover?.(hoverId); // light up the matching line row below
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [hoverId]);

  const onMove = (e: React.MouseEvent<HTMLCanvasElement>) => {
    const canvas = canvasRef.current;
    const viz = vizRef.current;
    if (!canvas || !viz) return;
    const r = canvas.getBoundingClientRect();
    const x = e.clientX - r.left;
    const y = e.clientY - r.top;
    setHoverId(viz.hitTest(x, y));
    setPos({ x, y });
    setBounds({ w: r.width, h: r.height });
  };
  const hov = items.find((i) => i.id === hoverId);

  return (
    <div className="overflow-hidden rounded-md border-2 border-line-soft">
      {/* Collapsed: the orbit stage. Click anywhere for trend + lines. */}
      <div
        onClick={() => setExpanded(!expanded)}
        className="relative cursor-pointer"
        style={{ height: 210 }}
        role="button"
        aria-label="Toggle pacing history"
        aria-expanded={expanded}
      >
        <div
          className="absolute inset-0"
          style={{
            background:
              "radial-gradient(120% 100% at 50% 0%, var(--surface-card), var(--surface-sunken))",
          }}
        >
          <canvas
            ref={canvasRef}
            onMouseMove={onMove}
            onMouseLeave={() => setHoverId(null)}
            className="block h-full w-full"
          />
        </div>
        <SignalTooltip item={hov} pos={pos} bounds={bounds} />
        {/* top labels */}
        <div className="pointer-events-none absolute inset-x-[18px] top-3.5 flex items-start justify-between">
          <div>
            <div className="eyebrow">Pacing Signal</div>
            <div className="mt-2 flex items-center gap-2.5">
              <span
                className="font-display text-[22px] uppercase leading-none tracking-[0.01em]"
                style={{ color: healthColor }}
              >
                {healthLabel}
              </span>
              <span className="font-mono text-[11.5px] text-fg-muted">
                {allPending
                  ? "lines pending activation"
                  : `${formatPercent(pacing.overall_pacing_percentage)} paced`}
              </span>
            </div>
          </div>
          <div className="pointer-events-auto flex items-center gap-3">
            <span
              className={cn(
                "font-mono text-[10px] tracking-[0.04em]",
                hov ? "text-fg-secondary" : "text-fg-faint"
              )}
            >
              {hov
                ? `${hov.code} · ${hov.label} · ${hov.pct != null ? hov.pct.toFixed(0) + "%" : "no data"}`
                : `${items.length} lines in flight · hover to read`}
            </span>
          </div>
        </div>
        {/* expand hint */}
        <div className="pointer-events-none absolute bottom-3 right-[18px] inline-flex items-center gap-1.5 text-fg-faint">
          <span className="font-mono text-[10px] tracking-[0.1em]">
            {expanded ? "HIDE" : "TREND + LINES"}
          </span>
          <ChevronDown
            className={cn(
              "h-[15px] w-[15px] transition-transform duration-base",
              expanded && "rotate-180"
            )}
          />
        </div>
      </div>

      {/* Expanded view */}
      {expanded && (
        <div className="space-y-6 border-t-2 border-line-soft bg-surface-card p-4 sm:p-5">
          {/* Historical trend chart */}
          <div>
            <Label className="mb-3.5">Pacing Trend · 60 days</Label>
            <HistoryChart code={code} asOfDate={asOfDate} />
          </div>

          {/* Per-line detail table */}
          <div className="pt-2">
            <Label className="mb-3">Line Breakdown</Label>
            <div className="overflow-x-auto">
              <table className="w-full min-w-[520px] border-collapse text-left text-[12.5px]">
                <thead>
                  <tr>
                    {["Line", "Platform", "Flight", "Pacing", "Status"].map(
                      (h, i) => (
                        <th
                          key={h}
                          className={cn(
                            "whitespace-nowrap pb-2.5 pr-3 font-mono text-[10px] font-medium uppercase tracking-[0.12em] text-fg-faint",
                            i > 2 && "text-right"
                          )}
                        >
                          {h}
                        </th>
                      )
                    )}
                  </tr>
                </thead>
                <tbody>
                  {pacing.lines.map((line: PacingLine) => (
                    <tr key={line.line_id} className="border-t border-line-soft">
                      <td className="max-w-[200px] truncate py-2.5 pr-3 text-fg-secondary">
                        {line.audience_name ||
                          line.channel_category ||
                          line.line_id.split("-").pop()}
                      </td>
                      <td className="py-2.5 pr-3 text-fg-muted">
                        {platformLabel(line.platform_id)}
                      </td>
                      <td className="whitespace-nowrap py-2.5 pr-3 font-mono text-[11px] text-fg-faint">
                        {line.flight_start && line.flight_end
                          ? `${line.flight_start.slice(5)} — ${line.flight_end.slice(5)}`
                          : "—"}
                      </td>
                      <td className="tnum py-2.5 pr-3 text-right font-mono font-semibold text-fg-secondary">
                        {isLinePending(line)
                          ? "—"
                          : formatPercent(line.pacing_percentage)}
                      </td>
                      <td className="py-2.5 text-right">
                        <PacingBadge
                          percentage={line.pacing_percentage}
                          lineStatus={line.line_status}
                          variant="label"
                          size="sm"
                        />
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
