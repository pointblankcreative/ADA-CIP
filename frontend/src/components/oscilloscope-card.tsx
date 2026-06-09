"use client";

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import {
  LineChart,
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
import {
  computeHealthScore,
  extractChannels,
  generateWavePath,
  pacingToColor,
  isLinePending,
} from "@/lib/oscilloscope";
import { formatPercent, platformLabel, cn } from "@/lib/utils";
import { PacingBadge } from "@/components/pacing-badge";
import { Label } from "@/components/ui";

// ── Collapsed: Animated Oscilloscope SVG ────────────────────────────

function OscilloscopeSVG({
  lines,
  overallPct,
  width = 320,
  height = 120,
}: {
  lines: PacingLine[];
  overallPct: number;
  width?: number;
  height?: number;
}) {
  const svgRef = useRef<SVGSVGElement>(null);
  const pathRefs = [
    useRef<SVGPathElement>(null),
    useRef<SVGPathElement>(null),
    useRef<SVGPathElement>(null),
  ];
  const glowRefs = [
    useRef<SVGPathElement>(null),
    useRef<SVGPathElement>(null),
    useRef<SVGPathElement>(null),
  ];
  const animRef = useRef<number>(0);
  const startRef = useRef<number>(0);

  const health = computeHealthScore(lines);
  const channels = extractChannels(lines, overallPct);
  const colors = channels.map((c) => pacingToColor(c.pct));

  const bandH = height / 3;
  const centers = useMemo(
    () => [bandH * 0.5, bandH * 1.5, bandH * 2.5],
    [bandH]
  );

  const animate = useCallback(
    (timestamp: number) => {
      if (!startRef.current) startRef.current = timestamp;
      const t = (timestamp - startRef.current) / 1000;

      for (let i = 0; i < 3; i++) {
        const d = generateWavePath(i, health, t, width, centers[i], bandH);
        pathRefs[i].current?.setAttribute("d", d);
        glowRefs[i].current?.setAttribute("d", d);
      }

      animRef.current = requestAnimationFrame(animate);
    },
    [health, width, bandH, centers]
  );

  useEffect(() => {
    animRef.current = requestAnimationFrame(animate);
    return () => cancelAnimationFrame(animRef.current);
  }, [animate]);

  // Grid lines for oscilloscope aesthetic
  const hLines = [0.25, 0.5, 0.75].map((f) => f * height);
  const vLines = Array.from({ length: 7 }, (_, i) => ((i + 1) / 8) * width);

  return (
    <svg
      ref={svgRef}
      viewBox={`0 0 ${width} ${height}`}
      className="h-full w-full"
      preserveAspectRatio="none"
    >
      <defs>
        <filter id="osc-glow">
          <feGaussianBlur stdDeviation="3" result="blur" />
          <feMerge>
            <feMergeNode in="blur" />
            <feMergeNode in="SourceGraphic" />
          </feMerge>
        </filter>
      </defs>

      {/* Grid */}
      {hLines.map((y) => (
        <line key={`h-${y}`} x1={0} y1={y} x2={width} y2={y} stroke="var(--chart-dim)" strokeWidth={0.5} opacity={0.2} />
      ))}
      {vLines.map((x) => (
        <line key={`v-${x}`} x1={x} y1={0} x2={x} y2={height} stroke="var(--chart-dim)" strokeWidth={0.5} opacity={0.16} />
      ))}

      {/* Glow layer (blurred duplicates) */}
      {[0, 1, 2].map((i) => (
        <path
          key={`glow-${i}`}
          ref={glowRefs[i]}
          d=""
          fill="none"
          stroke={colors[i]}
          strokeWidth={3}
          opacity={0.2}
          filter="url(#osc-glow)"
        />
      ))}

      {/* Sharp wave paths */}
      {[0, 1, 2].map((i) => (
        <path
          key={`wave-${i}`}
          ref={pathRefs[i]}
          d=""
          fill="none"
          stroke={colors[i]}
          strokeWidth={1.5}
          strokeLinecap="round"
          strokeLinejoin="round"
        />
      ))}
    </svg>
  );
}

// ── Expanded: Historical Pacing Chart ───────────────────────────────

const TOOLTIP_STYLE = {
  background: "var(--surface-raised)",
  border: "1.5px solid var(--border)",
  borderRadius: "4px",
  fontSize: "0.75rem",
  color: "var(--text-primary)",
};

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
    }));

  return (
    <div className="h-64">
      <ResponsiveContainer width="100%" height="100%">
        <LineChart data={chartData}>
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

          {/* Threshold bands */}
          <ReferenceArea y1={85} y2={115} fill="var(--ok)" fillOpacity={0.06} />
          <ReferenceArea y1={70} y2={85} fill="var(--warn)" fillOpacity={0.05} />
          <ReferenceArea y1={115} y2={130} fill="var(--warn)" fillOpacity={0.05} />

          {/* 100% reference line */}
          <ReferenceLine
            y={100}
            stroke="var(--text-faint)"
            strokeDasharray="4 4"
            strokeWidth={1}
          />

          <Line type="monotone" dataKey="high" stroke="var(--danger)" strokeWidth={1.5} dot={false} name="Highest line" />
          <Line type="monotone" dataKey="overall" stroke="var(--ok)" strokeWidth={2} dot={false} name="Overall" />
          <Line type="monotone" dataKey="low" stroke="var(--info)" strokeWidth={1.5} dot={false} name="Lowest line" />
        </LineChart>
      </ResponsiveContainer>
      <div className="mt-2 flex gap-[18px]">
        {(
          [
            ["Overall", "var(--ok)"],
            ["Highest line", "var(--danger)"],
            ["Lowest line", "var(--info)"],
          ] as Array<[string, string]>
        ).map(([k, c]) => (
          <span
            key={k}
            className="inline-flex items-center gap-1.5 font-mono text-[10.5px] text-fg-muted"
          >
            <span className="h-[2.5px] w-3.5" style={{ background: c }} />
            {k}
          </span>
        ))}
      </div>
    </div>
  );
}

// ── Main Oscilloscope Card ──────────────────────────────────────────

export function OscilloscopeCard({
  pacing,
  code,
  asOfDate,
}: {
  pacing: PacingResponse;
  code: string;
  /** Set in Retrospective Mode — passed through to the history chart so its
   *  trailing window anchors at the replay date, not today. */
  asOfDate?: string;
}) {
  const [expanded, setExpanded] = useState(false);
  const health = computeHealthScore(pacing.lines);
  const channels = extractChannels(pacing.lines, pacing.overall_pacing_percentage);

  const allPending = pacing.lines.length > 0 && pacing.lines.every(isLinePending);

  const healthLabel = allPending
    ? "Awaiting Data"
    : health > 0.85
      ? "Healthy"
      : health > 0.5
        ? "Watch"
        : "Critical";
  const healthColor = allPending
    ? "var(--info)"
    : health > 0.85
      ? "var(--ok)"
      : health > 0.5
        ? "var(--warn)"
        : "var(--danger)";

  return (
    <div className="overflow-hidden rounded-md border-2 border-line-soft">
      {/* Collapsed card — the scope screen is a CRT: always dark, both themes */}
      <div data-theme="dark">
        <button
          onClick={() => setExpanded(!expanded)}
          className="group block w-full text-left"
          aria-label="Toggle pacing history"
          aria-expanded={expanded}
        >
          <div className="relative h-36 sm:h-[150px]">
            {/* Oscilloscope background */}
            <div
              className="absolute inset-0"
              style={{
                background:
                  "radial-gradient(120% 100% at 50% 0%, var(--dark-700), var(--dark-900))",
              }}
            >
              <OscilloscopeSVG
                lines={pacing.lines}
                overallPct={pacing.overall_pacing_percentage}
              />
            </div>

            {/* Overlay: labels */}
            <div className="pointer-events-none absolute inset-x-[18px] top-3.5 flex items-start justify-between">
              <div>
                <div className="eyebrow">Pacing Oscilloscope</div>
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
              <div className="flex flex-col items-end gap-[5px]">
                {channels.map((ch, i) => (
                  <span
                    key={i}
                    className="inline-flex items-center gap-1.5 font-mono text-[10px] text-fg-muted"
                  >
                    <span className="max-w-[160px] truncate">{ch.label}</span>
                    <span
                      className="inline-block h-[7px] w-[7px] rounded-full"
                      style={{ backgroundColor: pacingToColor(ch.pct) }}
                    />
                  </span>
                ))}
              </div>
            </div>

            {/* Expand hint */}
            <div className="absolute bottom-3 right-[18px] inline-flex items-center gap-1.5 text-fg-faint transition-colors group-hover:text-fg-muted">
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
        </button>
      </div>

      {/* Expanded view — theme-aware (light on the live page) */}
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
                  {pacing.lines.map((line) => (
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
