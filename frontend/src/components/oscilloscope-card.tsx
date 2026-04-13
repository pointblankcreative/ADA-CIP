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
      className="w-full h-full"
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
        <line
          key={`h-${y}`}
          x1={0}
          y1={y}
          x2={width}
          y2={y}
          stroke="#334155"
          strokeWidth={0.5}
          opacity={0.25}
        />
      ))}
      {vLines.map((x) => (
        <line
          key={`v-${x}`}
          x1={x}
          y1={0}
          x2={x}
          y2={height}
          stroke="#334155"
          strokeWidth={0.5}
          opacity={0.25}
        />
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
  background: "#1e293b",
  border: "1px solid #334155",
  borderRadius: "0.5rem",
  fontSize: "0.75rem",
  color: "#e2e8f0",
};

function HistoryChart({ code }: { code: string }) {
  const [history, setHistory] = useState<PacingHistoryResponse | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    api.pacing
      .history(code)
      .then(setHistory)
      .catch(() => setHistory(null))
      .finally(() => setLoading(false));
  }, [code]);

  if (loading) {
    return (
      <div className="flex items-center justify-center h-48 text-slate-600 text-sm">
        Loading history...
      </div>
    );
  }

  if (!history || history.history.length === 0) {
    return (
      <div className="flex items-center justify-center h-48 text-slate-600 text-sm">
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
          <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" />
          <XAxis
            dataKey="date"
            tick={{ fontSize: 10, fill: "#64748b" }}
            interval="preserveStartEnd"
          />
          <YAxis
            domain={[50, 150]}
            tick={{ fontSize: 10, fill: "#64748b" }}
            tickFormatter={(v: number) => `${v}%`}
          />
          <Tooltip contentStyle={TOOLTIP_STYLE} formatter={(v: number) => `${v}%`} />

          {/* Threshold bands */}
          <ReferenceArea y1={85} y2={115} fill="#34d399" fillOpacity={0.04} />
          <ReferenceArea y1={70} y2={85} fill="#fbbf24" fillOpacity={0.04} />
          <ReferenceArea y1={115} y2={130} fill="#fbbf24" fillOpacity={0.04} />

          {/* 100% reference line */}
          <ReferenceLine
            y={100}
            stroke="#64748b"
            strokeDasharray="4 4"
            strokeWidth={1}
          />

          <Line
            type="monotone"
            dataKey="high"
            stroke="#f87171"
            strokeWidth={1.5}
            dot={false}
            name="Highest line"
          />
          <Line
            type="monotone"
            dataKey="overall"
            stroke="#34d399"
            strokeWidth={2}
            dot={false}
            name="Overall"
          />
          <Line
            type="monotone"
            dataKey="low"
            stroke="#60a5fa"
            strokeWidth={1.5}
            dot={false}
            name="Lowest line"
          />
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
}

// ── Main Oscilloscope Card ──────────────────────────────────────────

export function OscilloscopeCard({
  pacing,
  code,
}: {
  pacing: PacingResponse;
  code: string;
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
        ? "Attention"
        : "Critical";
  const healthColor = allPending
    ? "text-blue-400"
    : health > 0.85
      ? "text-emerald-400"
      : health > 0.5
        ? "text-amber-400"
        : "text-red-400";

  return (
    <div className="rounded-xl border border-slate-800 bg-slate-950 overflow-hidden">
      {/* Collapsed card */}
      <button
        onClick={() => setExpanded(!expanded)}
        className="w-full text-left group"
        aria-label="Toggle pacing history"
        aria-expanded={expanded}
      >
        <div className="relative h-32 sm:h-36">
          {/* Oscilloscope background */}
          <div className="absolute inset-0 bg-gradient-to-b from-slate-950 via-slate-950 to-slate-900">
            <OscilloscopeSVG
              lines={pacing.lines}
              overallPct={pacing.overall_pacing_percentage}
            />
          </div>

          {/* Overlay: subtle labels */}
          <div className="absolute inset-0 flex items-end justify-between px-4 pb-2 pointer-events-none">
            <div className="flex items-center gap-2">
              <span className={cn("text-xs font-medium", healthColor)}>
                {healthLabel}
              </span>
              <span className="text-[10px] text-slate-600">
                {allPending
                  ? "Lines pending activation"
                  : `${formatPercent(pacing.overall_pacing_percentage)} paced`}
              </span>
            </div>
            <div className="flex items-center gap-3 text-[10px] text-slate-600">
              {channels.map((ch, i) => (
                <span key={i} className="flex items-center gap-1">
                  <span
                    className="inline-block h-1.5 w-1.5 rounded-full"
                    style={{ backgroundColor: pacingToColor(ch.pct) }}
                  />
                  {ch.label}
                </span>
              ))}
            </div>
          </div>

          {/* Expand hint */}
          <div className="absolute top-2 right-3 text-slate-700 group-hover:text-slate-500 transition-colors">
            <svg
              xmlns="http://www.w3.org/2000/svg"
              viewBox="0 0 16 16"
              fill="currentColor"
              className={cn(
                "h-4 w-4 transition-transform",
                expanded && "rotate-180"
              )}
            >
              <path
                fillRule="evenodd"
                d="M4.22 6.22a.75.75 0 0 1 1.06 0L8 8.94l2.72-2.72a.75.75 0 1 1 1.06 1.06l-3.25 3.25a.75.75 0 0 1-1.06 0L4.22 7.28a.75.75 0 0 1 0-1.06Z"
              />
            </svg>
          </div>
        </div>
      </button>

      {/* Expanded view */}
      {expanded && (
        <div className="border-t border-slate-800 p-4 sm:p-5 space-y-5">
          {/* Historical trend chart */}
          <div>
            <h4 className="text-xs font-medium uppercase tracking-wider text-slate-500 mb-3">
              Pacing trend
            </h4>
            <HistoryChart code={code} />
          </div>

          {/* Per-line detail table */}
          <div>
            <h4 className="text-xs font-medium uppercase tracking-wider text-slate-500 mb-3">
              Line breakdown
            </h4>
            <div className="overflow-x-auto">
              <table className="w-full text-left text-xs">
                <thead>
                  <tr className="text-slate-600 uppercase tracking-wider">
                    <th className="pb-2 pr-4 font-medium">Line</th>
                    <th className="pb-2 pr-4 font-medium">Platform</th>
                    <th className="pb-2 pr-4 font-medium">Flight</th>
                    <th className="pb-2 pr-4 font-medium text-right">Pacing</th>
                    <th className="pb-2 font-medium text-right">Status</th>
                  </tr>
                </thead>
                <tbody>
                  {pacing.lines.map((line) => (
                    <tr
                      key={line.line_id}
                      className="border-t border-slate-800/50"
                    >
                      <td className="py-2 pr-4 text-slate-300 truncate max-w-[180px]">
                        {line.audience_name || line.channel_category || line.line_id.split("-").pop()}
                      </td>
                      <td className="py-2 pr-4 text-slate-500">
                        {platformLabel(line.platform_id)}
                      </td>
                      <td className="py-2 pr-4 text-slate-600 whitespace-nowrap">
                        {line.flight_start && line.flight_end
                          ? `${line.flight_start.slice(5)} — ${line.flight_end.slice(5)}`
                          : "—"}
                      </td>
                      <td className="py-2 pr-4 text-right tabular-nums text-slate-400">
                        {isLinePending(line)
                          ? "—"
                          : formatPercent(line.pacing_percentage)}
                      </td>
                      <td className="py-2 text-right">
                        <PacingBadge
                          percentage={line.pacing_percentage}
                          lineStatus={line.line_status}
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
