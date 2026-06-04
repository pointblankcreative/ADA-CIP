"use client";

import { useEffect, useState } from "react";
import {
  Activity,
  AlertTriangle,
  ChevronDown,
  ChevronRight,
  CircleCheck,
  CircleMinus,
  Play,
  TriangleAlert,
} from "lucide-react";
import {
  api,
  type DiagnosticOutput,
  type DiagnosticPillar,
  type DiagnosticSignal,
  type DiagnosticStatus,
  type PacingResponse,
  type PhaseSummary,
  type PacingLine,
} from "@/lib/api";
import { Card } from "@/components/card";
import { cn, formatCurrency, formatFlightDay, formatPercent, pacingColor, pacingStatus } from "@/lib/utils";

const PILLAR_LABELS: Record<string, string> = {
  distribution: "Distribution",
  attention: "Attention",
  resonance: "Resonance",
  acquisition: "Acquisition",
  funnel: "Funnel",
};

const PILLAR_ORDER_PERSUASION = ["distribution", "attention", "resonance"];
// Quality (Q1-Q3) is deferred pending per-client CRM integration —
// see docs/diagnostics/quality-pillar-deferred.md. Conversion campaigns
// now render only Acquisition + Funnel.
const PILLAR_ORDER_CONVERSION = ["acquisition", "funnel"];

function statusColor(status: DiagnosticStatus): string {
  if (status === "STRONG") return "text-emerald-400";
  if (status === "WATCH") return "text-amber-400";
  if (status === "ACTION") return "text-red-400";
  return "text-slate-500";
}

function statusBg(status: DiagnosticStatus): string {
  if (status === "STRONG") return "bg-emerald-500/10 border-emerald-500/30";
  if (status === "WATCH") return "bg-amber-500/10 border-amber-500/30";
  if (status === "ACTION") return "bg-red-500/10 border-red-500/30";
  return "bg-slate-800/40 border-slate-700";
}

function statusBarFill(status: DiagnosticStatus): string {
  if (status === "STRONG") return "bg-emerald-500";
  if (status === "WATCH") return "bg-amber-500";
  if (status === "ACTION") return "bg-red-500";
  return "bg-slate-600";
}

export interface RetrospectiveMetadata {
  cached: boolean;
  engineVersion: string;
}

export function DiagnosticsTab({
  code,
  asOfDate,
  onRetrospectiveMetadata,
}: {
  code: string;
  /**
   * When provided, fetch from the retrospective endpoint
   * (`/api/diagnostics/as-of/{asOfDate}/project/{code}`) instead of the
   * live latest-snapshot endpoint. Used by Retrospective Mode page
   * (ADAC-51 commit 7). Re-run affordance is suppressed in retro mode.
   */
  asOfDate?: string;
  /**
   * Optional callback invoked when retrospective metadata (cached flag +
   * engine version) is available. Lets the retro page surface those bits
   * in its banner without doing a separate fetch.
   */
  onRetrospectiveMetadata?: (meta: RetrospectiveMetadata) => void;
}) {
  const [outputs, setOutputs] = useState<DiagnosticOutput[]>([]);
  const [loading, setLoading] = useState(true);
  const [running, setRunning] = useState(false);
  const [error, setError] = useState<string | null>(null);
  // Multi-plan: pacing carries the phase mapping (sheet_id → phase_label,
  // line_ids → phase). Fetched alongside diagnostics so the per-phase
  // breakdown panel + signal-evidence chips render without extra round-trips.
  const [pacing, setPacing] = useState<PacingResponse | null>(null);
  // Active phase filter (sheet_id). null = "All phases" (aggregate view).
  const [activeSheetId, setActiveSheetId] = useState<string | null>(null);

  const load = () => {
    setLoading(true);
    // Pacing is fetched in parallel — failure here is non-fatal (the
    // phase-breakdown panel just won't render).
    api.pacing.get(code, asOfDate).then(setPacing).catch(() => setPacing(null));

    if (asOfDate) {
      api.retrospective
        .get(code, asOfDate)
        .then((resp) => {
          setOutputs(resp.diagnostics);
          setError(null);
          onRetrospectiveMetadata?.({
            cached: resp.cached,
            engineVersion: resp.engine_version,
          });
        })
        .catch((e) => setError(e instanceof Error ? e.message : String(e)))
        .finally(() => setLoading(false));
    } else {
      api.diagnostics
        .get(code)
        .then((data) => {
          setOutputs(data);
          setError(null);
        })
        .catch((e) => setError(e instanceof Error ? e.message : String(e)))
        .finally(() => setLoading(false));
    }
  };

  useEffect(() => {
    load();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [code, asOfDate]);

  const handleRun = async () => {
    setRunning(true);
    setError(null);
    try {
      const result = await api.diagnostics.run(code);
      if (result.status === "skipped") {
        setError(result.message ?? "Diagnostic run was skipped.");
      }
      load();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setRunning(false);
    }
  };

  if (loading) {
    return (
      <div className="space-y-4">
        <Card className="animate-pulse">
          <div className="h-8 w-64 rounded bg-slate-700" />
          <div className="mt-4 h-2 w-full rounded bg-slate-700" />
          <div className="mt-6 grid grid-cols-3 gap-4">
            {[0, 1, 2].map((i) => (
              <div key={i} className="h-20 rounded bg-slate-700/60" />
            ))}
          </div>
        </Card>
      </div>
    );
  }

  if (outputs.length === 0) {
    return (
      <Card className="flex flex-col items-center gap-4 py-12">
        <Activity className="h-10 w-10 text-slate-500" />
        <div className="text-center">
          {asOfDate ? (
            <>
              <p className="text-slate-300">
                No diagnostic data available for this project on {asOfDate}.
              </p>
              <p className="mt-1 text-xs text-slate-500">
                The campaign may not have been active on that date, or the
                project had no media plan yet.
              </p>
            </>
          ) : (
            <>
              <p className="text-slate-300">No diagnostic results yet for this project.</p>
              <p className="mt-1 text-xs text-slate-500">
                Diagnostics run automatically as part of the daily pipeline, or you
                can trigger a run now.
              </p>
            </>
          )}
        </div>
        {error && (
          <p className="text-xs text-red-400">{error}</p>
        )}
        {/* Re-run affordance suppressed in retro mode: there's nothing
            actionable since the engine already auto-computes on miss via
            the retrospective endpoint. */}
        {!asOfDate && (
          <button
            onClick={handleRun}
            disabled={running}
            className="flex items-center gap-2 rounded-md bg-brand-600/20 px-4 py-2 text-sm font-medium text-brand-300 hover:bg-brand-600/30 disabled:opacity-50"
          >
            <Play className="h-3.5 w-3.5" />
            {running ? "Running…" : "Run diagnostic now"}
          </button>
        )}
      </Card>
    );
  }

  return (
    <div className="space-y-6">
      {/* Re-run is meaningless in retro mode (the date is fixed; the
          retrospective endpoint already auto-computes on cache miss). */}
      {!asOfDate && (
        <div className="flex items-center justify-end">
          <button
            onClick={handleRun}
            disabled={running}
            className="flex items-center gap-2 rounded-md border border-slate-700 bg-slate-800/50 px-3 py-1.5 text-xs text-slate-300 hover:bg-slate-800 disabled:opacity-50"
          >
            <Play className="h-3 w-3" />
            {running ? "Running…" : "Re-run"}
          </button>
        </div>
      )}

      {error && (
        <div className="rounded-md border border-amber-500/30 bg-amber-500/10 px-3 py-2 text-xs text-amber-300">
          {error}
        </div>
      )}

      {/* Multi-plan: aggregate diagnostic stays unchanged. The breakdown panel
          shows how lines/budget split across phases, and clicking a phase
          chip filters the signal-evidence highlight below. The aggregate
          health score itself isn't recomputed per-phase — that would need
          schema-level work (one fact_diagnostic_signals row per phase). */}
      {pacing && pacing.phases && pacing.phases.length > 1 && (
        <PhaseBreakdownPanel
          phases={pacing.phases}
          lines={pacing.lines}
          activeSheetId={activeSheetId}
          onSelect={setActiveSheetId}
        />
      )}

      {outputs.map((out) => (
        <DiagnosticCard
          key={out.id}
          output={out}
          activePhaseLineIds={
            activeSheetId && pacing
              ? new Set(
                  pacing.lines
                    .filter((l) => l.sheet_id === activeSheetId)
                    .map((l) => l.line_id),
                )
              : null
          }
        />
      ))}
    </div>
  );
}

function PhaseBreakdownPanel({
  phases,
  lines,
  activeSheetId,
  onSelect,
}: {
  phases: PhaseSummary[];
  lines: PacingLine[];
  activeSheetId: string | null;
  onSelect: (sheetId: string | null) => void;
}) {
  // Map sheet_id → set of line_ids so the chip can show the count even when
  // pacing's per-phase aggregate dropped pending lines.
  const linesBySheet = new Map<string, number>();
  for (const l of lines) {
    if (!l.sheet_id) continue;
    linesBySheet.set(l.sheet_id, (linesBySheet.get(l.sheet_id) ?? 0) + 1);
  }

  return (
    <Card className="space-y-3">
      <div className="flex items-baseline justify-between">
        <div>
          <h3 className="text-sm font-semibold text-white">Phase breakdown</h3>
          <p className="text-[11px] text-slate-500">
            Aggregate health is shown below. Click a phase to highlight its lines in the signal evidence.
          </p>
        </div>
        {activeSheetId && (
          <button
            onClick={() => onSelect(null)}
            className="text-[11px] text-slate-400 hover:text-white"
          >
            Clear filter
          </button>
        )}
      </div>
      <div className="grid grid-cols-1 gap-2 md:grid-cols-2 lg:grid-cols-3">
        {phases.map((phase, idx) => {
          const isActive = activeSheetId === phase.sheet_id;
          const status = pacingStatus(phase.pacing_percentage);
          const heading =
            phase.phase_label ?? `Phase ${phase.display_order ?? idx + 1}`;
          return (
            <button
              key={phase.sheet_id}
              onClick={() => onSelect(isActive ? null : phase.sheet_id)}
              className={cn(
                "rounded-md border px-3 py-2 text-left transition-colors",
                isActive
                  ? "border-brand-500/60 bg-brand-500/10"
                  : "border-slate-700/60 bg-slate-900/40 hover:border-slate-600",
              )}
            >
              <div className="flex items-center justify-between">
                <div className="flex items-center gap-2">
                  <div className="text-sm font-semibold text-white">{heading}</div>
                  {!phase.is_active && (
                    <span className="rounded bg-slate-700/50 px-1.5 py-0.5 text-[10px] font-medium text-slate-400">
                      retired
                    </span>
                  )}
                </div>
                <div className={cn("text-xs font-semibold tabular-nums", pacingColor(status))}>
                  {formatPercent(phase.pacing_percentage)}
                </div>
              </div>
              <div className="mt-1 text-[11px] text-slate-500">
                {linesBySheet.get(phase.sheet_id) ?? phase.line_count} lines · {formatCurrency(phase.planned_budget)} planned
              </div>
              <div className="text-[11px] text-slate-400">
                {formatCurrency(phase.actual_spend_to_date)} spent of {formatCurrency(phase.planned_spend_to_date)} planned to date
              </div>
            </button>
          );
        })}
      </div>
    </Card>
  );
}

function DiagnosticCard({
  output,
  activePhaseLineIds,
}: {
  output: DiagnosticOutput;
  /**
   * When non-null, identifies the lines belonging to the user-selected phase
   * (multi-plan drill-down). The card surfaces an overlap chip showing how
   * many of the diagnostic's lines fall in the chosen phase, and dims the
   * card when the overlap is zero (the diagnostic doesn't apply to the
   * selected phase). The aggregate score is intentionally NOT recomputed —
   * per-phase scoring would need its own fact_diagnostic_signals row.
   */
  activePhaseLineIds: Set<string> | null;
}) {
  const pillarOrder =
    output.campaign_type === "persuasion"
      ? PILLAR_ORDER_PERSUASION
      : PILLAR_ORDER_CONVERSION;

  const flightPct = output.flight_total_days > 0
    ? Math.min((output.flight_day / output.flight_total_days) * 100, 100)
    : 0;

  const overlapCount = activePhaseLineIds
    ? output.line_ids.filter((id) => activePhaseLineIds.has(id)).length
    : null;
  const dimmed = overlapCount === 0;

  return (
    <Card className={cn(dimmed && "opacity-50")}>
      {/* Header: health score + flight progress */}
      <div className="flex flex-col gap-4 sm:flex-row sm:items-start sm:justify-between">
        <div>
          <div className="text-[11px] uppercase tracking-wide text-slate-500">
            Campaign Health · {output.campaign_type}
          </div>
          <div className="mt-1 flex items-baseline gap-3">
            <div
              className={cn(
                "text-4xl font-semibold tabular-nums",
                statusColor(output.health_status)
              )}
            >
              {output.health_score != null ? output.health_score.toFixed(0) : "—"}
            </div>
            {output.health_status && (
              <div className={cn("text-sm font-medium", statusColor(output.health_status))}>
                {output.health_status}
              </div>
            )}
            {/* AI-040: coverage-gated health. health_coverage is only
                populated on post-fix snapshots — legacy rows render the
                plain em-dash exactly as before. */}
            {output.health_score == null && output.health_coverage != null && (
              <div className="text-sm font-medium text-slate-400">
                INSUFFICIENT DATA
              </div>
            )}
          </div>
          {output.health_coverage != null && (
            <div className="mt-1 text-[11px] text-slate-500">
              {signalCoverageLabel(output)}
            </div>
          )}
          <div className="mt-1 text-xs text-slate-500">
            {formatFlightDay(
              {
                flightDay: output.flight_day,
                flightTotalDays: output.flight_total_days,
                daysRemaining:
                  output.flight_total_days != null && output.flight_day != null
                    ? output.flight_total_days - output.flight_day
                    : null,
              },
              "combined",
            )}
            {" · evaluated "}
            {output.evaluation_date}
          </div>
          {overlapCount !== null && (
            <div className="mt-2 inline-flex items-center gap-1.5 rounded-md border border-slate-700 bg-slate-900/60 px-2 py-1 text-[11px] text-slate-300">
              <span className="text-slate-500">Selected phase:</span>
              <span className="tabular-nums font-medium text-white">
                {overlapCount} / {output.line_ids.length}
              </span>
              <span className="text-slate-500">lines in this diagnostic</span>
            </div>
          )}
        </div>
        <EfficiencyStrip efficiency={output.efficiency} />
      </div>

      {/* Flight progress bar */}
      <div className="mt-4 h-1.5 w-full rounded-full bg-slate-800 overflow-hidden">
        <div
          className="h-full bg-brand-500/70"
          style={{ width: `${flightPct}%` }}
        />
      </div>

      {/* Pillars — column count matches pillar count (2 for conversion with
          Quality deferred, 3 for persuasion). */}
      <div
        className={cn(
          "mt-6 grid grid-cols-1 gap-3",
          pillarOrder.length === 2 ? "sm:grid-cols-2" : "sm:grid-cols-3"
        )}
      >
        {pillarOrder.map((p) => (
          <PillarGauge
            key={p}
            label={PILLAR_LABELS[p] ?? p}
            pillar={output.pillars[p]}
          />
        ))}
      </div>

      {/* Alerts */}
      {output.alerts.length > 0 && (
        <div className="mt-6 space-y-2">
          <div className="text-[11px] uppercase tracking-wide text-slate-500">
            Critical Alerts
          </div>
          {output.alerts.map((a, i) => (
            <div
              key={`${a.type}-${i}`}
              className={cn(
                "flex items-start gap-2 rounded-md border px-3 py-2 text-xs",
                a.severity === "critical"
                  ? "border-red-500/30 bg-red-500/10 text-red-300"
                  : "border-amber-500/30 bg-amber-500/10 text-amber-300"
              )}
            >
              <TriangleAlert className="mt-0.5 h-3.5 w-3.5 flex-shrink-0" />
              <div>
                <div className="font-medium uppercase tracking-wide">
                  {a.type.replace(/_/g, " ")}
                </div>
                <div className="mt-0.5 text-slate-300">{a.message}</div>
              </div>
            </div>
          ))}
        </div>
      )}

      {/* Signal list */}
      <div className="mt-6">
        <div className="text-[11px] uppercase tracking-wide text-slate-500">
          Signal Detail
        </div>
        <div className="mt-2 divide-y divide-slate-800 rounded-md border border-slate-800">
          {output.signals.map((s) => (
            <SignalRow key={s.id} signal={s} />
          ))}
          {output.signals.length === 0 && (
            <div className="px-3 py-2 text-xs text-slate-500">
              No signals were evaluated.
            </div>
          )}
        </div>
      </div>
    </Card>
  );
}

function PillarGauge({
  label,
  pillar,
}: {
  label: string;
  pillar: DiagnosticPillar | undefined;
}) {
  const score = pillar?.score ?? null;
  const status = pillar?.status ?? null;
  // AI-040: coverage metadata is only present on post-fix snapshots.
  // Legacy rows (hasCoverage === false) render exactly as before.
  const hasCoverage = pillar?.coverage != null;
  const coverageLabel =
    hasCoverage && pillar?.signals_total
      ? `${pillar.signals_active ?? 0} of ${pillar.signals_total} signals reporting`
      : null;

  return (
    <div className={cn("rounded-md border p-3", statusBg(status))}>
      <div className="flex items-center justify-between">
        <div className="text-xs font-medium text-slate-300">{label}</div>
        {status && (
          <div className={cn("text-[10px] font-semibold", statusColor(status))}>
            {status}
          </div>
        )}
        {!status && hasCoverage && (
          <div className="text-[10px] font-semibold text-slate-500">
            INSUFFICIENT DATA
          </div>
        )}
      </div>
      {score != null ? (
        <>
          <div className={cn("mt-2 text-2xl font-semibold tabular-nums", statusColor(status))}>
            {score.toFixed(0)}
          </div>
          {coverageLabel && (
            <div className="mt-1 text-[10px] leading-tight text-slate-500">
              {coverageLabel}
            </div>
          )}
          <div className="mt-2 h-1.5 w-full rounded-full bg-slate-800 overflow-hidden">
            <div
              className={cn("h-full", statusBarFill(status))}
              style={{ width: `${Math.min(score, 100)}%` }}
            />
          </div>
        </>
      ) : (
        <>
          <div className="mt-2 text-2xl font-semibold tabular-nums text-slate-500">
            —
          </div>
          <div className="mt-1 text-[10px] leading-tight text-slate-500">
            {coverageLabel
              ? `${coverageLabel} — below the coverage floor for a reliable score`
              : "Awaiting data — no signals have cleared their data guard yet"}
          </div>
          <div className="mt-2 h-1.5 w-full rounded-full bg-slate-800 overflow-hidden" />
        </>
      )}
    </div>
  );
}

function signalCoverageLabel(output: DiagnosticOutput): string {
  const pillars = Object.values(output.pillars);
  const active = pillars.reduce((n, p) => n + (p.signals_active ?? 0), 0);
  const total = pillars.reduce((n, p) => n + (p.signals_total ?? 0), 0);
  const pct =
    output.health_coverage != null
      ? ` · ${(output.health_coverage * 100).toFixed(0)}% of signal weight`
      : "";
  return `${active} of ${total} signals reporting${pct}`;
}

function SignalRow({ signal }: { signal: DiagnosticSignal }) {
  const [expanded, setExpanded] = useState(false);
  const Icon = !signal.guard_passed
    ? CircleMinus
    : signal.status === "STRONG"
      ? CircleCheck
      : signal.status === "ACTION"
        ? AlertTriangle
        : TriangleAlert;

  return (
    <div className="text-xs">
      <button
        type="button"
        onClick={() => setExpanded((v) => !v)}
        className="flex w-full items-center gap-3 px-3 py-2 text-left hover:bg-slate-800/40"
      >
        {expanded ? (
          <ChevronDown className="h-3 w-3 flex-shrink-0 text-slate-500" />
        ) : (
          <ChevronRight className="h-3 w-3 flex-shrink-0 text-slate-500" />
        )}
        <div className="w-8 font-mono text-slate-500">{signal.id}</div>
        <div className="flex-1 text-slate-300">{signal.name}</div>
        <Icon className={cn("h-3.5 w-3.5", statusColor(signal.status))} />
        <div className={cn("w-10 text-right tabular-nums font-medium", statusColor(signal.status))}>
          {signal.score != null ? signal.score.toFixed(0) : "—"}
        </div>
        <div className={cn("w-16 text-right text-[10px] font-semibold", statusColor(signal.status))}>
          {!signal.guard_passed ? "NO DATA" : (signal.status ?? "—")}
        </div>
      </button>
      {expanded && (
        <div className="border-t border-slate-800 bg-slate-900/40 px-3 py-3 space-y-2">
          {signal.diagnostic && (
            <p className="text-slate-300">{signal.diagnostic}</p>
          )}
          {!signal.guard_passed && signal.guard_reason && (
            <p className="text-slate-500">
              Guard failed: <span className="font-mono">{signal.guard_reason}</span>
            </p>
          )}
          <div className="grid grid-cols-2 gap-x-4 gap-y-1 text-[11px] text-slate-400 sm:grid-cols-3">
            {signal.raw_value != null && (
              <KV k="Value" v={signal.raw_value.toFixed(3)} />
            )}
            {signal.benchmark != null && (
              <KV k="Benchmark" v={signal.benchmark.toFixed(3)} />
            )}
            {signal.floor != null && (
              <KV k="Floor" v={signal.floor.toFixed(3)} />
            )}
            {Object.entries(signal.inputs ?? {}).map(([k, v]) => (
              <KV key={k} k={k} v={formatInput(v)} />
            ))}
          </div>
        </div>
      )}
    </div>
  );
}

function KV({ k, v }: { k: string; v: string }) {
  return (
    <div className="flex items-baseline gap-1">
      <span className="text-slate-500">{k}:</span>
      <span className="font-mono text-slate-300">{v}</span>
    </div>
  );
}

function formatInput(v: unknown): string {
  if (v == null) return "—";
  if (typeof v === "number")
    return Number.isInteger(v) ? v.toLocaleString() : v.toFixed(3);
  if (typeof v === "object") return JSON.stringify(v);
  return String(v);
}

function EfficiencyStrip({
  efficiency,
}: {
  efficiency: {
    cpm: number | null;
    cpc: number | null;
    cpa: number | null;
    cpcv: number | null;
    pacing_pct: number | null;
  };
}) {
  const items: Array<[string, string]> = [];
  if (efficiency.cpm != null) items.push(["CPM", `$${efficiency.cpm.toFixed(2)}`]);
  if (efficiency.cpc != null) items.push(["CPC", `$${efficiency.cpc.toFixed(2)}`]);
  if (efficiency.cpa != null) items.push(["CPA", `$${efficiency.cpa.toFixed(2)}`]);
  if (efficiency.cpcv != null) items.push(["CPCV", `$${efficiency.cpcv.toFixed(2)}`]);
  if (efficiency.pacing_pct != null)
    items.push(["Pacing", `${efficiency.pacing_pct.toFixed(0)}%`]);

  if (items.length === 0) return null;

  return (
    <div className="flex flex-wrap gap-x-4 gap-y-1 text-right text-[11px]">
      {items.map(([k, v]) => (
        <div key={k}>
          <div className="text-slate-500">{k}</div>
          <div className="font-mono text-slate-200">{v}</div>
        </div>
      ))}
    </div>
  );
}
