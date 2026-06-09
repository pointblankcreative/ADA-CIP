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
import { Btn, Eyebrow, Label, StatusPill } from "@/components/ui";
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
  if (status === "STRONG") return "text-ok";
  if (status === "WATCH") return "text-warn";
  if (status === "ACTION") return "text-danger";
  return "text-fg-muted";
}

/** Raw CSS token for inline styles (Folsom scores, tinted pillar cards). */
function statusVar(status: DiagnosticStatus): string {
  if (status === "STRONG") return "var(--ok)";
  if (status === "WATCH") return "var(--warn)";
  if (status === "ACTION") return "var(--danger)";
  return "var(--text-faint)";
}

function statusBarFill(status: DiagnosticStatus): string {
  if (status === "STRONG") return "bg-ok";
  if (status === "WATCH") return "bg-warn";
  if (status === "ACTION") return "bg-danger";
  return "bg-done";
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
          <div className="h-8 w-64 rounded bg-surface-sunken" />
          <div className="mt-4 h-2 w-full rounded bg-surface-sunken" />
          <div className="mt-6 grid grid-cols-3 gap-4">
            {[0, 1, 2].map((i) => (
              <div key={i} className="h-20 rounded bg-surface-sunken" />
            ))}
          </div>
        </Card>
      </div>
    );
  }

  if (outputs.length === 0) {
    return (
      <Card className="flex flex-col items-center gap-4 py-12">
        <Activity className="h-10 w-10 text-fg-faint" />
        <div className="text-center">
          {asOfDate ? (
            <>
              <p className="text-fg">
                No diagnostic data available for this project on {asOfDate}.
              </p>
              <p className="mt-1 text-xs text-fg-muted">
                The campaign may not have been active on that date, or the
                project had no media plan yet.
              </p>
            </>
          ) : (
            <>
              <p className="text-fg">No diagnostic results yet for this project.</p>
              <p className="mt-1 text-xs text-fg-muted">
                Diagnostics run automatically as part of the daily pipeline, or you
                can trigger a run now.
              </p>
            </>
          )}
        </div>
        {error && (
          <p className="text-xs text-danger">{error}</p>
        )}
        {/* Re-run affordance suppressed in retro mode: there's nothing
            actionable since the engine already auto-computes on miss via
            the retrospective endpoint. */}
        {!asOfDate && (
          <Btn
            variant="primary"
            size="sm"
            onClick={handleRun}
            disabled={running}
            icon={<Play className="h-3.5 w-3.5" />}
          >
            {running ? "Running…" : "Run diagnostic now"}
          </Btn>
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
          <Btn
            variant="outline"
            size="sm"
            onClick={handleRun}
            disabled={running}
            icon={<Play className="h-3 w-3" />}
          >
            {running ? "Running…" : "Re-run diagnostic"}
          </Btn>
        </div>
      )}

      {error && (
        <div className="rounded-md border-2 border-tint-warn bg-tint-warn px-3 py-2 text-xs text-warn">
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
          <Label className="text-fg-secondary">Phase breakdown</Label>
          <p className="mt-1 text-[11px] text-fg-muted">
            Aggregate health is shown below. Click a phase to highlight its lines in the signal evidence.
          </p>
        </div>
        {activeSheetId && (
          <button
            onClick={() => onSelect(null)}
            className="font-mono text-[11px] uppercase tracking-[0.06em] text-fg-muted hover:text-fg"
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
                "rounded-md border-2 px-3 py-2 text-left transition-colors duration-fast",
                isActive
                  ? "border-accent bg-tint-accent"
                  : "border-line-soft bg-surface-sunken hover:border-line",
              )}
            >
              <div className="flex items-center justify-between">
                <div className="flex items-center gap-2">
                  <div className="text-sm font-bold text-fg">{heading}</div>
                  {!phase.is_active && (
                    <span className="rounded-xs bg-surface-card px-1.5 py-0.5 font-mono text-[10px] font-medium uppercase tracking-[0.06em] text-fg-muted">
                      retired
                    </span>
                  )}
                </div>
                <div className={cn("tnum font-mono text-xs font-bold", pacingColor(status))}>
                  {formatPercent(phase.pacing_percentage)}
                </div>
              </div>
              <div className="mt-1 font-mono text-[11px] text-fg-muted">
                {linesBySheet.get(phase.sheet_id) ?? phase.line_count} lines · {formatCurrency(phase.planned_budget)} planned
              </div>
              <div className="text-[11px] text-fg-secondary">
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
    <Card className={cn("overflow-hidden p-0", dimmed && "opacity-50")}>
      {/* Header band: health score + efficiency strip */}
      <div className="flex flex-wrap items-start justify-between gap-6 border-b-2 border-line-soft bg-surface-sunken p-[22px]">
        <div>
          <Eyebrow>Campaign Health · {output.campaign_type}</Eyebrow>
          <div className="mt-3 flex items-end gap-4">
            <span
              className="tnum font-display text-[64px] leading-[0.82] tracking-[0.005em] sm:text-[76px]"
              style={{ color: statusVar(output.health_status) }}
            >
              {output.health_score != null ? output.health_score.toFixed(0) : "—"}
            </span>
            <div className="pb-1.5">
              {output.health_status ? (
                <StatusPill
                  label={output.health_status}
                  color={statusVar(output.health_status)}
                />
              ) : output.health_coverage != null ? (
                <StatusPill
                  label="Insufficient data"
                  color="var(--text-faint)"
                  dot={false}
                />
              ) : null}
              {output.health_coverage != null && (
                <div className="mt-2 font-mono text-[10.5px] text-fg-faint">
                  {signalCoverageLabel(output)}
                </div>
              )}
            </div>
          </div>
          <div className="mt-3.5 font-mono text-[11px] text-fg-muted">
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
          {/* Flight progress */}
          <div className="mt-2.5 w-[260px] max-w-full">
            <div className="h-[5px] overflow-hidden rounded-pill bg-surface-card">
              <div
                className="h-full rounded-pill bg-accent opacity-80"
                style={{ width: `${flightPct}%` }}
              />
            </div>
          </div>
          {overlapCount !== null && (
            <div className="mt-3 inline-flex items-center gap-1.5 rounded-sm border border-line bg-surface-card px-2 py-1 text-[11px] text-fg-secondary">
              <span className="text-fg-muted">Selected phase:</span>
              <span className="tnum font-mono font-semibold text-fg">
                {overlapCount} / {output.line_ids.length}
              </span>
              <span className="text-fg-muted">lines in this diagnostic</span>
            </div>
          )}
        </div>
        <EfficiencyStrip efficiency={output.efficiency} />
      </div>

      <div className="p-[22px]">
        {/* Pillars — column count matches pillar count (2 for conversion with
            Quality deferred, 3 for persuasion). */}
        <div
          className={cn(
            "grid grid-cols-1 gap-3",
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
            <Label>Critical Alerts</Label>
            {output.alerts.map((a, i) => {
              const c = a.severity === "critical" ? "var(--danger)" : "var(--warn)";
              return (
                <div
                  key={`${a.type}-${i}`}
                  className="flex items-start gap-2.5 rounded-sm px-3 py-2.5 text-xs"
                  style={{
                    border: `1.5px solid color-mix(in srgb, ${c} 35%, transparent)`,
                    background: `color-mix(in srgb, ${c} 9%, transparent)`,
                  }}
                >
                  <TriangleAlert
                    className="mt-0.5 h-3.5 w-3.5 flex-shrink-0"
                    style={{ color: c }}
                  />
                  <div>
                    <div
                      className="font-mono text-[10.5px] font-semibold uppercase tracking-[0.1em]"
                      style={{ color: c }}
                    >
                      {a.type.replace(/_/g, " ")}
                    </div>
                    <div className="mt-1 text-[13px] text-fg-secondary">{a.message}</div>
                  </div>
                </div>
              );
            })}
          </div>
        )}

        {/* Signal list */}
        <div className="mt-6">
          <Label>Signal Detail</Label>
          <div className="mt-2.5 divide-y divide-line-soft overflow-hidden rounded-sm border-[1.5px] border-line-soft">
            {output.signals.map((s) => (
              <SignalRow key={s.id} signal={s} />
            ))}
            {output.signals.length === 0 && (
              <div className="px-3 py-2 text-xs text-fg-muted">
                No signals were evaluated.
              </div>
            )}
          </div>
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
  const c = statusVar(status);
  // AI-040: coverage metadata is only present on post-fix snapshots.
  // Legacy rows (hasCoverage === false) render exactly as before.
  const hasCoverage = pillar?.coverage != null;
  const coverageLabel =
    hasCoverage && pillar?.signals_total
      ? `${pillar.signals_active ?? 0} of ${pillar.signals_total} signals reporting`
      : null;

  return (
    <div
      className="rounded-sm p-3.5"
      style={{
        border: `1.5px solid color-mix(in srgb, ${c} 28%, transparent)`,
        background: `color-mix(in srgb, ${c} 7%, transparent)`,
      }}
    >
      <div className="flex items-center justify-between">
        <div className="text-[13px] font-semibold text-fg-secondary">{label}</div>
        {status ? (
          <div
            className="font-mono text-[10px] font-semibold uppercase tracking-[0.06em]"
            style={{ color: c }}
          >
            {status}
          </div>
        ) : hasCoverage ? (
          <div className="font-mono text-[10px] font-semibold uppercase tracking-[0.06em] text-fg-muted">
            Insufficient data
          </div>
        ) : null}
      </div>
      {score != null ? (
        <>
          <div
            className="tnum mt-2.5 font-display text-[30px] leading-none"
            style={{ color: c }}
          >
            {score.toFixed(0)}
          </div>
          {coverageLabel && (
            <div className="mt-2 font-mono text-[9.5px] leading-tight text-fg-faint">
              {coverageLabel}
            </div>
          )}
          <div className="mt-2.5 h-1.5 w-full overflow-hidden rounded-pill bg-surface-sunken">
            <div
              className={cn("h-full rounded-pill transition-[width] duration-700 ease-snap", statusBarFill(status))}
              style={{ width: `${Math.min(score, 100)}%` }}
            />
          </div>
        </>
      ) : (
        <>
          <div className="tnum mt-2.5 font-display text-[30px] leading-none text-fg-faint">
            —
          </div>
          <div className="mt-2 font-mono text-[9.5px] leading-tight text-fg-faint">
            {coverageLabel
              ? `${coverageLabel} — below the coverage floor for a reliable score`
              : "Awaiting data — no signals have cleared their data guard yet"}
          </div>
          <div className="mt-2.5 h-1.5 w-full overflow-hidden rounded-pill bg-surface-sunken" />
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
  const noData = !signal.guard_passed;
  const Icon = noData
    ? CircleMinus
    : signal.status === "STRONG"
      ? CircleCheck
      : signal.status === "ACTION"
        ? AlertTriangle
        : TriangleAlert;
  const cls = noData ? "text-fg-faint" : statusColor(signal.status);

  return (
    <div className={cn("text-xs", expanded && "bg-surface-sunken")}>
      <button
        type="button"
        onClick={() => setExpanded((v) => !v)}
        className="flex w-full items-center gap-3 px-3.5 py-2.5 text-left transition-colors hover:bg-surface-sunken"
      >
        {expanded ? (
          <ChevronDown className="h-3 w-3 flex-shrink-0 text-fg-faint" />
        ) : (
          <ChevronRight className="h-3 w-3 flex-shrink-0 text-fg-faint" />
        )}
        <div className={cn("w-8 font-mono text-[11.5px] font-semibold", cls)}>
          {signal.id}
        </div>
        <div className="flex-1 text-[13px] text-fg-secondary">{signal.name}</div>
        <Icon className={cn("h-3.5 w-3.5", cls)} />
        <div className={cn("tnum w-10 text-right font-mono font-semibold", cls)}>
          {signal.score != null ? signal.score.toFixed(0) : "—"}
        </div>
        <div className={cn("w-16 text-right font-mono text-[10px] font-semibold tracking-[0.05em]", cls)}>
          {noData ? "NO DATA" : (signal.status ?? "—")}
        </div>
      </button>
      {expanded && (
        <div className="space-y-2 border-t border-line-soft px-3.5 py-3 pl-[52px]">
          {signal.diagnostic && (
            <p className="text-[12.5px] leading-relaxed text-fg-secondary">{signal.diagnostic}</p>
          )}
          {noData && signal.guard_reason && (
            <p className="text-fg-muted">
              Guard failed: <span className="font-mono">{signal.guard_reason}</span>
            </p>
          )}
          <div className="grid grid-cols-2 gap-x-4 gap-y-1 text-[11px] text-fg-secondary sm:grid-cols-3">
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
      <span className="text-fg-muted">{k}:</span>
      <span className="font-mono text-fg-secondary">{v}</span>
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
    <div className="flex flex-wrap justify-end gap-x-[22px] gap-y-3 text-right">
      {items.map(([k, v]) => (
        <div key={k}>
          <div className="label text-[9.5px]">{k}</div>
          <div className="tnum mt-1 font-mono text-base font-semibold text-fg">{v}</div>
        </div>
      ))}
    </div>
  );
}
