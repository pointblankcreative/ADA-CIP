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
  type DiagnosticSignal,
  type DiagnosticStatus,
} from "@/lib/api";
import { Card } from "@/components/card";
import { cn } from "@/lib/utils";

const PILLAR_LABELS: Record<string, string> = {
  distribution: "Distribution",
  attention: "Attention",
  resonance: "Resonance",
  acquisition: "Acquisition",
  funnel: "Funnel",
  quality: "Quality",
};

const PILLAR_ORDER_PERSUASION = ["distribution", "attention", "resonance"];
const PILLAR_ORDER_CONVERSION = ["acquisition", "funnel", "quality"];

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

export function DiagnosticsTab({ code }: { code: string }) {
  const [outputs, setOutputs] = useState<DiagnosticOutput[]>([]);
  const [loading, setLoading] = useState(true);
  const [running, setRunning] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const load = () => {
    setLoading(true);
    api.diagnostics
      .get(code)
      .then((data) => {
        setOutputs(data);
        setError(null);
      })
      .catch((e) => setError(e instanceof Error ? e.message : String(e)))
      .finally(() => setLoading(false));
  };

  useEffect(() => {
    load();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [code]);

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
          <p className="text-slate-300">No diagnostic results yet for this project.</p>
          <p className="mt-1 text-xs text-slate-500">
            Diagnostics run automatically as part of the daily pipeline, or you
            can trigger a run now.
          </p>
        </div>
        {error && (
          <p className="text-xs text-red-400">{error}</p>
        )}
        <button
          onClick={handleRun}
          disabled={running}
          className="flex items-center gap-2 rounded-md bg-brand-600/20 px-4 py-2 text-sm font-medium text-brand-300 hover:bg-brand-600/30 disabled:opacity-50"
        >
          <Play className="h-3.5 w-3.5" />
          {running ? "Running…" : "Run diagnostic now"}
        </button>
      </Card>
    );
  }

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <p className="text-xs text-slate-500">
          Phase 1 — persuasion Distribution pillar active. Attention, Resonance
          and Conversion campaigns arrive in later phases.
        </p>
        <button
          onClick={handleRun}
          disabled={running}
          className="flex items-center gap-2 rounded-md border border-slate-700 bg-slate-800/50 px-3 py-1.5 text-xs text-slate-300 hover:bg-slate-800 disabled:opacity-50"
        >
          <Play className="h-3 w-3" />
          {running ? "Running…" : "Re-run"}
        </button>
      </div>

      {error && (
        <div className="rounded-md border border-amber-500/30 bg-amber-500/10 px-3 py-2 text-xs text-amber-300">
          {error}
        </div>
      )}

      {outputs.map((out) => (
        <DiagnosticCard key={out.id} output={out} />
      ))}
    </div>
  );
}

function DiagnosticCard({ output }: { output: DiagnosticOutput }) {
  const pillarOrder =
    output.campaign_type === "persuasion"
      ? PILLAR_ORDER_PERSUASION
      : PILLAR_ORDER_CONVERSION;

  const flightPct = output.flight_total_days > 0
    ? Math.min((output.flight_day / output.flight_total_days) * 100, 100)
    : 0;

  return (
    <Card>
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
          </div>
          <div className="mt-1 text-xs text-slate-500">
            Day {output.flight_day} of {output.flight_total_days} · evaluated {output.evaluation_date}
          </div>
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

      {/* Pillars */}
      <div className="mt-6 grid grid-cols-1 gap-3 sm:grid-cols-3">
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
  pillar: { score: number | null; status: DiagnosticStatus } | undefined;
}) {
  const score = pillar?.score ?? null;
  const status = pillar?.status ?? null;

  return (
    <div className={cn("rounded-md border p-3", statusBg(status))}>
      <div className="flex items-center justify-between">
        <div className="text-xs font-medium text-slate-300">{label}</div>
        {status && (
          <div className={cn("text-[10px] font-semibold", statusColor(status))}>
            {status}
          </div>
        )}
      </div>
      <div className={cn("mt-2 text-2xl font-semibold tabular-nums", statusColor(status))}>
        {score != null ? score.toFixed(0) : "—"}
      </div>
      <div className="mt-2 h-1.5 w-full rounded-full bg-slate-800 overflow-hidden">
        <div
          className={cn("h-full", statusBarFill(status))}
          style={{ width: `${Math.min(score ?? 0, 100)}%` }}
        />
      </div>
    </div>
  );
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
