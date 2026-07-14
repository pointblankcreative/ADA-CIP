"use client";

/**
 * Summary tab — the verdict-first campaign landing. Composes existing
 * endpoints (pacing + diagnostics + performance.daily + alerts) into the
 * v0.3 layout: VerdictHero → spend trajectory → pace drivers + health
 * mini → open alerts. No new API surface.
 */
import { useEffect, useMemo, useState } from "react";
import { ArrowRight, Bell, SatelliteDish } from "lucide-react";
import {
  api,
  type Alert,
  type DiagnosticOutput,
  type PacingLine,
  type PacingResponse,
  type PerformanceResponse,
  type Project,
} from "@/lib/api";
import { computeFlight, verdict } from "@/lib/flight";
import { statusWord } from "@/lib/viz/health-core";
import { BandScale } from "@/components/band-scale";
import { Card } from "@/components/card";
import { Label } from "@/components/ui";
import { PlatformIcon } from "@/components/platform-icon";
import { VerdictHero, type DiagRiskSummary } from "@/components/project/verdict-hero";
import { ProjectionChart } from "@/components/project/projection-chart";
import {
  cn,
  diagnosticVar,
  formatCurrencyCompact,
  formatPercent,
  pacingStatus,
  pacingVar,
  platformLabel,
  severityVar,
} from "@/lib/utils";
import { formatAlertSource } from "@/lib/alert-labels";

/** Roll the diagnostics the tab already holds into a worst-case risk count for
 *  the verdict note (#4). Counts action/watch signals across engines. Returns
 *  null when nothing is flagged (or diagnostics are empty / still loading), so
 *  the note never shows noise. Names no signal or pillar by design. */
function deriveDiagRisk(diagnostics: DiagnosticOutput[]): DiagRiskSummary | null {
  let actionCount = 0;
  let watchCount = 0;
  for (const d of diagnostics) {
    for (const s of d.signals) {
      if (s.status === "ACTION") actionCount += 1;
      else if (s.status === "WATCH") watchCount += 1;
    }
  }
  if (actionCount === 0 && watchCount === 0) return null;
  return { actionCount, watchCount };
}

export function SummaryTab({
  project,
  code,
  alerts,
  onAcknowledge,
  onTab,
}: {
  project: Project;
  code: string;
  /** Project alerts, fetched once by the shell (also feeds the tab badge). */
  alerts: Alert[];
  /** Acknowledge handler owned by the shell (optimistic state update). */
  onAcknowledge: (alertId: string, note?: string) => Promise<void>;
  onTab: (tab: string) => void;
}) {
  const [pacing, setPacing] = useState<PacingResponse | null>(null);
  const [diagnostics, setDiagnostics] = useState<DiagnosticOutput[]>([]);
  const [perf, setPerf] = useState<PerformanceResponse | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    setLoading(true);
    Promise.all([
      api.pacing.get(code).catch(() => null),
      api.diagnostics.get(code).catch(() => [] as DiagnosticOutput[]),
      api.performance.get(code).catch(() => null),
    ])
      .then(([pac, diag, performance]) => {
        setPacing(pac);
        setDiagnostics(diag);
        setPerf(performance);
      })
      .finally(() => setLoading(false));
  }, [code]);

  const f = useMemo(() => computeFlight(project), [project]);
  const v = useMemo(() => verdict(project, f), [project, f]);
  const diagRisk = useMemo(() => deriveDiagRisk(diagnostics), [diagnostics]);

  if (loading) {
    return (
      <div className="space-y-4">
        <Card className="animate-pulse">
          <div className="h-10 w-56 rounded bg-surface-sunken" />
          <div className="mt-4 h-4 w-80 rounded bg-surface-sunken" />
          <div className="mt-6 h-2 w-full rounded bg-surface-sunken" />
        </Card>
        <Card className="h-56 animate-pulse" />
      </div>
    );
  }

  return (
    <div className="flex flex-col gap-4">
      <VerdictHero
        p={project}
        f={f}
        v={v}
        asOf={pacing?.as_of_date ?? null}
        diagRisk={diagRisk}
        onTab={onTab}
      />

      {f.noData ? (
        <AwaitingData project={project} />
      ) : (
        <>
          <Card>
            <div className="mb-2 flex items-center justify-between gap-3">
              <Label className="text-fg-secondary">
                Spend trajectory → flight end
              </Label>
              <span className="font-mono text-[10.5px] uppercase text-fg-faint">
                Day {f.elapsed} / {f.flightTotal}
              </span>
            </div>
            <ProjectionChart p={project} f={f} daily={perf?.daily ?? null} />
          </Card>

          <div className="grid items-start gap-4 lg:grid-cols-2">
            {pacing && pacing.lines.length > 0 && (
              <PaceDrivers
                lines={pacing.lines}
                projectSpend={f.spend}
                onTab={onTab}
              />
            )}
            {diagnostics
              .filter((d) => d.health_score != null)
              .map((d) => (
                <HealthMini key={d.id} output={d} onTab={onTab} />
              ))}
          </div>
        </>
      )}

      {alerts.length > 0 && (
        <SummaryAlerts alerts={alerts} onAcknowledge={onAcknowledge} />
      )}
    </div>
  );
}

/* ── Awaiting data ───────────────────────────────────────────────── */

function AwaitingData({ project }: { project: Project }) {
  return (
    <Card className="flex flex-col items-center gap-3.5 px-6 py-14 text-center">
      <div className="flex h-12 w-12 items-center justify-center rounded-md border-[1.5px] border-tint-info bg-tint-info">
        <SatelliteDish className="h-[22px] w-[22px] text-info" />
      </div>
      <div>
        <h3 className="text-lg font-bold text-fg">Awaiting first data</h3>
        <p className="mx-auto mt-2 max-w-[420px] text-[13.5px] leading-relaxed text-fg-muted">
          This flight begins {project.start_date}. Pacing, performance, and
          diagnostics populate once the platforms start spending and the daily
          pipeline runs.
        </p>
      </div>
    </Card>
  );
}

/* ── PaceDrivers — the lines pushing the pace off plan ───────────── */

function lineLabel(l: PacingLine): string {
  if (l.audience_name) return l.audience_name;
  if (l.line_code && l.channel_category)
    return `${l.line_code} · ${l.channel_category}`;
  if (l.channel_category) return l.channel_category;
  return platformLabel(l.platform_id);
}

function PaceDrivers({
  lines,
  projectSpend,
  onTab,
}: {
  lines: PacingLine[];
  /** Project-level spend (FlightMath.spend) — used to detect the case where
   *  the flight clearly spent money but none of it is attributed to lines. */
  projectSpend: number;
  onTab: (tab: string) => void;
}) {
  const top = useMemo(() => {
    return [...lines]
      .map((l) => ({
        l,
        delta: (l.actual_spend_to_date ?? 0) - (l.planned_spend_to_date ?? 0),
      }))
      .sort((a, b) => Math.abs(b.delta) - Math.abs(a.delta))
      .slice(0, 4);
  }, [lines]);

  // Unattributed flight: the project clearly spent money, yet every line's
  // actual spend is zero. Rendering a wall of "$0 of $X · 0.0% · −$0 vs plan"
  // rows is noise — show a calm one-line notice instead.
  const unattributed = useMemo(
    () =>
      projectSpend > 0 &&
      lines.every((l) => (l.actual_spend_to_date ?? 0) <= 0),
    [lines, projectSpend]
  );

  return (
    <Card>
      <div className="mb-4 flex items-center justify-between gap-3">
        <Label className="text-fg-secondary">What&apos;s driving the pace</Label>
        <button
          onClick={() => onTab("pacing")}
          className="inline-flex items-center gap-[5px] font-mono text-[11px] tracking-[0.04em] text-accent-ink hover:opacity-80"
        >
          All lines <ArrowRight className="h-[13px] w-[13px]" />
        </button>
      </div>
      {unattributed ? (
        <div className="text-[13px] leading-relaxed text-fg-secondary">
          Line-level spend isn&apos;t attributed for this flight yet.
        </div>
      ) : (
      <div className="flex flex-col gap-2.5">
        {top.map(({ l, delta }) => {
          const status = pacingStatus(l.pacing_percentage);
          const color = pacingVar(status);
          const over = delta > 0;
          const pct =
            l.planned_budget > 0
              ? Math.min((l.actual_spend_to_date / l.planned_budget) * 100, 100)
              : 0;
          const plannedPct =
            l.planned_budget > 0
              ? Math.min(
                  (l.planned_spend_to_date / l.planned_budget) * 100,
                  100
                )
              : null;
          return (
            <div
              key={l.line_id}
              className="grid grid-cols-[34px_minmax(0,1fr)_auto] items-center gap-3.5 sm:grid-cols-[34px_minmax(0,1fr)_150px_auto]"
            >
              <PlatformIcon platformId={l.platform_id} size={32} />
              <div className="min-w-0">
                <div className="truncate text-[13.5px] font-bold text-fg">
                  {lineLabel(l)}
                </div>
                <div className="mt-0.5 font-mono text-[10.5px] text-fg-faint">
                  {platformLabel(l.platform_id)} ·{" "}
                  {formatCurrencyCompact(l.actual_spend_to_date)} of{" "}
                  {formatCurrencyCompact(l.planned_budget)}
                </div>
              </div>
              <div className="hidden sm:block">
                <div className="relative h-2 w-full overflow-hidden rounded-pill bg-surface-sunken">
                  <div
                    className="h-full rounded-pill"
                    style={{ width: `${pct}%`, background: color }}
                  />
                  {plannedPct != null && (
                    <div
                      className="absolute -bottom-px -top-px z-[2] w-0.5 bg-fg-secondary"
                      style={{ left: `${plannedPct}%` }}
                    />
                  )}
                </div>
              </div>
              <div className="whitespace-nowrap text-right">
                <div
                  className="tnum font-mono text-[13px] font-bold"
                  style={{ color }}
                >
                  {formatPercent(l.pacing_percentage)}
                </div>
                <div
                  className={cn(
                    "mt-0.5 font-mono text-[10px]",
                    over ? "text-warn" : "text-fg-faint"
                  )}
                >
                  {over ? "+" : "−"}
                  {formatCurrencyCompact(Math.abs(delta))} vs plan
                </div>
              </div>
            </div>
          );
        })}
      </div>
      )}
    </Card>
  );
}

/* ── HealthMini — diagnostics pillars at a glance ────────────────── */

const PILLAR_ORDER: Record<string, string[]> = {
  persuasion: ["distribution", "attention", "resonance"],
  conversion: ["acquisition", "funnel"],
};
const PILLAR_LABELS: Record<string, string> = {
  distribution: "Distribution",
  attention: "Attention",
  resonance: "Resonance",
  acquisition: "Acquisition",
  funnel: "Funnel",
};

function HealthMini({
  output,
  onTab,
}: {
  output: DiagnosticOutput;
  onTab: (tab: string) => void;
}) {
  const order = PILLAR_ORDER[output.campaign_type] ?? Object.keys(output.pillars);
  const color = diagnosticVar(output.health_status);
  const pillars = Object.values(output.pillars);
  const active = pillars.reduce((n, p) => n + (p.signals_active ?? 0), 0);
  const total = pillars.reduce((n, p) => n + (p.signals_total ?? 0), 0);

  return (
    <Card>
      <div className="mb-4 flex items-center justify-between gap-3">
        <Label className="text-fg-secondary">
          Creative &amp; delivery health
          {output.campaign_type ? ` · ${output.campaign_type}` : ""}
        </Label>
        <button
          onClick={() => onTab("diagnostics")}
          className="inline-flex items-center gap-[5px] font-mono text-[11px] tracking-[0.04em] text-accent-ink hover:opacity-80"
        >
          Full diagnostics <ArrowRight className="h-[13px] w-[13px]" />
        </button>
      </div>
      <div className="mb-3 flex items-center gap-[18px]">
        <span
          className="tnum font-display text-[40px] leading-[0.9]"
          style={{ color }}
        >
          {output.health_score?.toFixed(0)}
        </span>
        <div>
          <span
            className="inline-flex items-center gap-1.5 rounded-pill px-2.5 py-1 font-mono text-[11.5px] font-semibold uppercase tracking-[0.06em]"
            style={{
              color,
              backgroundColor: `color-mix(in srgb, ${color} 13%, transparent)`,
              border: `1.5px solid color-mix(in srgb, ${color} 40%, transparent)`,
            }}
          >
            {statusWord(output.health_status)}
          </span>
          <div className="mt-1.5 font-mono text-[10.5px] text-fg-faint">
            {active}/{total} signals
            {output.health_coverage != null &&
              ` · ${(output.health_coverage * 100).toFixed(0)}% coverage`}
          </div>
        </div>
      </div>
      {/* Gauge, not thermometer: the goal is the on-pace zone, not 100 —
          see BandScale's rationale comment. */}
      <BandScale
        score={output.health_score}
        color={color}
        className="mb-[18px]"
      />
      <div className="flex flex-col gap-[13px]">
        {order.map((k) => {
          const pl = output.pillars[k];
          if (!pl) return null;
          const c = diagnosticVar(pl.status);
          return (
            <div key={k}>
              <div className="mb-1.5 flex items-baseline justify-between">
                <span className="text-[12.5px] font-semibold capitalize text-fg">
                  {PILLAR_LABELS[k] ?? k}
                </span>
                <span
                  className="font-mono text-[11px] font-semibold"
                  style={{ color: c }}
                >
                  {pl.score != null ? `${pl.score.toFixed(0)} · ` : ""}
                  {statusWord(pl.status)}
                </span>
              </div>
              <BandScale score={pl.score} color={c} />
            </div>
          );
        })}
      </div>
    </Card>
  );
}

/* ── SummaryAlerts — open alerts folded into the landing ─────────── */

function formatTimeAgo(dateStr: string): string {
  const diffMs = Date.now() - new Date(dateStr).getTime();
  const diffMin = Math.floor(diffMs / 60000);
  if (diffMin < 60) return `${diffMin}m ago`;
  const diffH = Math.floor(diffMin / 60);
  if (diffH < 24) return `${diffH}h ago`;
  return `${Math.floor(diffH / 24)}d ago`;
}

function SummaryAlerts({
  alerts,
  onAcknowledge,
}: {
  alerts: Alert[];
  onAcknowledge: (alertId: string, note?: string) => Promise<void>;
}) {
  const [busy, setBusy] = useState<string | null>(null);
  // Which alert has its note form open, and the draft note text.
  const [noteFor, setNoteFor] = useState<string | null>(null);
  const [noteText, setNoteText] = useState("");

  const ack = async (id: string) => {
    setBusy(id);
    try {
      await onAcknowledge(id, noteText || undefined);
      setNoteFor(null);
      setNoteText("");
    } catch {
      /* leave the alert as-is; the form stays open */
    } finally {
      setBusy(null);
    }
  };

  return (
    <Card className="overflow-hidden p-0">
      <div className="flex items-center gap-2.5 px-[18px] pt-4">
        <Bell className="h-[15px] w-[15px] text-fg-muted" />
        <Label className="text-fg-secondary">
          Open alerts · {alerts.filter((a) => !a.acknowledged_at).length}
        </Label>
      </div>
      <div className="mt-3">
        {alerts.map((a, i) => (
          <div
            key={a.alert_id}
            className={cn(
              "px-[18px] py-3",
              i > 0 && "border-t border-line-soft",
              a.acknowledged_at && "opacity-60"
            )}
          >
            <div className="flex items-start gap-3">
              <span
                className="mt-1.5 h-[7px] w-[7px] flex-shrink-0 rounded-full"
                style={{ backgroundColor: severityVar(a.severity) }}
              />
              <div className="min-w-0 flex-1">
                <div className="text-[13px] font-semibold leading-snug text-fg">
                  {a.title}
                </div>
                <div className="mt-0.5 text-[13px] leading-relaxed text-fg-secondary">
                  {a.message}
                </div>
                <div className="mt-1 font-mono text-[10px] uppercase tracking-[0.06em] text-fg-faint">
                  {formatAlertSource(a.alert_type)} ·{" "}
                  {formatTimeAgo(a.created_at)}
                  {a.acknowledged_at && (
                    <span className="text-ok">
                      {" "}
                      · acknowledged
                      {a.acknowledged_by ? ` by ${a.acknowledged_by}` : ""}
                    </span>
                  )}
                </div>
                {a.ack_note && (
                  <div className="mt-1 text-xs italic text-fg-muted">
                    “{a.ack_note}”
                  </div>
                )}
              </div>
              {!a.acknowledged_at && noteFor !== a.alert_id && (
                <button
                  onClick={() => {
                    setNoteFor(a.alert_id);
                    setNoteText("");
                  }}
                  className="flex-shrink-0 rounded-sm border-2 border-line px-2 py-1 font-mono text-[10px] font-semibold uppercase tracking-[0.06em] text-fg-muted transition-colors hover:border-line-strong hover:text-fg"
                  title="Acknowledge this alert"
                >
                  Ack
                </button>
              )}
            </div>
            {/* Acknowledge form: optional action note */}
            {!a.acknowledged_at && noteFor === a.alert_id && (
              <div className="mt-2.5 flex flex-wrap items-center gap-2 pl-[19px]">
                <input
                  autoFocus
                  value={noteText}
                  onChange={(e) => setNoteText(e.target.value)}
                  onKeyDown={(e) => {
                    if (e.key === "Enter") ack(a.alert_id);
                    if (e.key === "Escape") setNoteFor(null);
                  }}
                  maxLength={1000}
                  placeholder="Optional — what did you do? e.g. lowered Meta daily caps"
                  className="min-w-[220px] flex-1 rounded-sm border-2 border-line bg-surface-sunken px-2.5 py-1.5 text-xs text-fg placeholder:text-fg-faint outline-none focus:border-accent"
                />
                <button
                  onClick={() => ack(a.alert_id)}
                  disabled={busy === a.alert_id}
                  className="rounded-sm border-2 border-accent bg-accent px-2.5 py-1.5 font-mono text-[10px] font-bold uppercase tracking-[0.06em] text-on-accent hover:bg-accent-hover disabled:opacity-50"
                >
                  {busy === a.alert_id ? "…" : "Acknowledge"}
                </button>
                <button
                  onClick={() => setNoteFor(null)}
                  className="rounded-sm border-2 border-line px-2.5 py-1.5 font-mono text-[10px] font-semibold uppercase tracking-[0.06em] text-fg-muted hover:text-fg"
                >
                  Cancel
                </button>
              </div>
            )}
          </div>
        ))}
      </div>
    </Card>
  );
}
