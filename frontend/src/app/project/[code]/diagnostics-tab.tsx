"use client";

/**
 * Diagnostics tab v2 — the "Triage Board".
 *
 * Organized by what needs doing, not by pillar:
 *   Act now → Keep an eye on → On pace (compact) → Not reporting.
 * Mixed campaigns pool both engines' signals; engine + pillar become tags.
 * Evaluation history renders as ▲/▼ deltas, sparklines, and dot strips
 * (via the history endpoint's include_signals option).
 *
 * Preserved from v1: loading/empty/error states, the re-run button
 * (suppressed in retro mode), Retrospective Mode (asOfDate anchoring +
 * metadata callback), and the multi-phase breakdown panel — selecting a
 * phase dims signals whose engine has no lines in it.
 */
import { useEffect, useMemo, useState } from "react";
import { Activity, CheckCircle2, ChevronRight, Play } from "lucide-react";
import {
  api,
  type DiagnosticAlert,
  type DiagnosticHistoryPoint,
  type DiagnosticOutput,
  type PacingResponse,
  type PacingLine,
  type PhaseSummary,
} from "@/lib/api";
import {
  buildTriageModel,
  formatEvidence,
  PILLAR_LABELS,
  SIGNAL_EVIDENCE,
  SIGNAL_MEANINGS,
  type TriageEngineChip,
  type TriageSignal,
} from "@/lib/diagnostics";
import { BandScale } from "@/components/band-scale";
import { Card } from "@/components/card";
import { Btn, Eyebrow, Label } from "@/components/ui";
import { cn, formatCurrency, formatPercent, pacingColor, pacingStatus } from "@/lib/utils";
import { statusWord } from "@/lib/viz/health-core";

function statusVar(status: string | null | undefined): string {
  if (status === "STRONG") return "var(--ok)";
  if (status === "WATCH") return "var(--warn)";
  if (status === "ACTION") return "var(--danger)";
  return "var(--text-faint)";
}

export interface RetrospectiveMetadata {
  cached: boolean;
  engineVersion: string;
}

/* ── micro-viz ───────────────────────────────────────────────────── */

function DgDelta({ delta, size = 11 }: { delta: number | null; size?: number }) {
  if (delta == null) return null;
  const up = delta > 0;
  const flat = delta === 0;
  const c = flat ? "var(--text-faint)" : up ? "var(--ok)" : "var(--danger)";
  return (
    <span
      className="whitespace-nowrap font-mono font-semibold"
      style={{ fontSize: size, color: c }}
    >
      {flat ? "· 0" : (up ? "▲ " : "▼ ") + Math.abs(delta)}
    </span>
  );
}

function DgSpark({
  data,
  w = 56,
  h = 15,
  color = "var(--text-muted)",
}: {
  data: number[] | null;
  w?: number;
  h?: number;
  color?: string;
}) {
  if (!data || data.length < 2) return null;
  const min = Math.min(...data);
  const max = Math.max(...data);
  const rng = max - min || 1;
  const pts = data.map((v, i) => [
    (i / (data.length - 1)) * (w - 4) + 2,
    h - 2.5 - ((v - min) / rng) * (h - 5),
  ]);
  const d = pts
    .map((p, i) => (i ? "L" : "M") + p[0].toFixed(1) + " " + p[1].toFixed(1))
    .join(" ");
  const last = pts[pts.length - 1];
  return (
    <svg width={w} height={h} className="block flex-shrink-0 overflow-visible" aria-hidden="true">
      <path d={d} fill="none" stroke={color} strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
      <circle cx={last[0]} cy={last[1]} r="2" fill={color} />
    </svg>
  );
}

function DgDots({ dots, w = 64 }: { dots: number[]; w?: number }) {
  if (!dots || dots.length < 2) return null;
  return (
    <div
      className="flex items-end gap-[2.5px]"
      style={{ height: 22, width: w }}
      aria-hidden="true"
    >
      {dots.map((v, i) => {
        const last = i === dots.length - 1;
        const c = v >= 80 ? "var(--ok)" : v >= 60 ? "var(--warn)" : "var(--danger)";
        return (
          <div
            key={i}
            className="flex-1 rounded-[1px]"
            style={{
              height: Math.max(3, ((v - 40) / 60) * 22),
              background: c,
              opacity: last ? 1 : 0.38,
            }}
          />
        );
      })}
    </div>
  );
}

function DgEngine({ engine }: { engine: string }) {
  const isP = engine === "persuasion";
  return (
    <span
      className="inline-flex flex-shrink-0 items-center rounded-xs px-1.5 py-0.5 font-mono text-[8.5px] font-bold uppercase tracking-[0.1em]"
      title={`This signal comes from the ${engine} engine`}
      style={{
        background: isP
          ? "color-mix(in srgb, var(--info) 16%, transparent)"
          : "color-mix(in srgb, var(--accent-ink) 16%, transparent)",
        color: isP ? "var(--info)" : "var(--accent-ink)",
      }}
    >
      {isP ? "Persuasion" : "Conversion"}
    </span>
  );
}

function DgTag({ children }: { children: React.ReactNode }) {
  return (
    <span className="whitespace-nowrap rounded-xs border border-line px-1.5 py-0.5 font-mono text-[9px] font-semibold uppercase tracking-[0.1em] text-fg-muted">
      {children}
    </span>
  );
}

/* ── evidence (expand) ─────────────────────────────────────────────
   Three layers, plainest first:
     1. What this signal asks, in the user's words (SIGNAL_MEANINGS)
     2. The few numbers worth reading, human-labeled (SIGNAL_EVIDENCE)
     3. "All the numbers" — the full raw payload, collapsed by default */

function DgKV({ k, v }: { k: string; v: string }) {
  return (
    <span className="min-w-0 text-[11.5px] [overflow-wrap:anywhere]">
      <span className="text-fg-faint">{k}: </span>
      <span className="font-mono text-fg-secondary">{v}</span>
    </span>
  );
}

function formatInput(v: unknown): string {
  if (v == null) return "—";
  if (typeof v === "number")
    return Number.isInteger(v) ? v.toLocaleString() : v.toFixed(3);
  return String(v);
}

/* Raw payload, rendered as a readable tree rather than a JSON export:
   no braces, no quotes, no inner scrollbars. Empty fields are dropped
   entirely — an "excluded_no_metric: []" row reads like an unfinished
   corner of the tool, and an empty list carries no information. */

function rawLabel(k: string): string {
  return k.replace(/_/g, " ");
}

function isEmptyVal(v: unknown): boolean {
  if (v == null) return true;
  if (typeof v === "string") return v.length === 0;
  if (Array.isArray(v)) return v.length === 0;
  if (typeof v === "object") return Object.keys(v as object).length === 0;
  return false;
}

function RawTree({ v }: { v: unknown }): React.ReactElement {
  if (Array.isArray(v)) {
    if (v.every((x) => x == null || typeof x !== "object")) {
      return (
        <span className="font-mono text-[10.5px] text-fg-secondary [overflow-wrap:anywhere]">
          {v.map(formatInput).join(", ")}
        </span>
      );
    }
    return (
      <div className="space-y-2">
        {v.map((item, i) => (
          <div key={i} className="border-l border-line-soft pl-2.5">
            <RawTree v={item} />
          </div>
        ))}
      </div>
    );
  }
  if (v != null && typeof v === "object") {
    const entries = Object.entries(v as Record<string, unknown>).filter(
      ([, x]) => !isEmptyVal(x)
    );
    const scalars = entries.filter(([, x]) => x == null || typeof x !== "object");
    const nested = entries.filter(([, x]) => x != null && typeof x === "object");
    return (
      <div className="min-w-0 space-y-1.5">
        {scalars.length > 0 && (
          <div className="flex min-w-0 flex-wrap gap-x-3.5 gap-y-0.5">
            {scalars.map(([k, x]) => (
              <span key={k} className="text-[10.5px] [overflow-wrap:anywhere]">
                <span className="text-fg-faint">{rawLabel(k)} </span>
                <span className="font-mono text-fg-secondary">
                  {formatInput(x)}
                </span>
              </span>
            ))}
          </div>
        )}
        {nested.map(([k, x]) => (
          <div key={k} className="min-w-0">
            <div className="font-mono text-[9.5px] uppercase tracking-[0.08em] text-fg-faint">
              {rawLabel(k)}
            </div>
            <div className="mt-0.5 border-l border-line-soft pl-2.5">
              <RawTree v={x} />
            </div>
          </div>
        ))}
      </div>
    );
  }
  return (
    <span className="font-mono text-[10.5px] text-fg-secondary">
      {formatInput(v)}
    </span>
  );
}

function DgRawDump({ s }: { s: TriageSignal }) {
  const inputs = s.inputs ?? {};
  const entries = Object.entries(inputs).filter(([, v]) => !isEmptyVal(v));
  const scalars = entries.filter(([, v]) => typeof v !== "object");
  const objects = entries.filter(([, v]) => v != null && typeof v === "object");
  return (
    <div className="min-w-0 space-y-2.5 border-l-2 border-line-soft pl-3">
      <div className="flex min-w-0 flex-wrap gap-x-[18px] gap-y-1">
        {s.raw_value != null && <DgKV k="value" v={s.raw_value.toFixed(3)} />}
        {s.benchmark != null && <DgKV k="benchmark" v={s.benchmark.toFixed(3)} />}
        {s.floor != null && <DgKV k="floor" v={s.floor.toFixed(3)} />}
        {scalars.map(([k, v]) => (
          <DgKV key={k} k={rawLabel(k)} v={formatInput(v)} />
        ))}
      </div>
      {objects.map(([k, v]) => (
        <div key={k} className="min-w-0">
          <div className="font-mono text-[10px] uppercase tracking-[0.08em] text-fg-faint">
            {rawLabel(k)}
          </div>
          <div className="mt-1 rounded-sm bg-surface-sunken px-2.5 py-2">
            <RawTree v={v} />
          </div>
        </div>
      ))}
    </div>
  );
}

function DgEvidence({ s }: { s: TriageSignal }) {
  const [showRaw, setShowRaw] = useState(false);
  const meaning = SIGNAL_MEANINGS[s.id];
  const inputs = s.inputs ?? {};
  const facts = (SIGNAL_EVIDENCE[s.id] ?? []).filter(
    (f) => inputs[f.key] != null
  );
  return (
    <div className="min-w-0 space-y-2.5">
      {meaning && (
        <p className="max-w-[520px] text-[11.5px] leading-relaxed text-fg-muted">
          {meaning}
        </p>
      )}
      {facts.length > 0 && (
        <div className="flex min-w-0 flex-wrap gap-x-[18px] gap-y-1">
          {facts.map((f) => (
            <DgKV
              key={f.key}
              k={f.label}
              v={formatEvidence(inputs[f.key], f.fmt)}
            />
          ))}
        </div>
      )}
      {!s.guard_passed && s.guard_reason && (
        <DgKV k="Not scored" v={s.guard_reason.replace(/_/g, " ")} />
      )}
      <button
        onClick={(e) => {
          e.stopPropagation();
          setShowRaw(!showRaw);
        }}
        className="block font-mono text-[10px] uppercase tracking-[0.1em] text-fg-faint transition-colors duration-fast hover:text-fg-muted"
      >
        {showRaw ? "− Hide the numbers" : "+ All the numbers"}
      </button>
      {showRaw && <DgRawDump s={s} />}
    </div>
  );
}

function DgChevron({ open }: { open: boolean }) {
  return (
    <ChevronRight
      className={cn(
        "h-[13px] w-[13px] flex-shrink-0 text-fg-faint transition-transform duration-fast ease-snap",
        open && "rotate-90"
      )}
    />
  );
}

/* ── header chips ────────────────────────────────────────────────── */

function DgChip({ chip }: { chip: TriageEngineChip }) {
  return (
    <div className="rounded-md border-[1.5px] border-line bg-surface-card px-4 pb-[11px] pt-2.5">
      <div className="flex items-center gap-3">
        <span className="label text-[9.5px]">{chip.label}</span>
        <span
          className="tnum font-display text-[30px] leading-none"
          style={{ color: statusVar(chip.status) }}
        >
          {chip.score != null ? chip.score.toFixed(0) : "—"}
        </span>
        <div className="flex flex-col gap-0.5">
          <span
            className="font-mono text-[8.5px] font-bold tracking-[0.12em]"
            style={{ color: statusVar(chip.status) }}
          >
            {statusWord(chip.status)}
          </span>
          <DgDelta delta={chip.delta} size={10} />
        </div>
        <DgDots dots={chip.dots} />
      </div>
      {/* Gauge, not thermometer — see BandScale's rationale comment. */}
      <BandScale
        score={chip.score}
        color={statusVar(chip.status)}
        className="mt-2"
      />
    </div>
  );
}

/* ── zone cards ──────────────────────────────────────────────────── */

function DgActCard({
  s,
  mixed,
  dimmed,
}: {
  s: TriageSignal;
  mixed: boolean;
  dimmed: boolean;
}) {
  const [open, setOpen] = useState(false);
  return (
    <div
      className={cn(
        "min-w-0 flex-[1_1_380px] overflow-hidden rounded-md bg-surface-card shadow-hard transition-opacity",
        dimmed && "opacity-40"
      )}
      style={{ border: "1.5px solid color-mix(in srgb, var(--danger) 45%, transparent)" }}
    >
      <div
        className="px-[18px] pb-4 pt-4"
        style={{ background: "color-mix(in srgb, var(--danger) 7%, transparent)" }}
      >
        {/* Lead with the signal's plain name — "D3" alone means nothing
            to most users. The code stays as quiet metadata beside it. */}
        <div className="flex flex-wrap items-center gap-x-2 gap-y-1.5">
          <span className="font-mono text-[11px] font-bold text-danger">{s.id}</span>
          <span className="text-[11.5px] font-semibold text-fg-muted">{s.name}</span>
          {mixed && <DgEngine engine={s.engine} />}
          {s.pillar && <DgTag>{PILLAR_LABELS[s.pillar] ?? s.pillar}</DgTag>}
          <span className="ml-auto flex items-center gap-2">
            <DgSpark data={s.trend} w={62} h={16} color="var(--danger)" />
            <DgDelta delta={s.delta} />
          </span>
        </div>
        <p className="mt-3 text-[15px] font-semibold leading-normal text-fg">
          {s.diagnostic}
        </p>
        {/* Guidance, not a control: labelled prose that wraps freely.
            The old solid chip read as a clickable button and clipped
            against the card edge when the copy ran long. */}
        {s.action && (
          <p className="mt-3 min-w-0 text-[13px] leading-snug [overflow-wrap:anywhere]">
            <span className="font-mono text-[9.5px] font-bold uppercase tracking-[0.12em] text-accent-ink">
              Suggested move
            </span>
            <span className="mx-1.5 text-fg-faint">→</span>
            <span className="font-semibold text-fg-secondary">{s.action}</span>
          </p>
        )}
      </div>
      <button
        onClick={() => setOpen(!open)}
        className="flex w-full items-center justify-between gap-2.5 border-t border-line-soft px-[18px] py-[11px] text-left"
      >
        <span className="inline-flex items-center gap-[7px] font-mono text-[11px] text-fg-muted">
          <DgChevron open={open} />
          {open ? "Hide details" : "Details"}
        </span>
        <span className="font-mono text-[11px] text-fg-muted">
          score <b className="text-danger">{s.score?.toFixed(0) ?? "—"}</b>
        </span>
      </button>
      {open && (
        <div className="px-[18px] pb-4 pt-1">
          <DgEvidence s={s} />
        </div>
      )}
    </div>
  );
}

function DgWatchCard({
  s,
  mixed,
  dimmed,
}: {
  s: TriageSignal;
  mixed: boolean;
  dimmed: boolean;
}) {
  const [open, setOpen] = useState(false);
  return (
    <div
      className={cn(
        "rounded-md border-[1.5px] border-line bg-surface-card transition-opacity",
        dimmed && "opacity-40"
      )}
    >
      <button
        onClick={() => setOpen(!open)}
        className="block w-full px-[15px] py-[13px] text-left"
      >
        <div className="flex flex-wrap items-center gap-x-[7px] gap-y-1">
          <span className="font-mono text-[10.5px] font-bold text-warn">{s.id}</span>
          <span className="text-[11px] font-semibold text-fg-muted">{s.name}</span>
          {mixed && <DgEngine engine={s.engine} />}
          {s.pillar && <DgTag>{PILLAR_LABELS[s.pillar] ?? s.pillar}</DgTag>}
          <span className="ml-auto flex items-center gap-2">
            <DgSpark data={s.trend} w={52} h={14} color="var(--warn)" />
            <DgDelta delta={s.delta} size={10} />
          </span>
        </div>
        <p className="mt-[9px] text-[12.5px] leading-relaxed text-fg-secondary">
          {s.diagnostic}
        </p>
        <div className="mt-2.5 flex items-center justify-between">
          <span className="inline-flex items-center gap-1.5 font-mono text-[10px] text-fg-faint">
            <DgChevron open={open} />
            {open ? "Hide details" : "Details"}
          </span>
          <span className="tnum font-mono text-xs font-bold text-warn">
            {s.score?.toFixed(0) ?? "—"}
          </span>
        </div>
      </button>
      {open && (
        <div className="px-[15px] pb-[13px]">
          <DgEvidence s={s} />
        </div>
      )}
    </div>
  );
}

function DgHealthyRow({
  s,
  mixed,
  dimmed,
}: {
  s: TriageSignal;
  mixed: boolean;
  dimmed: boolean;
}) {
  const [open, setOpen] = useState(false);
  return (
    <div
      className={cn(
        "min-w-0 border-l border-t border-line-soft transition-opacity",
        dimmed && "opacity-40"
      )}
    >
      <button
        onClick={() => setOpen(!open)}
        className="flex w-full min-w-0 items-center gap-2.5 px-4 py-[9px] text-left"
      >
        <span className="w-[22px] flex-shrink-0 font-mono text-[10.5px] font-bold text-ok">
          {s.id}
        </span>
        {mixed && <DgEngine engine={s.engine} />}
        <span className="flex-1 truncate text-[12.5px] text-fg-secondary">{s.name}</span>
        <DgSpark data={s.trend} w={52} h={14} color="var(--ok)" />
        <span className="tnum w-6 text-right font-mono text-xs font-bold text-ok">
          {s.score?.toFixed(0) ?? "—"}
        </span>
        <span className="w-[34px] text-right">
          <DgDelta delta={s.delta} size={10} />
        </span>
      </button>
      {open && (
        <div className="px-4 pb-[11px] pl-12">
          <p className="mb-[7px] text-xs leading-relaxed text-fg-secondary">{s.diagnostic}</p>
          <DgEvidence s={s} />
        </div>
      )}
    </div>
  );
}

function DgZoneHead({
  color,
  title,
  meta,
}: {
  color: string;
  title: string;
  meta: string;
}) {
  return (
    <div className="mb-3 mt-[30px] flex items-baseline gap-3">
      <span
        className="font-display text-[22px] uppercase tracking-[0.01em]"
        style={{ color }}
      >
        {title}
      </span>
      <span className="font-mono text-[10.5px] text-fg-faint">{meta}</span>
    </div>
  );
}

/* ── phase breakdown (preserved from v1) ─────────────────────────── */

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
  const linesBySheet = new Map<string, number>();
  for (const l of lines) {
    if (!l.sheet_id) continue;
    linesBySheet.set(l.sheet_id, (linesBySheet.get(l.sheet_id) ?? 0) + 1);
  }

  return (
    <Card className="mt-5 space-y-3">
      <div className="flex items-baseline justify-between">
        <div>
          <Label className="text-fg-secondary">Phase breakdown</Label>
          <p className="mt-1 text-[11px] text-fg-muted">
            Click a phase to dim signals from engines with no lines in it.
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
                  : "border-line-soft bg-surface-sunken hover:border-line"
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
                {linesBySheet.get(phase.sheet_id) ?? phase.line_count} lines ·{" "}
                {formatCurrency(phase.planned_budget)} planned
              </div>
            </button>
          );
        })}
      </div>
    </Card>
  );
}

/* ── the tab ─────────────────────────────────────────────────────── */

export function DiagnosticsTab({
  code,
  asOfDate,
  onRetrospectiveMetadata,
}: {
  code: string;
  /** Retrospective Mode: fetch from the as-of endpoint; history anchors at
   *  this date; re-run is suppressed (snapshots are read-only). */
  asOfDate?: string;
  /** Lets the retro page surface cached/engine-version in its banner. */
  onRetrospectiveMetadata?: (meta: RetrospectiveMetadata) => void;
}) {
  const [outputs, setOutputs] = useState<DiagnosticOutput[]>([]);
  const [history, setHistory] = useState<DiagnosticHistoryPoint[]>([]);
  const [loading, setLoading] = useState(true);
  const [running, setRunning] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [pacing, setPacing] = useState<PacingResponse | null>(null);
  const [activeSheetId, setActiveSheetId] = useState<string | null>(null);

  const load = () => {
    setLoading(true);
    // Pacing feeds the phase panel; history feeds trends/deltas. Both are
    // non-fatal on failure — the board renders without them.
    api.pacing.get(code, asOfDate).then(setPacing).catch(() => setPacing(null));
    api.diagnostics
      .history(code, 30, undefined, asOfDate, true)
      .then(setHistory)
      .catch(() => setHistory([]));

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

  const model = useMemo(
    () => buildTriageModel(outputs, history),
    [outputs, history]
  );

  /* Phase dimming: engines whose output shares no line with the selected
     phase get their signals dimmed (same semantics as v1's card dimming,
     applied to the pooled board). */
  const dimEngines = useMemo(() => {
    if (!activeSheetId || !pacing) return new Set<string>();
    const phaseLineIds = new Set(
      pacing.lines
        .filter((l) => l.sheet_id === activeSheetId)
        .map((l) => l.line_id)
    );
    return new Set(
      outputs
        .filter((o) => !o.line_ids.some((id) => phaseLineIds.has(id)))
        .map((o) => o.campaign_type)
    );
  }, [activeSheetId, pacing, outputs]);
  const isDim = (s: TriageSignal) => dimEngines.has(s.engine);

  if (loading) {
    return (
      <div className="space-y-4">
        <Card className="animate-pulse">
          <div className="h-8 w-64 rounded bg-surface-sunken" />
          <div className="mt-4 flex gap-3">
            {[0, 1].map((i) => (
              <div key={i} className="h-14 w-56 rounded bg-surface-sunken" />
            ))}
          </div>
          <div className="mt-6 h-32 rounded bg-surface-sunken" />
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
        {error && <p className="text-xs text-danger">{error}</p>}
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

  const first = outputs[0];
  const campaignTypeLabel = model.mixed ? "mixed" : first.campaign_type;
  // Efficiency: per-engine metrics partition naturally (CPM/CPC from the
  // persuasion subset, CPA from conversion); merge first-non-null per key.
  const eff: Record<string, number | null> = {};
  for (const key of ["cpm", "cpc", "cpa", "cpcv", "pacing_pct"] as const) {
    eff[key] = outputs.map((o) => o.efficiency?.[key]).find((v) => v != null) ?? null;
  }
  const effBits: string[] = [];
  if (eff.cpm != null) effBits.push("CPM $" + eff.cpm.toFixed(2));
  if (eff.cpc != null) effBits.push("CPC $" + eff.cpc.toFixed(2));
  if (eff.cpa != null) effBits.push("CPA $" + eff.cpa.toFixed(2));
  if (eff.cpcv != null) effBits.push("CPCV $" + eff.cpcv.toFixed(3));
  if (eff.pacing_pct != null) effBits.push("Pacing " + eff.pacing_pct.toFixed(0) + "%");

  const { act, watch, strong, dead } = model;

  /* The engine emits one CRITICAL alert per ACTION signal. On this board
     the signal already renders as an ACT NOW card with the same
     diagnostic, so the per-signal alert banner is a duplicate — suppress
     it when its signal is on the board. Engine-level alerts (health
     regression, etc.) keep their banners, and signal alerts still
     surface everywhere else (Summary, Alerts page, Slack). */
  const onBoard = new Set([...act, ...watch].map((s) => s.id));
  const allAlerts: DiagnosticAlert[] = outputs
    .flatMap((o) => o.alerts ?? [])
    .filter((a) => !(a.signal_id && onBoard.has(a.signal_id)));
  const critAlerts = allAlerts.filter((a) => a.severity === "critical");
  const softAlerts = allAlerts.filter((a) => a.severity !== "critical");

  return (
    <div>
      {/* header row */}
      <div className="flex flex-wrap items-start justify-between gap-[18px]">
        <Eyebrow>Campaign health · {campaignTypeLabel}</Eyebrow>
        <div className="flex items-center gap-4">
          <div className="text-right font-mono">
            <div className="whitespace-nowrap text-[11px] text-fg-muted">
              Day {first.flight_day} of {first.flight_total_days} · evaluated{" "}
              {first.evaluation_date}
            </div>
            <div className="mt-[3px] whitespace-nowrap text-[10.5px] text-fg-faint">
              {effBits.join(" · ")}
            </div>
          </div>
          {!asOfDate && (
            <Btn
              variant="outline"
              size="sm"
              onClick={handleRun}
              disabled={running}
              icon={<Play className="h-3 w-3" />}
            >
              {running ? "Running…" : "Re-run diagnostic"}
            </Btn>
          )}
        </div>
      </div>

      {error && (
        <div className="mt-3 rounded-md border-2 border-tint-warn bg-tint-warn px-3 py-2 text-xs text-warn">
          {error}
        </div>
      )}

      {/* score chips */}
      <div className="mt-4 flex flex-wrap items-center gap-3">
        {model.chips.map((chip) => (
          <DgChip key={chip.id} chip={chip} />
        ))}
        <span className="ml-auto font-mono text-[10.5px] text-fg-faint">
          {model.signalsActive}/{model.signalsTotal} signals
          {model.coverage != null &&
            ` · ${(model.coverage * 100).toFixed(0)}% weight`}
        </span>
      </div>

      {/* phase breakdown (multi-plan projects) */}
      {pacing && pacing.phases && pacing.phases.length > 1 && (
        <PhaseBreakdownPanel
          phases={pacing.phases}
          lines={pacing.lines}
          activeSheetId={activeSheetId}
          onSelect={setActiveSheetId}
        />
      )}

      {/* ACT NOW */}
      <DgZoneHead
        color="var(--danger)"
        title="Act now"
        meta={
          act.length || critAlerts.length
            ? `${act.length} signal${act.length === 1 ? "" : "s"} off pace${
                critAlerts.length
                  ? ` + ${critAlerts.length} critical alert${critAlerts.length === 1 ? "" : "s"}`
                  : ""
              }`
            : "nothing requires action"
        }
      />
      {critAlerts.map((a, i) => (
        <div
          key={i}
          className="mb-3 flex items-center gap-3 rounded-md px-4 py-3"
          style={{
            border: "1.5px solid color-mix(in srgb, var(--danger) 50%, transparent)",
            background: "color-mix(in srgb, var(--danger) 9%, transparent)",
          }}
        >
          <span className="whitespace-nowrap font-mono text-[9.5px] font-bold uppercase tracking-[0.12em] text-danger">
            Critical · {a.type.replace(/_/g, " ")}
          </span>
          <span className="text-[13px] text-fg-secondary">{a.message}</span>
        </div>
      ))}
      {act.length > 0 ? (
        /* items-start: expanding one card must not stretch its row
           siblings — equalized heights read as "data missing" on the
           cards that simply aren't expanded. */
        <div className="flex flex-wrap items-start gap-3.5">
          {act.map((s) => (
            <DgActCard key={`${s.engine}-${s.id}`} s={s} mixed={model.mixed} dimmed={isDim(s)} />
          ))}
        </div>
      ) : (
        critAlerts.length === 0 && (
          <div className="flex items-center gap-2.5 rounded-md px-4 py-[13px]"
            style={{
              border: "1.5px solid color-mix(in srgb, var(--ok) 35%, transparent)",
              background: "color-mix(in srgb, var(--ok) 6%, transparent)",
            }}
          >
            <CheckCircle2 className="h-[15px] w-[15px] flex-shrink-0 text-ok" />
            <span className="text-[13px] text-fg-secondary">
              No signals off pace — nothing needs intervention today.
            </span>
          </div>
        )
      )}

      {/* KEEP AN EYE ON */}
      {(watch.length > 0 || softAlerts.length > 0) && (
        <>
          <DgZoneHead
            color="var(--warn)"
            title="Keep an eye on"
            meta={`${watch.length} signal${watch.length === 1 ? "" : "s"} drifting${
              softAlerts.length
                ? ` · ${softAlerts.length} alert${softAlerts.length === 1 ? "" : "s"}`
                : ""
            }`}
          />
          <div className="grid grid-cols-[repeat(auto-fill,minmax(330px,1fr))] items-start gap-3">
            {watch.map((s) => (
              <DgWatchCard key={`${s.engine}-${s.id}`} s={s} mixed={model.mixed} dimmed={isDim(s)} />
            ))}
            {softAlerts.map((a, i) => (
              <div
                key={"al" + i}
                className="rounded-md px-[15px] py-[13px]"
                style={{
                  border: "1.5px dashed color-mix(in srgb, var(--warn) 50%, transparent)",
                  background: "color-mix(in srgb, var(--warn) 5%, transparent)",
                }}
              >
                <div className="font-mono text-[9.5px] font-bold uppercase tracking-[0.12em] text-warn">
                  Alert · {a.type.replace(/_/g, " ")}
                </div>
                <p className="mt-[9px] text-[12.5px] leading-relaxed text-fg-secondary">
                  {a.message}
                </p>
              </div>
            ))}
          </div>
        </>
      )}

      {/* HEALTHY */}
      {strong.length > 0 && (
        <>
          <DgZoneHead
            color="var(--ok)"
            title="On pace"
            meta={`${strong.length} signals on pace — compact view`}
          />
          <div className="overflow-hidden rounded-md border-[1.5px] border-line-soft bg-surface-card">
            {/* -1px margins hide the rows' outer borders against the card edge */}
            <div className="-ml-px -mt-px grid grid-cols-[repeat(auto-fill,minmax(340px,1fr))]">
              {strong.map((s) => (
                <DgHealthyRow key={`${s.engine}-${s.id}`} s={s} mixed={model.mixed} dimmed={isDim(s)} />
              ))}
            </div>
          </div>
        </>
      )}

      {/* NOT REPORTING */}
      {dead.length > 0 && (
        <div className="mt-[22px] flex flex-wrap items-baseline gap-6 border-t border-line-soft pt-3.5">
          <span className="label whitespace-nowrap text-[9.5px]">Not reporting</span>
          <div className="flex flex-wrap gap-x-[22px] gap-y-1.5">
            {dead.map((s) => (
              <span key={`${s.engine}-${s.id}`} className="font-mono text-[10.5px] text-fg-faint">
                <b className="text-fg-muted">{s.id}</b> {s.name} —{" "}
                {(s.guard_reason || "guard failed").replace(/_/g, " ")}
              </span>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}
