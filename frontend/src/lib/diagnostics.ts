/**
 * Triage Board model — turns raw DiagnosticOutput(s) + evaluation history
 * into the pooled, enriched shape the redesigned Diagnostics tab renders.
 *
 * Mixed campaigns produce two engine outputs (persuasion + conversion);
 * the board pools their signals and tags each with its engine. Pillars are
 * derived from signal ID prefixes; trends and deltas come from the history
 * endpoint's per-signal scores (include_signals=true).
 */
import type {
  DiagnosticHistoryPoint,
  DiagnosticOutput,
  DiagnosticSignal,
} from "@/lib/api";

/* ── Pillar derivation — signal IDs encode their pillar ─────────────── */

export const PILLAR_LABELS: Record<string, string> = {
  distribution: "Distribution",
  attention: "Attention",
  resonance: "Resonance",
  acquisition: "Acquisition",
  funnel: "Funnel",
  quality: "Quality",
};

const PILLAR_BY_PREFIX: Record<string, string> = {
  D: "distribution",
  A: "attention",
  R: "resonance",
  C: "acquisition",
  F: "funnel",
  Q: "quality",
};

export function signalPillar(id: string): string | null {
  return PILLAR_BY_PREFIX[id?.charAt(0)?.toUpperCase()] ?? null;
}

/* ── Curated action copy — shown on ACT NOW cards ───────────────────────
   Keyed by signal ID, matched to the PRODUCTION signal definitions (the
   `name=` arguments at the SignalResult construction sites — same source
   of truth as lib/alert-labels.ts SIGNAL_NAMES). Deliberately
   generic-but-useful imperatives; tune freely, this file is the single
   source. A future engine version can supply per-evaluation actions and
   this map becomes the fallback. Unknown IDs simply render no chip. */

export const SIGNAL_ACTIONS: Record<string, string> = {
  // Persuasion · Distribution
  D1: "Rebalance budget toward efficient-reach lines", // Reach Attainment
  D2: "Consolidate audiences to build frequency", // Frequency Adequacy
  D3: "Rebalance platform budgets to even out delivery", // Frequency Distribution
  D4: "Trim overlapping audiences and push new reach", // Incremental Reach
  D5: "Confirm platform delivery and smooth daily pacing", // Delivery Cadence
  // Persuasion · Attention
  A1: "Test shorter cuts or stronger openings", // Video Completion Quality
  A3: "Shift spend to higher-viewability placements", // Viewability
  A4: "Refresh the creative rotation", // Focused View
  A5: "Rotate in fresh creative this week", // Creative Fatigue
  // Persuasion · Resonance
  R1: "Review creative tone against engagement quality", // Engagement Quality Ratio
  R3: "Tighten the landing page path", // Landing Page Depth
  // Conversion · Acquisition
  C1: "Shift budget to the cheapest converting lines", // CPA vs Target
  C2: "Raise caps on converting lines", // Volume Trajectory
  C3: "Refresh audiences before CPA creep compounds", // CPA Trend
  // Conversion · Funnel
  F1: "Test new hooks and calls to action", // Click-Through Rate
  F2: "Fix link tags and landing page load", // Landing Page Load Rate
  F3: "Move the form above the fold", // Scroll & Form Discovery
  F4: "Cut form fields to reduce friction", // Form Completion Rate
  F5: "Strengthen the post-conversion journey", // Post-Conversion Activation
};

/* ── Triage model ───────────────────────────────────────────────────── */

export interface TriageSignal extends DiagnosticSignal {
  /** Which engine produced this signal (campaign_type of its output). */
  engine: string;
  /** Derived pillar key (distribution / attention / …) or null. */
  pillar: string | null;
  /** Score change vs the previous evaluation, when history is available. */
  delta: number | null;
  /** Trailing per-evaluation scores (oldest → newest), max ~6 points. */
  trend: number[] | null;
  /** Curated action suggestion (ACTION cards only). */
  action: string | null;
}

export interface TriageEngineChip {
  /** Chip label: "Campaign" for single-engine, engine name for mixed. */
  id: string;
  label: string;
  score: number | null;
  status: DiagnosticOutput["health_status"];
  delta: number | null;
  /** Trailing health scores (oldest → newest) for the dot strip. */
  dots: number[];
}

export interface TriageModel {
  mixed: boolean;
  chips: TriageEngineChip[];
  signals: TriageSignal[];
  act: TriageSignal[];
  watch: TriageSignal[];
  strong: TriageSignal[];
  dead: TriageSignal[];
  signalsActive: number;
  signalsTotal: number;
  /** Mean coverage across outputs (null when no output reports it). */
  coverage: number | null;
}

const TREND_POINTS = 6;
const DOT_POINTS = 9;

function lastN<T>(arr: T[], n: number): T[] {
  return arr.length > n ? arr.slice(arr.length - n) : arr;
}

/** Per-signal score series from history rows of one campaign_type. */
function signalSeries(
  history: DiagnosticHistoryPoint[],
  campaignType: string
): Map<string, number[]> {
  const series = new Map<string, number[]>();
  for (const row of history) {
    if (row.campaign_type !== campaignType || !row.signals) continue;
    for (const s of row.signals) {
      if (s.score == null) continue;
      const arr = series.get(s.id) ?? [];
      arr.push(s.score);
      series.set(s.id, arr);
    }
  }
  return series;
}

/** Health-score series for one campaign_type (oldest → newest). */
function healthSeries(
  history: DiagnosticHistoryPoint[],
  campaignType: string
): number[] {
  return history
    .filter((r) => r.campaign_type === campaignType && r.health_score != null)
    .map((r) => r.health_score as number);
}

function deltaOf(series: number[] | undefined): number | null {
  if (!series || series.length < 2) return null;
  return Math.round(
    series[series.length - 1] - series[series.length - 2]
  );
}

/**
 * Build the Triage Board model from the live outputs + history.
 * `history` may be empty (endpoint failed / no snapshots) — trends, deltas
 * and dot strips simply don't render.
 */
export function buildTriageModel(
  outputs: DiagnosticOutput[],
  history: DiagnosticHistoryPoint[]
): TriageModel {
  const mixed = outputs.length > 1;

  const signals: TriageSignal[] = outputs.flatMap((out) => {
    const series = signalSeries(history, out.campaign_type);
    return out.signals.map((s) => {
      const trendFull = series.get(s.id);
      const trend = trendFull ? lastN(trendFull, TREND_POINTS) : null;
      return {
        ...s,
        engine: out.campaign_type,
        pillar: signalPillar(s.id),
        delta: deltaOf(trendFull),
        trend: trend && trend.length >= 2 ? trend : null,
        action:
          s.status === "ACTION" ? (SIGNAL_ACTIONS[s.id] ?? null) : null,
      };
    });
  });

  const chips: TriageEngineChip[] = outputs.map((out) => {
    const hs = healthSeries(history, out.campaign_type);
    return {
      id: out.campaign_type,
      label: mixed ? out.campaign_type : "Campaign",
      score: out.health_score,
      status: out.health_status,
      delta: deltaOf(hs),
      dots: lastN(hs, DOT_POINTS),
    };
  });

  const byScoreAsc = (a: TriageSignal, b: TriageSignal) =>
    (a.score ?? 101) - (b.score ?? 101);
  const live = signals.filter((s) => s.guard_passed);

  const coverages = outputs
    .map((o) => o.health_coverage)
    .filter((c): c is number => c != null);

  return {
    mixed,
    chips,
    signals,
    act: live.filter((s) => s.status === "ACTION").sort(byScoreAsc),
    watch: live.filter((s) => s.status === "WATCH").sort(byScoreAsc),
    strong: live
      .filter((s) => s.status === "STRONG")
      .sort((a, b) => (b.score ?? -1) - (a.score ?? -1)),
    dead: signals.filter((s) => !s.guard_passed),
    signalsActive: live.filter((s) => s.status != null).length,
    signalsTotal: signals.length,
    coverage: coverages.length
      ? coverages.reduce((a, c) => a + c, 0) / coverages.length
      : null,
  };
}
