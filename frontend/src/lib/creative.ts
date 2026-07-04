/**
 * Creative verdict engine — sibling of lib/flight.ts.
 *
 * First principles: the only creative question a media buyer has is
 * "who earns the next dollar, and who is burnt out?" Everything here
 * answers that literally, computed client-side from the rotation
 * endpoint plus PB campaign-history benchmark quartiles.
 *
 * The campaign objective declares ONE primary KPI:
 *   awareness          → completion_rate (higher is better)
 *   conversion / mixed → cpa (lower is better)
 * Explicitly NOT cost-per-completed-view — product decision: clients
 * see rates, not internal cost-per-attention constructions.
 */
import type {
  BenchmarkResponse,
  ObjectiveType,
  RotationCreative,
} from "@/lib/api";
import { formatNumberCompact } from "@/lib/utils";

/* ── Quartile reads against PB campaign history ──────────────────── */

/**
 * PB campaign-history quartiles for one metric, numerically ordered
 * (p25 < p50 < p75) the way the benchmarks endpoint stores them.
 * Direction is applied here via lowerIsBetter.
 */
export interface QuartileBench {
  p25: number;
  p50: number;
  p75: number;
  lowerIsBetter?: boolean;
}

export type QuartileWord =
  | "TOP QUARTILE"
  | "ABOVE MEDIAN"
  | "BELOW MEDIAN"
  | "BOTTOM QUARTILE";

export interface QuartileRead {
  word: QuartileWord;
  tone: "ok" | "warn" | "danger";
  /** CSS colour token for the tone. */
  color: string;
  /** 0 (bottom quartile) → 3 (top quartile). */
  rank: 0 | 1 | 2 | 3;
  /** Marker position 0–1 along the track (p25..p75 occupy 22%..78%). */
  pos: number;
  tick25: number;
  tick50: number;
  tick75: number;
}

const TONE_VARS: Record<QuartileRead["tone"], string> = {
  ok: "var(--ok)",
  warn: "var(--warn)",
  danger: "var(--danger)",
};

/**
 * Where does this campaign's number sit against PB history?
 * Returns null when there's no value or no benchmark — callers render
 * an explicit NO BENCHMARK state rather than guessing.
 */
export function quartileRead(
  value: number | null | undefined,
  bench: QuartileBench | null | undefined
): QuartileRead | null {
  if (value == null || bench == null) return null;
  // Normalize to "bigger is better" space so the comparisons read once.
  const flip = bench.lowerIsBetter ? -1 : 1;
  const v = flip * value;
  const qBad = flip * (bench.lowerIsBetter ? bench.p75 : bench.p25);
  const qMid = flip * bench.p50;
  const qGood = flip * (bench.lowerIsBetter ? bench.p25 : bench.p75);

  let word: QuartileWord;
  let tone: QuartileRead["tone"];
  let rank: QuartileRead["rank"];
  if (v >= qGood) {
    word = "TOP QUARTILE";
    tone = "ok";
    rank = 3;
  } else if (v >= qMid) {
    word = "ABOVE MEDIAN";
    tone = "ok";
    rank = 2;
  } else if (v >= qBad) {
    word = "BELOW MEDIAN";
    tone = "warn";
    rank = 1;
  } else {
    word = "BOTTOM QUARTILE";
    tone = "danger";
    rank = 0;
  }

  const span = qGood - qBad || 1;
  const pos = Math.max(
    0.04,
    Math.min(0.95, 0.22 + ((v - qBad) / span) * 0.56)
  );
  return {
    word,
    tone,
    color: TONE_VARS[tone],
    rank,
    pos,
    tick25: 0.22,
    tick50: 0.22 + ((qMid - qBad) / span) * 0.56,
    tick75: 0.78,
  };
}

/* ── #17: per-metric direction cue for a quartile word ───────────── */

export type CueMetric =
  | "cpm"
  | "cpc"
  | "cpa"
  | "ctr"
  | "completion_rate"
  | "frequency";

const QUARTILE_CUES: Record<CueMetric, Record<QuartileWord, string>> = {
  cpm: {
    "TOP QUARTILE": "cheaper reach than most PB campaigns, worth a quick quality check",
    "ABOVE MEDIAN": "cheaper reach than the PB median, worth a quick quality check",
    "BELOW MEDIAN": "costlier reach than the PB median",
    "BOTTOM QUARTILE": "costlier reach than most PB campaigns",
  },
  cpc: {
    "TOP QUARTILE": "cheaper clicks than most PB campaigns",
    "ABOVE MEDIAN": "cheaper clicks than the PB median",
    "BELOW MEDIAN": "pricier clicks than the PB median",
    "BOTTOM QUARTILE": "pricier clicks than most PB campaigns",
  },
  cpa: {
    "TOP QUARTILE": "cheaper results than most PB campaigns",
    "ABOVE MEDIAN": "cheaper results than the PB median",
    "BELOW MEDIAN": "pricier results than the PB median",
    "BOTTOM QUARTILE": "pricier results than most PB campaigns",
  },
  ctr: {
    "TOP QUARTILE": "stronger click-through than most PB campaigns",
    "ABOVE MEDIAN": "stronger click-through than the PB median",
    "BELOW MEDIAN": "softer click-through than the PB median",
    "BOTTOM QUARTILE": "softer click-through than most PB campaigns",
  },
  completion_rate: {
    "TOP QUARTILE": "more of the video finished than most PB campaigns",
    "ABOVE MEDIAN": "more finished than the PB median",
    "BELOW MEDIAN": "fewer finished than the PB median",
    "BOTTOM QUARTILE": "fewer finished than most PB campaigns",
  },
  frequency: {
    "TOP QUARTILE": "light exposure, lots of room to build",
    "ABOVE MEDIAN": "light exposure, room to build",
    "BELOW MEDIAN": "heavy exposure, watch for fatigue",
    "BOTTOM QUARTILE": "heavy exposure, fatigue risk",
  },
};

export function quartileCue(
  metric: CueMetric | null | undefined,
  word: QuartileWord | null | undefined
): string | null {
  if (!metric || !word) return null;
  return QUARTILE_CUES[metric]?.[word] ?? null;
}

/* ── Benchmarks → the metrics the creative surfaces read ─────────── */

export interface CreativeBenches {
  ctr?: QuartileBench;
  cpm?: QuartileBench;
  cpc?: QuartileBench;
  cpa?: QuartileBench;
  conversion_rate?: QuartileBench;
  /** Backend metric name is `vcr`. */
  completion_rate?: QuartileBench;
  hook_rate?: QuartileBench;
  engagement_rate?: QuartileBench;
  frequency?: QuartileBench;
}

const LOWER_IS_BETTER = new Set(["cpm", "cpc", "cpa", "frequency"]);

/** Map the benchmarks endpoint payload into direction-aware quartiles. */
export function buildBenches(
  bench: BenchmarkResponse | null | undefined
): CreativeBenches {
  const out: CreativeBenches = {};
  if (!bench) return out;
  const take = (metric: string): QuartileBench | undefined => {
    const bv = bench.benchmarks[metric];
    if (!bv || bv.p25 == null || bv.p50 == null || bv.p75 == null) {
      return undefined;
    }
    return {
      p25: bv.p25,
      p50: bv.p50,
      p75: bv.p75,
      lowerIsBetter: LOWER_IS_BETTER.has(metric),
    };
  };
  out.ctr = take("ctr");
  out.cpm = take("cpm");
  out.cpc = take("cpc");
  out.cpa = take("cpa");
  out.conversion_rate = take("conversion_rate");
  out.completion_rate = take("vcr");
  out.hook_rate = take("hook_rate");
  out.engagement_rate = take("engagement_rate");
  out.frequency = take("frequency");
  return out;
}

/* ── The primary KPI, declared by the objective ──────────────────── */

export interface PrimaryKpi {
  id: "completion_rate" | "cpa";
  label: string;
  lowerIsBetter: boolean;
  /** Plain-language rationale for the hero footer. */
  why: string;
  /** Legend line under the rotation cards. */
  stages: string;
}

export function primaryKpi(objective: ObjectiveType): PrimaryKpi {
  if (objective === "awareness") {
    return {
      id: "completion_rate",
      label: "Completion",
      lowerIsBetter: false,
      why:
        "This flight is bought for reach and attention, not clicks. " +
        "Completed views set the rank: hook rate backs it up, frequency " +
        "is the guardrail. Click metrics are shown for completeness, not " +
        "as a KPI.",
      stages:
        "Hook rate = 3-second views over impressions. Watched = completed " +
        "views. Clicked = click-through.",
    };
  }
  return {
    id: "cpa",
    label: "Cost per result",
    lowerIsBetter: true,
    why:
      "This flight is bought for results, so dollars per result set the " +
      "rank. Attention metrics explain the price: they don't set it.",
    stages:
      "Hook rate = 3-second views over impressions. Watched = completed " +
      "views. Clicked = click-through. Converted = platform-attributed " +
      "cost per result.",
  };
}

/* ── #5: the report-ready conversions number ─────────────────────────
   Platform-attributed conversions are the single defensible figure for a
   client report; GA4 tracks a broader set of site key events separately.
   This is a small honest selector — it does not blend, it declares which
   number the report should quote. */

export function pickReportConversions(
  platformConv: number,
  ga4Conv: number,
  objective: string
): { value: number; source: "platform" | "ga4" | "none" } {
  if (platformConv > 0) return { value: platformConv, source: "platform" };
  if (ga4Conv > 0) return { value: ga4Conv, source: "ga4" };
  return { value: 0, source: "none" };
}

/* ── #19: unresolved-creative guard ──────────────────────────────────
   A variant whose name never resolved to a real asset: empty/whitespace,
   the literal "Unknown", the "Unnamed creative" fallback, or a bare
   dimensions string (e.g. "2160x3840"). These are not graded — they are
   flagged as needing a name. */

export function isUnresolvedVariant(name: string): boolean {
  if (!name || !name.trim()) return true;
  const n = name.trim();
  if (/^unknown$/i.test(n)) return true;
  if (/^unnamed creative$/i.test(n)) return true;
  if (/^\s*\d+\s*[x×]\s*\d+\s*$/.test(n)) return true;
  return false;
}

/* ── Verdicts: SCALE / HOLD / REFRESH / EARLY / UNRESOLVED ────────── */

export type CreativeVerdict =
  | "SCALE"
  | "HOLD"
  | "REFRESH"
  | "EARLY"
  | "UNRESOLVED";

export interface JudgedCreative {
  creative: RotationCreative;
  verdict: CreativeVerdict;
  /** CSS colour token for borders / accents. */
  tone: string;
  /** One plain sentence on why the verdict landed. */
  reason: string;
  fatigued: boolean;
  /** The value that sets this creative's rank (rate fraction or $CPA).
   *  Awareness statics rank on engagement because they have no
   *  completion signal. */
  primaryValue: number | null;
  primaryRead: QuartileRead | null;
}

/** Volume guard: below this, no judgment is rendered. */
const VOLUME_MIN_IMPRESSIONS = 1000;
const VOLUME_MIN_SPEND = 100;
/** Latest frequency above this reads as fatigue. */
const FATIGUE_FREQUENCY = 4;
/** "Materially declining": CTR down 25%+ across the trend window. */
const FATIGUE_CTR_DROP = 0.25;
const FATIGUE_MIN_POINTS = 5;

export function isFatigued(cr: RotationCreative): boolean {
  const freqTrend = cr.trend?.frequency ?? [];
  const latestFreq = freqTrend.length
    ? freqTrend[freqTrend.length - 1]
    : cr.frequency;
  if (latestFreq != null && latestFreq > FATIGUE_FREQUENCY) return true;

  const ctrTrend = (cr.trend?.ctr ?? []).filter((v) => v != null && v > 0);
  if (ctrTrend.length >= FATIGUE_MIN_POINTS) {
    const first = ctrTrend[0];
    const last = ctrTrend[ctrTrend.length - 1];
    if (first > 0 && (first - last) / first >= FATIGUE_CTR_DROP) return true;
  }
  return false;
}

export function underVolumeGuard(cr: RotationCreative): boolean {
  return (
    cr.impressions < VOLUME_MIN_IMPRESSIONS || cr.spend < VOLUME_MIN_SPEND
  );
}

/** The metric a creative ranks on, given the objective and its type. */
export function primaryValueFor(
  cr: RotationCreative,
  objective: ObjectiveType
): { value: number | null; bench: keyof CreativeBenches } {
  if (objective === "awareness") {
    if (cr.type === "static") {
      return { value: cr.engagement_rate, bench: "engagement_rate" };
    }
    return { value: cr.completion_rate, bench: "completion_rate" };
  }
  return { value: cr.cpa, bench: "cpa" };
}

/* ── Per-creative reason copy ─────────────────────────────────────────
   The verdict sets the framing; each creative's OWN signals fill it in, so
   no two cards read identically (the reason used to be one static string
   per verdict, which made every same-verdict card identical). */

const cap = (s: string): string =>
  s ? s.charAt(0).toUpperCase() + s.slice(1) : s;

function readWord(
  value: number | null | undefined,
  bench: QuartileBench | null | undefined
): string {
  const r = quartileRead(value, bench);
  return r ? ` (${r.word.toLowerCase()})` : "";
}

/** A concrete clause from THIS creative's strongest reported signal: the
 *  primary KPI when present, else the best secondary metric, else raw
 *  delivery. Guarantees each card's reason carries its own numbers. */
function signalClause(
  cr: RotationCreative,
  objective: ObjectiveType,
  benches: CreativeBenches
): string {
  const { value, bench } = primaryValueFor(cr, objective);
  if (value != null) {
    const label =
      objective === "awareness"
        ? cr.type === "static"
          ? "engagement"
          : "completion"
        : "cost per result";
    const fmt = objective === "awareness" ? formatRate : formatMoney;
    return `${label} ${fmt(value)}${readWord(value, benches[bench])}`;
  }
  if (cr.ctr != null)
    return `${formatRate(cr.ctr)} CTR${readWord(cr.ctr, benches.ctr)}`;
  if (cr.completion_rate != null)
    return `${formatRate(cr.completion_rate)} completion${readWord(
      cr.completion_rate,
      benches.completion_rate
    )}`;
  if (cr.engagement_rate != null)
    return `${formatRate(cr.engagement_rate)} engagement${readWord(
      cr.engagement_rate,
      benches.engagement_rate
    )}`;
  return `${formatNumberCompact(cr.impressions)} impressions, ${formatMoney(
    cr.spend
  )} spent`;
}

/** The fatigue driver in this creative's own numbers — frequency and/or a
 *  falling CTR trend. At least one is set whenever isFatigued is true. */
function fatigueDriver(cr: RotationCreative): string {
  const freqTrend = cr.trend?.frequency ?? [];
  const latestFreq = freqTrend.length
    ? freqTrend[freqTrend.length - 1]
    : cr.frequency;
  const ctrTrend = (cr.trend?.ctr ?? []).filter((v) => v != null && v > 0);
  const drop =
    ctrTrend.length >= FATIGUE_MIN_POINTS && ctrTrend[0] > 0
      ? (ctrTrend[0] - ctrTrend[ctrTrend.length - 1]) / ctrTrend[0]
      : null;
  const bits: string[] = [];
  if (latestFreq != null && latestFreq > FATIGUE_FREQUENCY)
    bits.push(`frequency is up to ${formatTimes(latestFreq)}`);
  if (drop != null && drop >= FATIGUE_CTR_DROP)
    bits.push(
      `click-through has slipped ${Math.round(drop * 100)}% across the flight`
    );
  return bits.join(" and ") || "the audience has seen it";
}

export function judgeCreative(
  cr: RotationCreative,
  objective: ObjectiveType,
  benches: CreativeBenches
): JudgedCreative {
  const { value, bench } = primaryValueFor(cr, objective);
  const read = quartileRead(value, benches[bench]);

  /* #19: an unresolved name is not graded. The card rebuilds the full
     reason where spend/impressions are formatted (dimensions-only vs
     generic); the neutral placeholder here keeps a sentence if the card
     ever renders j.reason directly. */
  if (isUnresolvedVariant(cr.variant)) {
    return {
      creative: cr,
      verdict: "UNRESOLVED",
      tone: "var(--text-muted)",
      reason:
        "This creative has not been matched to a name yet, so it is not " +
        "graded. Give it a name to fold it into the rotation read.",
      fatigued: false,
      primaryValue: value,
      primaryRead: read,
    };
  }

  if (underVolumeGuard(cr)) {
    return {
      creative: cr,
      verdict: "EARLY",
      tone: "var(--text-faint)",
      reason: `Only ${formatNumberCompact(cr.impressions)} impressions and ${formatMoney(
        cr.spend
      )} spent so far — under the volume guard. A verdict lands once it has real delivery behind it.`,
      fatigued: false,
      primaryValue: value,
      primaryRead: read,
    };
  }

  if (isFatigued(cr)) {
    return {
      creative: cr,
      verdict: "REFRESH",
      tone: "var(--warn)",
      reason: `Fatigued: ${fatigueDriver(cr)}. Rotate a fresh cut in${
        objective !== "awareness" ? " before the cost climbs" : ""
      }.`,
      fatigued: true,
      primaryValue: value,
      primaryRead: read,
    };
  }

  if (read && read.rank === 3) {
    return {
      creative: cr,
      verdict: "SCALE",
      tone: "var(--ok)",
      reason: `${cap(
        signalClause(cr, objective, benches)
      )} against PB history, with volume behind it and no fatigue. Send the next dollar here.`,
      fatigued: false,
      primaryValue: value,
      primaryRead: read,
    };
  }

  return {
    creative: cr,
    verdict: "HOLD",
    tone: "var(--text-muted)",
    reason: `${cap(
      signalClause(cr, objective, benches)
    )} — steady without breaking away. Keep it in rotation while the leader carries the weight.`,
    fatigued: false,
    primaryValue: value,
    primaryRead: read,
  };
}

/**
 * Rank for display: best primary KPI first, EARLY creatives last,
 * unreported primaries after reported ones; ties break on spend.
 * Awareness ranks on rates (higher better); conversion/mixed on CPA
 * (lower better).
 */
export function rankCreatives(
  judged: JudgedCreative[],
  objective: ObjectiveType
): JudgedCreative[] {
  const lowerIsBetter = objective !== "awareness";
  return [...judged].sort((a, b) => {
    const aEarly = a.verdict === "EARLY" ? 1 : 0;
    const bEarly = b.verdict === "EARLY" ? 1 : 0;
    if (aEarly !== bEarly) return aEarly - bEarly;
    const aNull = a.primaryValue == null ? 1 : 0;
    const bNull = b.primaryValue == null ? 1 : 0;
    if (aNull !== bNull) return aNull - bNull;
    if (a.primaryValue != null && b.primaryValue != null) {
      const diff = lowerIsBetter
        ? a.primaryValue - b.primaryValue
        : b.primaryValue - a.primaryValue;
      if (diff !== 0) return diff;
    }
    return b.creative.spend - a.creative.spend;
  });
}

/* ── The call: template-generated plain-language verdict ─────────── */

export interface CreativeCall {
  headline: string;
  body: string;
}

/**
 * "Scale one. Swap one." — the page's opening line, generated from the
 * verdicts. Input should already be ranked (rankCreatives).
 */
export function buildCreativeCall(
  judgedAll: JudgedCreative[],
  objective: ObjectiveType
): CreativeCall {
  /* #19: unresolved creatives carry no verdict and never appear in the
     counts/lists — same exclusion EARLY gets, applied one level up so
     every clause below is blind to them. */
  const judged = judgedAll.filter((j) => j.verdict !== "UNRESOLVED");
  if (judged.length === 0) {
    return {
      headline: "No rotation yet.",
      body: "Ad-level creative data has not arrived for this campaign.",
    };
  }

  const early = judged.filter((j) => j.verdict === "EARLY");
  if (early.length === judged.length) {
    return {
      headline: "Too early to call.",
      body:
        "Every creative is still under the volume guard. Verdicts arrive " +
        "once each has real delivery behind it.",
    };
  }

  const scale = judged.filter((j) => j.verdict === "SCALE");
  const refresh = judged.filter((j) => j.verdict === "REFRESH");
  const hold = judged.filter((j) => j.verdict === "HOLD");
  /* Variant names are raw ad names until someone sets aliases, and a
     12-creative rotation read as a run-on sentence of full system names.
     Strip the project-code prefix, and above two names per group, switch
     to counts: the cards below carry the specifics. */
  const name = (j: JudgedCreative) =>
    j.creative.variant.replace(/^\d{4,6}\s*[-·:]?\s*/, "").trim() ||
    j.creative.variant;
  const list = (js: JudgedCreative[]) =>
    js.length > 2
      ? `${js.length} creatives`
      : js.map(name).join(" and ");
  const bestCostAmong = (js: JudgedCreative[]): string | null => {
    const vals = js.map((j) => j.primaryValue).filter((v): v is number => v != null);
    if (vals.length === 0) return null;
    return formatMoney(Math.min(...vals));
  };
  const creditPhrase = (winners: JudgedCreative[]): string => {
    const cost = objective !== "awareness" ? bestCostAmong(winners) : null;
    const subj =
      winners.length > 1
        ? `${list(winners)} are earning their keep`
        : `${list(winners)} is earning its keep`;
    return cost ? `${cap(subj)}, delivering results from ${cost}` : cap(subj);
  };

  if (judged.length === 1) {
    const j = judged[0];
    if (j.verdict === "SCALE") {
      return {
        headline: "One creative. It works.",
        body: `${name(j)} is the whole rotation and it is earning top-quartile results. The risk is fatigue, not performance: get a second cut ready.`,
      };
    }
    if (j.verdict === "REFRESH") {
      return {
        headline: "One creative. It's tired.",
        body: `${name(j)} is the whole rotation and the audience has seen it. There is nothing to rotate to: a replacement is the move.`,
      };
    }
    return {
      headline: "One creative. Holding.",
      body: `${name(j)} is the whole rotation, earning its keep without breaking away. A challenger cut would give this campaign something to test.`,
    };
  }

  /* #23: assemble the call from independent clauses so the holds can lead
     when there's no scale verdict. Every count/placeholder and the exact
     prior wording is preserved — only the order changes. */
  const scaleClause =
    scale.length > 0
      ? `${list(scale)} ${scale.length > 1 ? "earn" : "earns"} the next dollar.`
      : null;
  const refreshClause =
    refresh.length > 0
      ? refresh.length > 1
        ? `${list(refresh)} are fatigued and getting pricier by the sync: swap them.`
        : `${list(refresh)} is fatigued and getting pricier by the sync: swap it.`
      : null;
  const holdClause =
    hold.length > 0
      ? `${list(hold)} ${hold.length > 1 ? "hold" : "holds"}.`
      : null;
  const earlyClause =
    early.length > 0
      ? `${list(early)} ${early.length > 1 ? "are" : "is"} too new to judge.`
      : null;
  const join = (cs: (string | null)[]) => cs.filter(Boolean).join(" ");

  // scale + refresh: winner already leads — UNCHANGED
  if (scale.length > 0 && refresh.length > 0)
    return {
      headline: "Scale one. Swap one.",
      body: join([scaleClause, refreshClause, holdClause, earlyClause]),
    };
  // scale only — UNCHANGED
  if (scale.length > 0)
    return {
      headline: "Feed the leader.",
      body: join([scaleClause, holdClause, earlyClause]),
    };
  // #23: no scale. Credit the holds first, then the swap (kept).
  if (refresh.length > 0) {
    if (hold.length > 0)
      return {
        headline: "The steady ones are holding. Refresh the rest.",
        body: join([`${creditPhrase(hold)}.`, refreshClause, earlyClause]),
      };
    return {
      headline: "Time to refresh.",
      body: join([refreshClause, earlyClause]),
    };
  }
  // hold only
  return {
    headline: "Hold the rotation.",
    body:
      join([hold.length > 0 ? `${creditPhrase(hold)}.` : null, earlyClause]) ||
      "Nothing is fatigued and nothing has broken away. Keep the split as is.",
  };
}

/* ── Rotation imbalance: the winner is underfed ──────────────────── */

/** Top creative's spend share must be at least this fraction of the
 *  rotation median before we stay quiet. */
const IMBALANCE_RATIO = 0.75;
/** And the absolute gap must be worth acting on (5 share points). */
const IMBALANCE_MIN_GAP = 0.05;

/**
 * When the top-ranked creative's spend share sits materially below the
 * rotation median, the rotation is underfeeding its strongest creative.
 * Returns callout copy, or null when the split looks fine.
 */
export function rotationImbalance(
  judged: JudgedCreative[],
  objective: ObjectiveType,
  kpiLabel: string
): string | null {
  const ranked = rankCreatives(judged, objective).filter(
    (j) =>
      j.verdict !== "EARLY" &&
      j.verdict !== "UNRESOLVED" &&
      j.primaryValue != null
  );
  if (ranked.length < 2) return null;
  const top = ranked[0];

  const shares = judged
    .filter((j) => j.verdict !== "UNRESOLVED")
    .map((j) => j.creative.spend_share)
    .filter((s) => s != null)
    .sort((a, b) => a - b);
  if (shares.length < 2) return null;
  const mid = Math.floor(shares.length / 2);
  const median =
    shares.length % 2 === 0 ? (shares[mid - 1] + shares[mid]) / 2 : shares[mid];

  const share = top.creative.spend_share;
  if (share < median * IMBALANCE_RATIO && median - share >= IMBALANCE_MIN_GAP) {
    return (
      `${top.creative.variant} wins on ${kpiLabel.toLowerCase()} but takes ` +
      `only ${Math.round(share * 100)}% of rotation spend. The strongest ` +
      `creative is being underfed: rebalance toward it.`
    );
  }
  return null;
}

/* ── KPI lenses: both resonance matrices read through one ────────── */

export type LensId =
  | "cpa"
  | "hook"
  | "completion"
  | "engagement"
  | "ctr"
  | "cpm";

export interface Lens {
  id: LensId;
  label: string;
  explain: string;
  /** The objective's primary KPI lens (rendered with the ✱ mark). */
  primary?: boolean;
  /** CPM only makes sense where rooms have prices — shown on the
   *  creative × platform matrix only. */
  platformMatrixOnly?: boolean;
}

export function lensesFor(objective: ObjectiveType): Lens[] {
  if (objective === "awareness") {
    return [
      {
        id: "completion",
        label: "COMPLETION",
        explain:
          "Share of video starts watched to the end: this campaign's primary KPI.",
        primary: true,
      },
      {
        id: "hook",
        label: "HOOK RATE",
        explain: "Share of impressions held for 3 seconds or more (the hook).",
      },
      {
        id: "engagement",
        label: "ENGAGEMENT",
        explain: "Reactions, shares and saves per impression.",
      },
      {
        id: "ctr",
        label: "CLICKS",
        explain:
          "Shown for completeness: clicks are not a KPI on an awareness flight.",
      },
      {
        id: "cpm",
        label: "CPM",
        explain: "Cost per thousand impressions: rooms have prices.",
        platformMatrixOnly: true,
      },
    ];
  }
  return [
    {
      id: "cpa",
      label: "RESULT COST",
      explain:
        "Platform-attributed cost per result: this campaign's primary KPI.",
      primary: true,
    },
    {
      id: "hook",
      label: "HOOK RATE",
      explain: "Share of impressions held for 3 seconds or more (the hook).",
    },
    {
      id: "completion",
      label: "COMPLETION",
      explain: "Share of video starts watched to the end.",
    },
    {
      id: "ctr",
      label: "CLICKS",
      explain: "Click-through. A click is not the same click everywhere.",
    },
    {
      id: "cpm",
      label: "CPM",
      explain: "Cost per thousand impressions: rooms have prices.",
      platformMatrixOnly: true,
    },
  ];
}

/* ── Resolving one matrix cell under a lens ──────────────────────── */

/** The metric fields both matrix cell shapes share. `cpm` is optional
 *  because the audience matrix doesn't carry it (no CPM lens there).
 *  `broken` is a forward slot: the contract doesn't flag broken
 *  tracking per-cell yet, but the display state exists. */
export interface LensCellInput {
  spend: number;
  hook_rate: number | null;
  completion_rate: number | null;
  engagement_rate: number | null;
  ctr: number | null;
  conversions: number;
  cpa: number | null;
  cpm?: number | null;
  broken?: boolean;
}

export interface ResolvedCell {
  kind: "value" | "na" | "broken" | "empty";
  text?: string;
  read?: QuartileRead | null;
  tag?: string;
}

export function resolveCell(
  cell: LensCellInput | null | undefined,
  lens: LensId,
  benches: CreativeBenches,
  /** Label for honest gaps on attention metrics (e.g. "STATIC · N/A"). */
  naTag = "NOT REPORTED"
): ResolvedCell {
  if (!cell) return { kind: "empty" };
  if (cell.broken) return { kind: "broken" };

  if (lens === "cpa") {
    if (cell.cpa == null) {
      return {
        kind: "na",
        tag: cell.spend > 0 && cell.conversions === 0 ? "NO RESULTS" : "NO DATA",
      };
    }
    return {
      kind: "value",
      text: formatMoney(cell.cpa),
      read: quartileRead(cell.cpa, benches.cpa),
    };
  }
  if (lens === "cpm") {
    if (cell.cpm == null) return { kind: "na", tag: "NO DATA" };
    return {
      kind: "value",
      text: formatMoney(cell.cpm),
      read: quartileRead(cell.cpm, benches.cpm),
    };
  }
  if (lens === "hook" || lens === "completion") {
    const v = lens === "hook" ? cell.hook_rate : cell.completion_rate;
    if (v == null) return { kind: "na", tag: naTag };
    return {
      kind: "value",
      text: formatRate(v),
      read: quartileRead(
        v,
        lens === "hook" ? benches.hook_rate : benches.completion_rate
      ),
    };
  }
  if (lens === "engagement") {
    if (cell.engagement_rate == null) return { kind: "na", tag: naTag };
    return {
      kind: "value",
      text: formatRate(cell.engagement_rate),
      read: quartileRead(cell.engagement_rate, benches.engagement_rate),
    };
  }
  if (cell.ctr == null) return { kind: "na" };
  return {
    kind: "value",
    text: formatRate(cell.ctr),
    read: quartileRead(cell.ctr, benches.ctr),
  };
}

/* ── Formatting (rates arrive as fractions, money as dollars) ────── */

export function formatRate(v: number | null | undefined): string {
  if (v == null) return "—";
  const pct = v * 100;
  return pct >= 10 ? `${pct.toFixed(0)}%` : `${pct.toFixed(2)}%`;
}

export function formatMoney(v: number | null | undefined): string {
  if (v == null) return "—";
  return `$${v.toFixed(2)}`;
}

export function formatTimes(v: number | null | undefined): string {
  if (v == null) return "—";
  return `${v.toFixed(1)}×`;
}
