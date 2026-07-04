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
import { platformLabel } from "@/lib/utils";

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

/* ── Guard reasons — plain-language "why this isn't reporting" copy ─────
   Signals that fail a guard come back with a machine token in
   `guard_reason` (see backend/services/diagnostics/shared/guards.py and the
   per-pillar signal modules). The raw token (e.g. "min_impressions_1000",
   "single_platform") leaks pipeline jargon to the user, so guardCopy()
   maps each to a short, honest sentence in one of three categories:

     pending  — the data type isn't built into the platform yet
     waiting  — the data just hasn't arrived in enough volume yet
     na       — the check doesn't apply to this campaign's shape/objective

   Never frame a quiet check as "broken / failed / error / pipeline" — a
   guard withholding a score is the system being careful, not failing. */

export type GuardCategory = "pending" | "waiting" | "na";

export const GUARD_CATEGORY_LABEL: Record<GuardCategory, string> = {
  pending: "Coming soon",
  waiting: "Waiting on data",
  na: "Not applicable",
};

/* Keys are the exact backend tokens. Templated guards that suffix a
   threshold (e.g. `min_impressions_1000`) are stored under their
   normalized prefix (`min_impressions`); guardCopy() strips the trailing
   `_<number>` before the lookup. */
export const GUARD_REASONS: Record<
  string,
  { text: string; category: GuardCategory }
> = {
  // ── Pending: data type not yet available in the transformation ──
  no_audio_data_in_transformation: {
    text: "Audio completion isn't available yet.",
    category: "pending",
  },
  no_earned_data_in_transformation: {
    text: "Earned and viral reach (free reshares) isn't available yet.",
    category: "pending",
  },

  // ── Not applicable: the check doesn't fit this campaign ──
  single_platform: {
    text: "Runs on a single platform, so there is nothing to compare across platforms.",
    category: "na",
  },
  no_target_cpa: {
    text: "No target cost per result is set, so there is nothing to measure against.",
    category: "na",
  },
  no_key_events_configured: {
    text: "No key events are configured to count as conversions.",
    category: "na",
  },
  arch_b_only: {
    text: "Only applies to campaigns that send clicks to a landing page.",
    category: "na",
  },
  no_arch_a_lp_reporting: {
    text: "This campaign's landing page doesn't report the views this check needs.",
    category: "na",
  },
  no_reporting_platforms: {
    text: "No platform on this campaign reports the data this check needs.",
    category: "na",
  },
  no_lp_view_reporting: {
    text: "Landing-page views aren't reported for this campaign.",
    category: "na",
  },
  no_ffs_data: {
    text: "No form-friction inputs are set, so there is nothing to benchmark against.",
    category: "na",
  },
  no_media_plan: {
    text: "No media plan is attached, so there is nothing to measure against.",
    category: "na",
  },
  no_planned_impressions: {
    text: "The media plan has no planned impressions to measure against.",
    category: "na",
  },

  // ── Waiting: not enough has come in yet ──
  no_reach_data: { text: "Reach hasn't come in yet.", category: "waiting" },
  no_frequency_data: {
    text: "Frequency hasn't come in yet.",
    category: "waiting",
  },
  no_engagement_data: {
    text: "Engagement hasn't come in yet.",
    category: "waiting",
  },
  no_attention_data: {
    text: "Attention data hasn't come in yet.",
    category: "waiting",
  },
  no_quartile_data: {
    text: "Video completion data hasn't come in yet.",
    category: "waiting",
  },
  no_landing_page_views: {
    text: "Landing-page views haven't come in yet.",
    category: "waiting",
  },
  no_clickable_impressions: {
    text: "No clickable impressions have come in yet.",
    category: "waiting",
  },
  no_starts: { text: "No video starts have come in yet.", category: "waiting" },
  below_min_starts: {
    text: "Not enough video starts yet to read this reliably.",
    category: "waiting",
  },
  no_impressions: {
    text: "No impressions have come in yet.",
    category: "waiting",
  },
  no_clicks: { text: "No clicks have come in yet.", category: "waiting" },
  no_in_platform_leads: {
    text: "No in-platform leads have come in yet.",
    category: "waiting",
  },
  no_conversions: {
    text: "No conversions have come in yet.",
    category: "waiting",
  },
  no_daily_data: {
    text: "Not enough day-by-day data yet to read a trend.",
    category: "waiting",
  },
  zero_conversions: {
    text: "No conversions have come in yet.",
    category: "waiting",
  },
  zero_attention_baseline: {
    text: "Not enough attention data yet to set a baseline.",
    category: "waiting",
  },
  zero_expected_volume: {
    text: "Not enough planned volume yet to measure against.",
    category: "waiting",
  },
  zero_planned_reach: {
    text: "Not enough planned reach yet to measure against.",
    category: "waiting",
  },
  zero_window_impressions: {
    text: "Not enough impressions in this window yet to read it.",
    category: "waiting",
  },
  zero_mean: {
    text: "Not enough data yet to read a stable average.",
    category: "waiting",
  },
  insufficient_data: {
    text: "Not enough data yet to read this reliably.",
    category: "waiting",
  },
  insufficient_days: {
    text: "Not enough days into the flight yet to read this.",
    category: "waiting",
  },
  insufficient_days_after_volume_filter: {
    text: "Not enough qualifying days yet to read a trend.",
    category: "waiting",
  },
  insufficient_daily_data: {
    text: "Not enough day-by-day data yet to read a trend.",
    category: "waiting",
  },
  insufficient_daily_series: {
    text: "Not enough day-by-day data yet to read a trend.",
    category: "waiting",
  },
  insufficient_cpa_days: {
    text: "Not enough days with conversions yet to read cost stability.",
    category: "waiting",
  },
  insufficient_per_platform_impressions: {
    text: "Not enough impressions per platform yet to compare them.",
    category: "waiting",
  },

  // ── Waiting: templated minimum thresholds (normalized prefixes) ──
  min_days: {
    text: "Not enough days into the flight yet to read this.",
    category: "waiting",
  },
  min_impressions: {
    text: "Not enough impressions yet to read this reliably.",
    category: "waiting",
  },
  min_spend: {
    text: "Not enough spend yet to read cost metrics.",
    category: "waiting",
  },
  min_clicks: {
    text: "Not enough clicks yet to read this reliably.",
    category: "waiting",
  },
  min_clicks_for_lp: {
    text: "Not enough clicks yet to read landing-page load.",
    category: "waiting",
  },
  min_clicks_for_f1: {
    text: "Not enough clicks yet to read click-through reliably.",
    category: "waiting",
  },
  min_ga4_sessions: {
    text: "Not enough site sessions yet to read the funnel.",
    category: "waiting",
  },
  min_conversions: {
    text: "Not enough conversions yet to read a stable cost.",
    category: "waiting",
  },
  min_video_starts: {
    text: "Not enough video starts yet to read completion.",
    category: "waiting",
  },
  min_engagements: {
    text: "Not enough engagements yet to read this reliably.",
    category: "waiting",
  },
  min_form_starts: {
    text: "Not enough form starts yet to read completion.",
    category: "waiting",
  },
  min_form_submits: {
    text: "Not enough form submissions yet to read this.",
    category: "waiting",
  },
  min_viewability_measured: {
    text: "Not enough measured impressions yet to read viewability.",
    category: "waiting",
  },
};

/* Strip a trailing `_<int|float>` threshold so templated tokens like
   `min_impressions_1000` resolve to the `min_impressions` family entry. */
const GUARD_FALLBACK: { text: string; category: GuardCategory } = {
  text: "Not enough data to report this yet.",
  category: "waiting",
};

export function guardCopy(
  reason: string | null
): { text: string; category: GuardCategory } {
  if (reason == null) return GUARD_FALLBACK;
  const exact = GUARD_REASONS[reason];
  if (exact) return exact;
  const normalized = reason.replace(/_-?\d+(\.\d+)?$/, "");
  const fam = GUARD_REASONS[normalized];
  if (fam) return fam;
  return GUARD_FALLBACK;
}

/* ── Curated action copy — shown on ACT NOW cards ───────────────────────
   Keyed by signal ID, matched to the PRODUCTION signal definitions (the
   `name=` arguments at the SignalResult construction sites — same source
   of truth as lib/alert-labels.ts SIGNAL_NAMES). Deliberately
   generic-but-useful imperatives; tune freely, this file is the single
   source. A future engine version can supply per-evaluation actions and
   this map becomes the fallback. Unknown IDs simply render no chip. */

export type SignalOwner = "Media" | "Trading" | "Creative" | "Web" | "Client";

/** Display label for an owner tag. Sentence case, deliberately NOT the
 *  mono-uppercase styling the platform/pillar chips use, so an owner can
 *  never be misread as a platform. */
export const OWNER_LABELS: Record<SignalOwner, string> = {
  Media: "Media team",
  Trading: "Trading desk",
  Creative: "Creative team",
  Web: "Web team",
  Client: "Client",
};

export interface SignalAction {
  /** The suggested move itself, advisory, never commanding. */
  action: string;
  /** Team that typically owns this move (routing, not a command). */
  owner: SignalOwner;
  /** Optional caveat where the move may be constrained by an outside
   *  party (contracts, IO minimums, DSP or deal availability, budget
   *  caps, creative in flight, landing-page ownership). */
  hedge?: string;
  /** Optional "take action in <platform>" pointer. Left unset for now:
   *  the worst platform is per-evaluation, not constant per signal. */
  platform?: string;
}

export const SIGNAL_ACTIONS: Record<string, SignalAction> = {
  // Persuasion · Distribution
  D1: { action: "Rebalance budget toward efficient-reach lines", owner: "Media", hedge: "where contract or IO minimums allow" }, // Reach Attainment
  D2: { action: "Consolidate audiences to build frequency", owner: "Media" }, // Frequency Adequacy
  D3: { action: "Rebalance platform budgets to even out delivery", owner: "Media" }, // Frequency Distribution
  D4: { action: "Trim overlapping audiences and push new reach", owner: "Trading", hedge: "subject to DSP and deal availability" }, // Incremental Reach
  D5: { action: "Confirm platform delivery and smooth daily pacing", owner: "Media" }, // Delivery Cadence
  // Persuasion · Attention
  A1: { action: "Test shorter cuts or stronger openings", owner: "Creative" }, // Video Completion Quality
  A3: { action: "Shift spend to higher-viewability placements", owner: "Trading" }, // Viewability
  A4: { action: "Refresh the creative rotation", owner: "Creative" }, // Focused View
  A5: { action: "Rotate in fresh creative this week", owner: "Creative", hedge: "if new creative is ready or in review" }, // Creative Fatigue
  // Persuasion · Resonance
  R1: { action: "Review creative tone against engagement quality", owner: "Creative" }, // Engagement Quality Ratio
  R3: { action: "Tighten the landing page path", owner: "Web" }, // Landing Page Depth
  // Conversion · Acquisition
  C1: { action: "Shift budget to the cheapest converting lines", owner: "Media", hedge: "within any per-line budget caps" }, // CPA vs Target
  C2: { action: "Raise caps on converting lines", owner: "Media", hedge: "if budget headroom and pacing allow" }, // Volume Trajectory
  C3: { action: "Refresh audiences before CPA creep compounds", owner: "Media" }, // CPA Trend
  // Conversion · Funnel
  F1: { action: "Test new hooks and calls to action", owner: "Creative" }, // Click-Through Rate
  F2: { action: "Fix link tags and landing page load", owner: "Web" }, // Landing Page Load Rate
  F3: { action: "Move the form above the fold", owner: "Web", hedge: "if the landing page is ours to change" }, // Scroll & Form Discovery
  F4: { action: "Cut form fields to reduce friction", owner: "Web" }, // Form Completion Rate
  F5: { action: "Strengthen the post-conversion journey", owner: "Client" }, // Post-Conversion Activation
};

/* ── Plain-language explainers — shown when a signal card is expanded ──
   One sentence per signal: what question it answers, in the user's
   words. Pairs with the curated evidence fields below; the full raw
   payload stays available behind the "All the numbers" toggle. */

export const SIGNAL_MEANINGS: Record<string, string> = {
  D1: "Are we reaching as many people as the plan called for by this point in the flight?",
  D2: "Are people seeing the ad often enough to absorb it, without seeing it so often they tune out?",
  D3: "Is delivery spread evenly across platforms, or is one hogging impressions while others starve?",
  D4: "Is each platform adding new people, or are they paying to reach the same audience twice?",
  D5: "Is spend flowing in a steady daily rhythm, or arriving in bursts with dark days?",
  A1: "Once people start the video, how long do they keep watching?",
  A2: "Once people start the audio ad, do they listen through?",
  A3: "Are the ads actually on screen long enough to be seen?",
  A4: "Do people stop and watch, or scroll straight past?",
  A5: "Is the creative wearing out from repetition?",
  R1: "When people engage, is it deliberate (reactions, clicks through) or just passive?",
  R2: "Are people sharing the ads beyond the paid audience?",
  R3: "Do visitors who click through actually engage with the landing page?",
  C1: "What does a conversion cost, versus what it should cost for this form and audience?",
  C2: "Are leads arriving at the pace this budget should produce?",
  C3: "Is the cost per conversion stable, or creeping up as the audience taps out?",
  F1: "Do the ads earn clicks?",
  F2: "Do paid clicks actually make it onto the landing page?",
  F3: "Do visitors get far enough down the page to find the form?",
  F4: "Once people start the form, do they finish it?",
  F5: "After converting, do leads take another meaningful step?",
};

/* ── Curated evidence fields — the few numbers worth surfacing ─────────
   Per signal, an ordered list of `inputs` keys with human labels and
   formats. Keys missing from a given evaluation simply don't render
   (e.g. F4's two shapes: completion_rate for landing-page forms,
   click_to_lead_rate for in-platform forms). */

export type EvidenceFmt =
  | "num" // integer with thousands separators
  | "pct" // 0–1 ratio as whole percent
  | "pct2" // 0–1 ratio as percent, 2 decimals (CTR-scale)
  | "money"
  | "f1" // 1 decimal place
  | "f2" // 2 decimal places
  | "pctday" // signed percent per day
  | "platform" // platform_id → display label (google_ads → Google Ads)
  | "str";

export interface EvidenceField {
  key: string;
  label: string;
  fmt: EvidenceFmt;
}

export const SIGNAL_EVIDENCE: Record<string, EvidenceField[]> = {
  D1: [
    { key: "actual_reach", label: "People reached", fmt: "num" },
    { key: "pro_rated_reach", label: "Planned by now", fmt: "num" },
    { key: "planned_reach", label: "Planned for full flight", fmt: "num" },
  ],
  D2: [
    { key: "avg_frequency", label: "Avg exposures per person", fmt: "f1" },
    { key: "worst_platform", label: "Platform to watch", fmt: "platform" },
  ],
  D3: [
    { key: "cv", label: "Unevenness (0 = perfectly even)", fmt: "f2" },
  ],
  D4: [
    { key: "worst_platform", label: "Least efficient", fmt: "platform" },
    { key: "best_platform", label: "Most efficient", fmt: "platform" },
    { key: "effective_unique_reach", label: "Est. unique people", fmt: "num" },
  ],
  D5: [
    { key: "worst_platform", label: "Platform to watch", fmt: "platform" },
    { key: "worst_gap_days", label: "Days with zero delivery", fmt: "num" },
  ],
  A1: [
    { key: "weighted_q100_rate", label: "Watch to the end", fmt: "pct" },
    { key: "total_starts", label: "Video starts", fmt: "num" },
    { key: "worst_platform", label: "Platform to watch", fmt: "platform" },
  ],
  A3: [
    { key: "viewability_rate", label: "Ads actually seen", fmt: "pct" },
    { key: "measured_impressions", label: "Impressions measured", fmt: "num" },
  ],
  A4: [
    { key: "weighted_rate", label: "Hold attention", fmt: "pct" },
    { key: "worst_platform", label: "Platform to watch", fmt: "platform" },
  ],
  A5: [
    { key: "daily_change_pct", label: "Attention change per day", fmt: "pctday" },
    { key: "worst_platform", label: "Fading fastest", fmt: "platform" },
  ],
  R1: [
    { key: "quality_ratio", label: "Deliberate share", fmt: "pct" },
    { key: "total_engagement", label: "Total engagements", fmt: "num" },
  ],
  R3: [
    { key: "engaged_session_rate", label: "Engaged sessions", fmt: "pct" },
    { key: "scroll_rate", label: "Visitors who scroll", fmt: "pct" },
    { key: "sessions", label: "Sessions", fmt: "num" },
  ],
  C1: [
    { key: "actual_cpa", label: "Cost per conversion", fmt: "money" },
    { key: "target_cpa", label: "Expected", fmt: "money" },
    { key: "conversions", label: "Conversions", fmt: "num" },
    { key: "spend", label: "Spend", fmt: "money" },
  ],
  C2: [
    { key: "rolling_avg_daily", label: "Leads per day", fmt: "f1" },
    { key: "expected_daily", label: "Expected", fmt: "f1" },
  ],
  C3: [
    { key: "daily_change_pct", label: "CPA change per day", fmt: "pctday" },
    { key: "weighted_mean_cpa", label: "Average CPA this week", fmt: "money" },
  ],
  F1: [
    { key: "actual_ctr", label: "Click-through rate", fmt: "pct2" },
    { key: "benchmark", label: "Typical", fmt: "pct2" },
    { key: "clickable_clicks", label: "Clicks", fmt: "num" },
  ],
  F2: [
    { key: "load_rate", label: "Clicks reaching the page", fmt: "pct" },
    { key: "reporting_landing_page_views", label: "Page views", fmt: "num" },
  ],
  F3: [
    { key: "discovery_rate", label: "Visitors reaching the form", fmt: "pct" },
    { key: "scroll_rate", label: "Visitors who scroll", fmt: "pct" },
  ],
  F4: [
    { key: "completion_rate", label: "Form starters who finish", fmt: "pct" },
    { key: "click_to_lead_rate", label: "Tappers who complete", fmt: "pct" },
    { key: "friction_adjusted_target", label: "Expected", fmt: "pct" },
    { key: "click_to_lead_benchmark", label: "Expected", fmt: "pct" },
    { key: "form_submits", label: "Form submits", fmt: "num" },
  ],
  F5: [
    { key: "activation_rate", label: "Take a second step", fmt: "pct" },
    { key: "denominator", label: "Conversions counted", fmt: "num" },
  ],
};

export function formatEvidence(v: unknown, fmt: EvidenceFmt): string {
  if (v == null) return "—";
  if (fmt === "platform") {
    const s = String(v);
    const label = platformLabel(s);
    // Unknown ids fall through platformLabel unchanged — degrade to
    // Title Case instead of leaking snake_case (mirrors the backend).
    return label === s
      ? s.replace(/_/g, " ").replace(/\b\w/g, (c) => c.toUpperCase())
      : label;
  }
  if (fmt === "str") return String(v).replace(/_/g, " ");
  const n = typeof v === "number" ? v : Number(v);
  if (Number.isNaN(n)) return String(v);
  switch (fmt) {
    case "num":
      return Math.round(n).toLocaleString();
    case "pct":
      return (n * 100).toFixed(n * 100 >= 10 ? 0 : 1) + "%";
    case "pct2":
      return (n * 100).toFixed(2) + "%";
    case "money":
      return "$" + n.toLocaleString(undefined, { maximumFractionDigits: 2 });
    case "f1":
      return n.toFixed(1);
    case "f2":
      return n.toFixed(2);
    case "pctday":
      return (n > 0 ? "+" : "") + n.toFixed(1) + "%/day";
    default:
      return String(v);
  }
}

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
  action: SignalAction | null;
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
