/**
 * ADA metric glossary, plain-language definitions for the acronyms and
 * signal names that pepper the dashboards. Pure data, server-safe (no
 * "use client"): the Glossary component (components/glossary.tsx) reads
 * this on the client, but the dictionary itself has no React or DOM
 * dependency, so it can be imported anywhere.
 *
 * Resolution is alias-aware: a signal rendered by its engine id (e.g.
 * "D4") resolves to the same entry as its slug ("incremental_reach"),
 * which lets callers wrap signal names by id without knowing the slug.
 */

export interface MetricDefinition {
  key: string;
  label: string;
  definition: string;
  how?: string;
  unit?: string;
  aliases?: string[];
}

const DICTIONARY: Record<string, MetricDefinition> = {
  /* ---- WIRED entries ---- */
  cpm: {
    key: "cpm",
    label: "CPM",
    definition:
      "What you pay for a thousand ad impressions, the base price of being seen.",
    how: "Spend divided by impressions, times 1,000.",
    unit: "$ per 1,000 impressions",
  },
  cpc: {
    key: "cpc",
    label: "CPC",
    definition: "What you pay each time someone clicks the ad.",
    how: "Spend divided by link clicks.",
    unit: "$ per click",
  },
  cpa: {
    key: "cpa",
    label: "CPA",
    definition:
      "What it costs to get one conversion (a lead, signup, or sale).",
    how: "Spend divided by counted conversions for the flight.",
    unit: "$ per conversion",
  },
  cpcv: {
    key: "cpcv",
    label: "CPCV",
    definition:
      "What you pay for each qualifying video view (15 seconds, or the whole clip if it is shorter, on Meta; a completed view elsewhere).",
    how: "Total spend divided by qualifying video views.",
    unit: "$ per qualifying view",
  },
  incremental_reach: {
    key: "incremental_reach",
    label: "Incremental Reach",
    definition:
      "Whether a platform's share of the reach matches its share of the budget. A platform taking a big slice of spend but adding little new reach is the flag.",
    how: "It compares each platform's reach against its share of spend. It does not remove people who saw ads on more than one platform, so the same person can be counted on each.",
    aliases: ["D4"],
  },
  focused_view: {
    key: "focused_view",
    label: "Focused View",
    definition: "Whether people actually stop and watch, or scroll straight past.",
    how: "ADA's own roll-up across platforms of how many video impressions hold attention long enough to count as a real view (around 15 seconds on Meta, 3 seconds elsewhere).",
    aliases: ["A4"],
  },
  sets_rank: {
    key: "sets_rank",
    label: "Sets rank",
    definition:
      "The yardstick this campaign is judged by. ADA ranks the creative by this one, not by whatever looks best.",
    how: "It follows the campaign's goal: for awareness, how many people finish the video; for conversion, what each conversion costs. Look for the asterisk that marks it.",
  },
  clicks_all: {
    key: "clicks_all",
    label: "clicks_all",
    definition:
      "Every click a platform counts, not just the clicks that reach the destination site.",
    how: "A raw platform field. ADA uses link clicks (the ones that reach the landing page) for click-through rate, so the two can differ.",
  },
  self_serve_budget: {
    key: "self_serve_budget",
    label: "Self-serve budget",
    definition:
      "The part of the budget that runs on self-serve ad platforms and reports spend back to ADA. Pacing is measured against this.",
    how: "The contracted budget minus any direct buys booked off-platform.",
  },
  direct_buys: {
    key: "direct_buys",
    label: "Direct buys",
    definition:
      "Placements booked directly off-platform, so no live spend feed reaches ADA. They count toward the total budget but not toward tracked spend or pacing.",
  },
  sessions_arrival: {
    key: "sessions_arrival",
    label: "Clicks that arrive",
    definition:
      "A single click can lead to more than one session (a return visit, the session resuming, or the visitor coming back on another device), so sessions running above 100% of clicks is normal, not a tracking error. Clicks are counted differently on each platform.",
    how: "GA4 sessions divided by paid clicks across the flight.",
    unit: "% of paid clicks",
  },

  /* ---- PRE-SEEDED entries (not wired yet; siblings register/override) ---- */
  pillar_distribution: {
    key: "pillar_distribution",
    label: "Distribution",
    definition:
      "Whether the ads are reaching enough of the right people, often enough, spread evenly across platforms.",
    how: "Built from the signals under it that have enough data to report (reach, frequency, spread across platforms), with the more important ones counting for more; the rest sit out.",
  },
  pillar_attention: {
    key: "pillar_attention",
    label: "Attention",
    definition:
      "Whether people who see the ads actually watch them, rather than scrolling past.",
    how: "Built from the signals under it that have enough data to report (completion, viewability, focused view), with the more important ones counting for more; the rest sit out.",
  },
  pillar_resonance: {
    key: "pillar_resonance",
    label: "Resonance",
    definition:
      "Whether the ads land, measured by deliberate engagement and what people do after they click.",
    how: "Built from the signals under it that have enough data to report (engagement quality, landing-page depth), with the more important ones counting for more; the rest sit out.",
  },
  pillar_acquisition: {
    key: "pillar_acquisition",
    label: "Acquisition",
    definition:
      "Whether conversions are coming in at the right cost and the right pace.",
    how: "Built from the signals under it that have enough data to report (cost per conversion, conversion pace), with the more important ones counting for more; the rest sit out.",
  },
  pillar_funnel: {
    key: "pillar_funnel",
    label: "Funnel",
    definition:
      "Whether the path from click to completed action holds up (click, page load, scroll, form).",
    how: "Built from the signals under it that have enough data to report (click-through, page load, scroll, form completion), with the more important ones counting for more; the rest sit out.",
  },
  pillar_quality: {
    key: "pillar_quality",
    label: "Quality",
    definition:
      "Whether the data feeding a signal is solid enough to trust the read.",
  },
  engine_persuasion: {
    key: "engine_persuasion",
    label: "Persuasion",
    definition:
      "ADA's read on awareness and consideration lines, did the campaign reach, hold, and move people.",
    how: "Rolls up its pillars (Distribution, Attention, Resonance), each built from its own signals.",
  },
  engine_conversion: {
    key: "engine_conversion",
    label: "Conversion",
    definition:
      "ADA's read on direct-response lines, did the campaign turn spend into leads or sales efficiently.",
    how: "Rolls up its pillars (Acquisition, Funnel), each built from its own signals.",
  },
  band_strong: {
    key: "band_strong",
    label: "Strong (70 and up)",
    definition:
      "The campaign is firing on most signals. ADA treats 70 and up as success, not as a near-miss of 100.",
  },
  band_watch: {
    key: "band_watch",
    label: "Watch (40 to 69)",
    definition:
      "Some signals are slipping. Worth a look this week, not an emergency.",
  },
  band_action: {
    key: "band_action",
    label: "At risk (under 40)",
    definition:
      "One or more signals need action now to keep the flight on track.",
  },
  band_no_signal: {
    key: "band_no_signal",
    label: "No signal",
    definition:
      "Not enough delivery yet to score this. An empty read means it is too early to call, not that something is wrong.",
  },
  score_scale: {
    key: "score_scale",
    label: "Health score (0 to 100)",
    definition:
      "A 0 to 100 read on whether the campaign is working, separate from pacing. Pacing asks whether spend is on schedule, the health score asks whether the campaign is performing. Crossing 70 is the goal, 100 needs every signal perfect at once, which live campaigns rarely hit.",
    how: "The overall score blends the pillars by weight, and is eased early in a flight when data is still thin.",
  },
  hook_rate: {
    key: "hook_rate",
    label: "Hook rate",
    definition:
      "How many people are still watching three seconds in, a read on whether the opening earns attention.",
    how: "Three-second video views divided by impressions.",
    unit: "% of impressions",
  },
  guard_insufficient_data: {
    key: "guard_insufficient_data",
    label: "Not enough data",
    definition:
      "ADA is holding this signal back until there is enough volume to call it fairly, so it does not alarm on noise.",
  },
  guard_below_threshold: {
    key: "guard_below_threshold",
    label: "Below reporting threshold",
    definition:
      "The numbers are too small to read reliably yet. The signal stays quiet rather than guess.",
  },
  guard_no_creative: {
    key: "guard_no_creative",
    label: "No creative to judge",
    definition:
      "This signal needs creative or video data that is not present for this campaign.",
  },
  guard_not_applicable: {
    key: "guard_not_applicable",
    label: "Not applicable here",
    definition:
      "This signal does not apply to this campaign's objective or line types, so ADA skips it.",
  },
  guard_no_landing_page: {
    key: "guard_no_landing_page",
    label: "No landing page data",
    definition:
      "This signal needs site or landing-page analytics that are not connected for this campaign.",
  },
};

/* Alias index: lowercased alias -> canonical key. Rebuilt whenever the
   dictionary changes (module load + registerTerms). */
let aliasIndex: Record<string, string> = {};

function rebuildAliasIndex(): void {
  const next: Record<string, string> = {};
  for (const entry of Object.values(DICTIONARY)) {
    if (!entry.aliases) continue;
    for (const alias of entry.aliases) {
      next[alias.toLowerCase()] = entry.key;
    }
  }
  aliasIndex = next;
}

rebuildAliasIndex();

/**
 * Resolve a term by key, then by alias (case-insensitive), else undefined.
 */
export function lookupTerm(termKey: string): MetricDefinition | undefined {
  const direct = DICTIONARY[termKey];
  if (direct) return direct;
  const aliased = aliasIndex[termKey.toLowerCase()];
  if (aliased) return DICTIONARY[aliased];
  return undefined;
}

/**
 * Register or override dictionary entries by key. Siblings #3 (score
 * legend), #8 (hook rate), #9 (pillar defs), #13 (guard reasons) register
 * or override here; their entries are pre-seeded above so this is a no-op
 * until those siblings ship their own copy.
 */
export function registerTerms(entries: MetricDefinition[]): void {
  for (const entry of entries) {
    DICTIONARY[entry.key] = entry;
  }
  rebuildAliasIndex();
}

/** All dictionary entries, convenience for introspection / tests. */
export function allTerms(): MetricDefinition[] {
  return Object.values(DICTIONARY);
}
