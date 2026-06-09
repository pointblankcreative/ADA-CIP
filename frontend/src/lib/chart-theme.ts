/**
 * Shared Recharts theming — Point Blank design system.
 *
 * Every chart consumes these tokens instead of per-chart hex so the same
 * chart renders correctly in light and dark contexts. Series colours map
 * to the restrained status palette (clarity over brand, per the _ds
 * dashboard guidance) with chartreuse reserved for spend — the hero metric.
 */
import type { ObjectiveType } from "@/lib/api";

export const CHART_TOOLTIP_STYLE: React.CSSProperties = {
  background: "var(--surface-raised)",
  border: "1.5px solid var(--border)",
  borderRadius: "4px",
  fontSize: "0.75rem",
  color: "var(--text-primary)",
};

export const CHART_TICK = {
  fontSize: 10.5,
  fill: "var(--text-faint)",
  fontFamily: "var(--font-mono)",
} as const;

export const CHART_GRID = "var(--border-soft)";

/** Series palette — semantic, theme-aware. */
export const SERIES = {
  spend: "var(--accent-ink)",
  spendFill: "var(--pb-chartreuse)",
  reach: "var(--info)",
  frequency: "var(--warn)",
  vcr: "var(--info)",
  cpa: "var(--ok)",
  cpaEffective: "var(--text-faint)",
  conversions: "var(--ok)",
  conversionRate: "var(--info)",
  sessions: "var(--info)",
} as const;

export const OBJECTIVE_BADGE: Record<
  ObjectiveType,
  { label: string; cls: string }
> = {
  awareness: {
    label: "Awareness",
    cls: "bg-tint-info border-tint-info text-info",
  },
  conversion: {
    label: "Conversion",
    cls: "bg-tint-ok border-tint-ok text-ok",
  },
  mixed: {
    label: "Mixed",
    cls: "bg-tint-accent border-tint-accent text-accent-ink",
  },
};

/** Standard table header cell classes (mono, tracked, uppercase). */
export const TH_CLS =
  "px-5 py-3 font-mono text-[10px] font-medium uppercase tracking-[0.1em] text-fg-faint";
