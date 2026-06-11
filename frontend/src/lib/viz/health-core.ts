/**
 * Signal instruments — shared health model.
 *
 * Every campaign (or media plan line) becomes a "signal": a body on the
 * Orbit stage and, when sound is on, a voice in the mix. Ported from the
 * Claude Design Signal Lab export (app/viz/health-core.js).
 */
import type { PacingLine, Project } from "@/lib/api";
import { platformLabel } from "@/lib/utils";

export type Severity = "ok" | "watch" | "critical" | "nodata";

export interface SignalItem {
  id: string;
  code: string;
  label: string;
  sub?: string;
  pct: number | null;
  sev: Severity;
  /** Signed deviation from 100%, clamped to ±1 (±40pts = full scale). */
  dev: number;
  /** 0..1 relative size (budget share of the largest item). */
  weight: number;
  budget: number;
  spend: number;
  days: number | null;
  kind: "campaign" | "line";
}

/* Theme-aware palette — severity colours + canvas inks swap together.
   Light values come from the brand's light tokens (status colours darkened
   for paper so they hold the same contrast hierarchy). */
const PALETTES = {
  dark: {
    ok: "#CAFF28",
    watch: "#F0B73E",
    critical: "#E5594E",
    nodata: "#5B8FD6",
    ink: [255, 255, 255] as const,
    bg: [26, 24, 24] as const,
  },
  light: {
    ok: "#6f8c00",
    watch: "#B97D08",
    critical: "#C4392E",
    nodata: "#3F6FB0",
    ink: [34, 32, 32] as const,
    bg: [245, 244, 241] as const,
  },
};

type ThemeMode = keyof typeof PALETTES;

let themeMode: ThemeMode = "light";

export const COLORS: Record<Severity, string> = {
  ok: PALETTES.light.ok,
  watch: PALETTES.light.watch,
  critical: PALETTES.light.critical,
  nodata: PALETTES.light.nodata,
};

export function setTheme(m: string) {
  themeMode = (m in PALETTES ? m : "light") as ThemeMode;
  const p = PALETTES[themeMode];
  COLORS.ok = p.ok;
  COLORS.watch = p.watch;
  COLORS.critical = p.critical;
  COLORS.nodata = p.nodata;
}

/** Sync the canvas palette with the nearest data-theme context. */
export function syncThemeFromElement(el: Element | null) {
  setTheme(el?.closest('[data-theme="dark"]') ? "dark" : "light");
}

/* Canvas inks: foreground + background at any alpha, in the current theme.
   Dark hairlines on paper read lighter than white ones on black at the
   same alpha — boost low alphas in light mode so structure stays visible. */
export function ink(a: number): string {
  const c = PALETTES[themeMode].ink;
  const al = themeMode === "light" ? Math.min(1, a * 1.65) : a;
  return `rgba(${c[0]},${c[1]},${c[2]},${al})`;
}

export function bg(a: number): string {
  const c = PALETTES[themeMode].bg;
  return `rgba(${c[0]},${c[1]},${c[2]},${a})`;
}

export function themeBoost(): number {
  return themeMode === "light" ? 1.4 : 1;
}

export const STATUS_WORD: Record<Severity, string> = {
  ok: "On pace",
  watch: "Drifting",
  critical: "Off pace",
  nodata: "No signal",
};

export const SOUND_DESC: Record<Severity, string> = {
  ok: "Running steady — a low, even hum. On plan and holding.",
  watch: "Drifting — you can hear it wavering against the rest.",
  critical: "Off pace — an unsteady wobble with a rattle underneath.",
  nodata: "Silent. A ping every few seconds until data lands.",
};

export function classify(pct: number | null | undefined): {
  sev: Severity;
  dev: number;
} {
  if (pct == null || isNaN(pct)) return { sev: "nodata", dev: 0 };
  const dev = Math.max(-1, Math.min(1, (pct - 100) / 40));
  const a = Math.abs(pct - 100);
  const sev: Severity = a < 8 ? "ok" : a < 22 ? "watch" : "critical";
  return { sev, dev };
}

/* ── item builders ─────────────────────────────────────────────────── */

/** Active campaigns orbit the platform core (Flightdeck SignalsPanel). */
export function campaignSignalItems(projects: Project[]): SignalItem[] {
  const maxBudget = Math.max(1, ...projects.map((p) => p.net_budget ?? 0));
  return projects.map((p) => {
    const { sev, dev } = classify(p.pacing_percentage);
    return {
      id: p.project_code,
      code: p.project_code,
      label: p.project_name,
      sub: p.client_name ?? undefined,
      pct: p.pacing_percentage,
      sev,
      dev,
      weight: (p.net_budget ?? 0) / maxBudget,
      budget: p.net_budget ?? 0,
      spend: p.total_spend ?? 0,
      days: p.days_remaining,
      kind: "campaign",
    };
  });
}

/** A campaign's line items orbit the campaign core (Pacing Signal). */
export function lineSignalItems(lines: PacingLine[]): SignalItem[] {
  const maxBudget = Math.max(1, ...lines.map((l) => l.planned_budget ?? 0));
  return lines.map((l) => {
    const { sev, dev } = classify(l.pacing_percentage);
    return {
      id: l.line_id,
      code: l.line_code ?? l.line_id.split("-").pop() ?? l.line_id,
      label: l.audience_name ?? platformLabel(l.platform_id),
      sub: `${platformLabel(l.platform_id)} · ${l.channel_category}`,
      pct: l.pacing_percentage,
      sev,
      dev,
      weight: (l.planned_budget ?? 0) / maxBudget,
      budget: l.planned_budget ?? 0,
      spend: l.actual_spend_to_date ?? 0,
      days: l.remaining_days,
      kind: "line",
    };
  });
}

export function fmtMoneyShort(n: number | null | undefined): string {
  if (n == null) return "—";
  if (n >= 1000) return "$" + Math.round(n / 1000) + "K";
  return "$" + Math.round(n);
}
