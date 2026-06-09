/**
 * Flight projection + verdict engine — ported from the ADA Flightdeck
 * prototype (app/flight.jsx).
 *
 * First principles: the only question a media buyer has is "is the money
 * landing, and if not, what do I do?" Everything here answers that
 * literally, derived client-side from the existing /api/projects payload —
 * no backend changes.
 */
import type { Project } from "@/lib/api";
import {
  formatCurrency,
  formatPercent,
  pacingStatus,
  type PacingStatus,
} from "@/lib/utils";

export interface FlightMath {
  flightTotal: number;
  elapsed: number;
  remaining: number;
  budget: number;
  spend: number;
  ended: boolean;
  noData: boolean;
  dailyRate: number;
  dailyPlanned: number;
  dailyNeeded: number;
  plannedToDate: number;
  budgetRemaining: number;
  projectedFinal: number | null;
  deltaAmount: number | null;
  deltaPct: number | null;
  daysToExhaust: number;
  /** Positive = budget runs out that many days BEFORE flight end. */
  exhaustEarlyDays: number;
  status: PacingStatus;
  spentPct: number;
  plannedPct: number;
  projPct: number | null;
}

function dayDiff(a: string, b: string): number {
  return Math.round(
    (new Date(b + "T00:00:00").getTime() - new Date(a + "T00:00:00").getTime()) /
      864e5
  );
}

export function computeFlight(p: Project): FlightMath {
  const flightTotal = Math.max(1, dayDiff(p.start_date, p.end_date) + 1);
  const remaining = Math.max(0, p.days_remaining ?? 0);
  const elapsed = Math.max(1, Math.min(flightTotal, flightTotal - remaining));
  const budget = p.net_budget ?? 0;
  const spend = p.total_spend ?? 0;
  const ended = p.status !== "active";
  const noData = !ended && (spend === 0 || p.pacing_percentage == null);

  const dailyRate = spend / elapsed; // recent average burn
  const dailyPlanned = budget / flightTotal; // even-plan daily target
  const plannedToDate = p.pacing_percentage
    ? spend / (p.pacing_percentage / 100)
    : budget * (elapsed / flightTotal);
  const budgetRemaining = budget - spend;

  // Projection: if the flight keeps hitting this % of plan, it lands here.
  // (Consistent with the pacing % everyone reads — the plan curve may be
  //  back-loaded, so extrapolating the past daily average would mislead.)
  const projectedFinal = noData
    ? null
    : ended
      ? spend
      : budget * ((p.pacing_percentage ?? 100) / 100);
  const deltaAmount = projectedFinal == null ? null : projectedFinal - budget; // +over / −under
  const deltaPct =
    projectedFinal == null || budget <= 0
      ? null
      : ((deltaAmount as number) / budget) * 100;

  // Forward burn implied by the projection, for the "runs out early" read.
  const forwardDaily =
    projectedFinal != null && remaining > 0
      ? (projectedFinal - spend) / remaining
      : dailyRate;
  const daysToExhaust =
    forwardDaily > 0 ? budgetRemaining / forwardDaily : Infinity;
  const exhaustEarlyDays = isFinite(daysToExhaust)
    ? Math.round(remaining - daysToExhaust)
    : 0;
  const dailyNeeded =
    remaining > 0 ? Math.max(0, budgetRemaining) / remaining : 0;

  const status = pacingStatus(p.pacing_percentage);

  return {
    flightTotal,
    elapsed,
    remaining,
    budget,
    spend,
    ended,
    noData,
    dailyRate,
    dailyPlanned,
    dailyNeeded,
    plannedToDate,
    budgetRemaining,
    projectedFinal,
    deltaAmount,
    deltaPct,
    daysToExhaust,
    exhaustEarlyDays,
    status,
    spentPct: budget > 0 ? (spend / budget) * 100 : 0,
    plannedPct: budget > 0 ? (plannedToDate / budget) * 100 : 0,
    projPct:
      projectedFinal == null || budget <= 0
        ? null
        : (projectedFinal / budget) * 100,
  };
}

/* ── Verdict — plain-language read, Point Blank voice ───────────── */

export type VerdictIcon = "gauge" | "trending-up" | "activity";

export interface Verdict {
  /** Display word — Folsom, ALL CAPS. */
  word: string;
  /** CSS colour token, e.g. "var(--ok)". */
  tone: string;
  headline: string;
  detail: string;
  action: { label: string; icon: VerdictIcon } | null;
}

export function verdict(p: Project, f: FlightMath): Verdict {
  const dn = formatCurrency(Math.round(f.dailyNeeded));
  const absDelta = formatCurrency(Math.abs(Math.round(f.deltaAmount ?? 0)));
  const absPct = Math.abs(f.deltaPct ?? 0).toFixed(0);
  const d = Math.abs(f.exhaustEarlyDays);

  if (f.ended) {
    const finalOver = f.spend > f.budget;
    return {
      word: "LANDED",
      tone: "var(--done)",
      headline: finalOver ? "Closed slightly over budget." : "Closed on budget.",
      detail: `Final spend ${formatCurrency(f.spend)} of ${formatCurrency(
        f.budget
      )} — ${formatPercent(p.pacing_percentage)} of plan.`,
      action: null,
    };
  }
  if (f.noData) {
    return {
      word: "DARK",
      tone: "var(--info)",
      headline: "No data yet.",
      detail: `Flight opens ${p.start_date}. Pacing lights up once the platforms start spending.`,
      action: null,
    };
  }
  switch (f.status) {
    case "on-track":
      return {
        word: "ON PACE",
        tone: "var(--ok)",
        headline: "The money's landing on plan.",
        detail: `Tracking ${formatPercent(
          p.pacing_percentage
        )} of plan — projected to finish on budget, on schedule.`,
        action: null,
      };
    case "warning-over":
      return {
        word: "RUNNING HOT",
        tone: "var(--warn)",
        headline: "Spending ahead of plan.",
        detail: `At this rate the budget's gone ~${d} day${
          d === 1 ? "" : "s"
        } before the flight ends. Trim to ${dn}/day to land clean.`,
        action: { label: "Throttle daily caps", icon: "gauge" },
      };
    case "critical-over":
      return {
        word: "BURNING DOWN",
        tone: "var(--danger)",
        headline: "Too fast — the budget won't last the flight.",
        detail: `Empties ~${d} day${
          d === 1 ? "" : "s"
        } early on current pace. Throttle to ${dn}/day now or it goes dark before the finish.`,
        action: { label: "Throttle now", icon: "gauge" },
      };
    case "warning-under":
      return {
        word: "LAGGING",
        tone: "var(--warn)",
        headline: "Money isn't going out fast enough.",
        detail: `~${absDelta} (${absPct}%) projected to sit unspent. Lift daily spend to ${dn}/day to use the full budget.`,
        action: { label: "Lift daily spend", icon: "trending-up" },
      };
    case "critical-under":
      return {
        word: "STALLED",
        tone: "var(--danger)",
        headline: "Badly underspending.",
        detail: `Over ${absPct}% of budget at risk of going to waste. Get the lines live and raise caps this week.`,
        action: { label: "Diagnose lines", icon: "activity" },
      };
    default:
      return {
        word: "UNKNOWN",
        tone: "var(--text-faint)",
        headline: "Pacing unavailable.",
        detail: "No pacing read for this flight.",
        action: null,
      };
  }
}

/** Status dot colour for a project row / palette entry. */
export function flightDotColor(p: Project, f: FlightMath): string {
  if (f.ended) return "var(--done)";
  if (f.noData) return "var(--info)";
  if (f.status === "on-track") return "var(--ok)";
  if (f.status.includes("critical")) return "var(--danger)";
  return "var(--warn)";
}
