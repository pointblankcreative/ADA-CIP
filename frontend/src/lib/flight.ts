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
  /** Full contracted budget incl. direct buys (for total-budget display). */
  totalBudget: number;
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

/** Today as an ISO YYYY-MM-DD string, local-midnight anchored. */
function todayIso(): string {
  const d = new Date();
  d.setHours(0, 0, 0, 0);
  return d.toISOString().slice(0, 10);
}

export function computeFlight(p: Project): FlightMath {
  const flightTotal = Math.max(1, dayDiff(p.start_date, p.end_date) + 1);
  // Derive remaining from end_date relative to today rather than trusting a
  // raw days_remaining the API may leave stale on a finished flight (Finding
  // #2: a landed campaign surfaced "176d remaining"). When end_date is in the
  // past, the flight has zero days left no matter what days_remaining says;
  // when it disagrees with the date, prefer the smaller (never invent runway).
  const daysToEnd = dayDiff(todayIso(), p.end_date); // <0 once end_date is past
  const rawRemaining = Math.max(0, p.days_remaining ?? 0);
  const remaining =
    daysToEnd <= 0 ? 0 : Math.min(rawRemaining, daysToEnd);
  const elapsed = Math.max(1, Math.min(flightTotal, flightTotal - remaining));
  const totalBudget = p.net_budget ?? 0;
  // Pacing runs against the TRACKABLE (self-serve) budget: direct buys carry
  // budget but never accrue trackable spend, so including them understates
  // pacing and would tell the buyer to spend into budget that was never theirs
  // to pace (the "lift to $X/day to use the full budget" wrong action).
  // `totalBudget` keeps the full contract value for display. No-op for the
  // common all-self-serve campaign where direct_budget is 0/absent.
  const directBudget = p.direct_budget ?? 0;
  const budget = Math.max(0, totalBudget - directBudget);
  const spend = p.total_spend ?? 0;
  const ended = p.status !== "active";
  // Finding #4: a live flight whose pacing collapses to exactly 0 (no planned
  // baseline, e.g. spend present but unattributed) is "awaiting data", not a
  // confident 0%; treat it as no-data instead of a red STALLED verdict.
  const noData =
    !ended &&
    (spend === 0 || p.pacing_percentage == null || p.pacing_percentage === 0);

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
    totalBudget,
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

/**
 * Early-flight ramp grace: in the first week (and first quarter) of a flight,
 * under-pacing is expected — delivery ramps and platform data lags 1-2 days, so
 * the even-pace plan-to-date naturally runs ahead of actual. Treat under-pacing
 * in that window as an informational "ramping" state rather than escalating to
 * LAGGING / STALLED, which are mature-flight verdicts. Over-pacing is left alone
 * (burning the budget too fast early is still worth flagging), and zero spend is
 * handled separately as DARK / no-data.
 */
function earlyFlightRamping(f: FlightMath): boolean {
  return (
    !f.ended &&
    !f.noData &&
    f.elapsed <= 7 &&
    f.elapsed / f.flightTotal < 0.25 &&
    (f.status === "warning-under" || f.status === "critical-under")
  );
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
      detail:
        `Final spend ${formatCurrency(f.spend)} of ${formatCurrency(
          f.budget
        )} — ${formatPercent(p.pacing_percentage)} of plan.` +
        (f.totalBudget > f.budget
          ? ` Self-serve budget only; ${formatCurrency(
              f.totalBudget
            )} total incl. direct buys.`
          : ""),
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
  if (earlyFlightRamping(f)) {
    return {
      word: "RAMPING",
      tone: "var(--info)",
      headline: "Early in the flight — still ramping.",
      detail: `Day ${f.elapsed} of ${f.flightTotal}. Tracking ${formatPercent(
        p.pacing_percentage
      )} of the even-pace plan; early under-pacing is normal while delivery builds and platform data lags a day or two. Worth a look if it's still soft by the end of week one.`,
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
  if (earlyFlightRamping(f)) return "var(--info)";
  if (f.status === "on-track") return "var(--ok)";
  if (f.status.includes("critical")) return "var(--danger)";
  return "var(--warn)";
}
