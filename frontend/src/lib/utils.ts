import { clsx, type ClassValue } from "clsx";
import { twMerge } from "tailwind-merge";

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs));
}

export function formatCurrency(value: number | null | undefined, currency = "CAD"): string {
  if (value == null) return "$0";
  return new Intl.NumberFormat("en-CA", {
    style: "currency",
    currency,
    minimumFractionDigits: 0,
    maximumFractionDigits: 0,
  }).format(value);
}

export function formatNumber(value: number | null | undefined): string {
  if (value == null) return "0";
  return new Intl.NumberFormat("en-CA").format(value);
}

export function formatPercent(value: number | null | undefined): string {
  if (value == null) return "—";
  return `${value.toFixed(1)}%`;
}

/**
 * Magnitude-aware currency tick formatter for chart axes.
 *
 * Under $1k:   "$200", "$400"
 * $1k–$1M:    "$1k", "$25k"
 * $1M+:       "$1.2M"
 *
 * Use this on any Recharts `<XAxis>` / `<YAxis>` whose values represent
 * spend in dollars. Pairing every spend axis with this helper avoids the
 * "four $0k labels" bug (AI-020) for projects whose daily spend never
 * crosses $1,000.
 */
export function formatCurrencyTick(value: number | null | undefined): string {
  const n = value ?? 0;
  const abs = Math.abs(n);
  if (abs >= 1_000_000) return `$${(n / 1_000_000).toFixed(1)}M`;
  if (abs >= 1_000) return `$${Math.round(n / 1_000)}k`;
  return `$${Math.round(n)}`;
}

export type PacingStatus =
  | "critical-over"
  | "warning-over"
  | "on-track"
  | "warning-under"
  | "critical-under"
  | "unknown";

export function pacingStatus(percentage: number | null | undefined): PacingStatus {
  if (percentage == null) return "unknown";
  if (percentage > 130) return "critical-over";
  if (percentage > 115) return "warning-over";
  if (percentage < 70) return "critical-under";
  if (percentage < 85) return "warning-under";
  return "on-track";
}

export function pacingColor(status: PacingStatus): string {
  const colors: Record<PacingStatus, string> = {
    "critical-over": "text-red-400",
    "warning-over": "text-amber-400",
    "on-track": "text-emerald-400",
    "warning-under": "text-amber-400",
    "critical-under": "text-red-400",
    unknown: "text-slate-500",
  };
  return colors[status];
}

export function pacingBg(status: PacingStatus): string {
  const colors: Record<PacingStatus, string> = {
    "critical-over": "bg-red-500/20 border-red-500/30",
    "warning-over": "bg-amber-500/20 border-amber-500/30",
    "on-track": "bg-emerald-500/20 border-emerald-500/30",
    "warning-under": "bg-amber-500/20 border-amber-500/30",
    "critical-under": "bg-red-500/20 border-red-500/30",
    unknown: "bg-slate-500/20 border-slate-500/30",
  };
  return colors[status];
}

export function pacingBarColor(status: PacingStatus): string {
  const colors: Record<PacingStatus, string> = {
    "critical-over": "bg-red-500",
    "warning-over": "bg-amber-500",
    "on-track": "bg-emerald-500",
    "warning-under": "bg-amber-500",
    "critical-under": "bg-red-500",
    unknown: "bg-slate-600",
  };
  return colors[status];
}

export function severityColor(severity: string): string {
  if (severity === "critical") return "text-red-400 bg-red-500/15 border-red-500/30";
  if (severity === "warning") return "text-amber-400 bg-amber-500/15 border-amber-500/30";
  return "text-blue-400 bg-blue-500/15 border-blue-500/30";
}

export function platformLabel(id: string): string {
  const labels: Record<string, string> = {
    meta: "Meta",
    google_ads: "Google Ads",
    stackadapt: "StackAdapt",
    linkedin: "LinkedIn",
    tiktok: "TikTok",
    snapchat: "Snapchat",
    perion: "Perion/DOOH",
    reddit: "Reddit",
    pinterest: "Pinterest",
  };
  return labels[id] ?? id;
}

export function platformIcon(id: string): string {
  const icons: Record<string, string> = {
    meta: "M",
    google_ads: "G",
    stackadapt: "S",
    linkedin: "L",
    tiktok: "T",
    snapchat: "Sc",
    perion: "P",
    reddit: "R",
    pinterest: "Pi",
  };
  return icons[id] ?? id.charAt(0).toUpperCase();
}

export function daysUntil(dateStr: string | null): number {
  if (!dateStr) return 0;
  const target = new Date(dateStr + "T00:00:00");
  const now = new Date();
  now.setHours(0, 0, 0, 0);
  return Math.ceil((target.getTime() - now.getTime()) / (1000 * 60 * 60 * 24));
}

/**
 * Returns true if `platformId` is in the backend-declared support list for a
 * given metric. `supportList` comes from
 * PerformanceResponse.metric_platforms[metric] and carries platform LABELS
 * (e.g. "Meta", "Google Ads", "StackAdapt") while `platformId` is the
 * snake_case ID ("meta", "google_ads", "stackadapt"). To match across both
 * forms we normalize each side by lowercasing AND stripping every
 * non-alphanumeric character, so "google_ads" and "Google Ads" both collapse
 * to "googleads" and compare equal. Plain case-insensitive equality is not
 * enough — it only worked for single-word names.
 *
 * Used to disambiguate backend `0` from backend `null` when a platform
 * simply doesn't report a metric (e.g. Google Ads / StackAdapt don't report
 * engagements; their rows return engagement_rate=0.0, not null).
 */
export function platformSupportsMetric(
  platformId: string | null | undefined,
  supportList: string[] | undefined,
): boolean {
  if (!platformId || !supportList || supportList.length === 0) return false;
  const normalize = (s: string) => s.toLowerCase().replace(/[^a-z0-9]/g, "");
  const needle = normalize(platformId);
  return supportList.some((p) => normalize(p) === needle);
}

/**
 * Render an engagement rate cell for a single-platform row (AdSetRow, AdRow).
 * Shows "—" when the platform doesn't support engagements, or when the value
 * is null. Otherwise returns the formatted percentage.
 */
export function renderEngagementRate(
  engagementRate: number | null | undefined,
  platformId: string | null | undefined,
  supportList: string[] | undefined,
): string {
  if (!platformSupportsMetric(platformId, supportList)) return "—";
  if (engagementRate == null) return "—";
  return formatPercent(engagementRate * 100);
}

/**
 * Render an engagement rate cell for a multi-platform row
 * (CreativeVariantRow.platforms is a string[] because one variant may ship to
 * multiple platforms). A row gets a real number if at least one of its
 * platforms supports the metric — the value remains meaningful because it's a
 * weighted average across that supporting subset.
 */
export function renderEngagementRateMulti(
  engagementRate: number | null | undefined,
  platforms: string[] | null | undefined,
  supportList: string[] | undefined,
): string {
  if (!platforms || platforms.length === 0) return "—";
  const supported = platforms.some((p) => platformSupportsMetric(p, supportList));
  if (!supported) return "—";
  if (engagementRate == null) return "—";
  return formatPercent(engagementRate * 100);
}

export type FlightDayVariant = "combined" | "short";

export interface FlightDayInput {
  /** Current day-of-flight (1-indexed). From DiagnosticOutput.flight_day. */
  flightDay?: number | null;
  /** Total scheduled flight length in days. Derive from end - start if not on hand. */
  flightTotalDays?: number | null;
  /** Days remaining from today. From Project.days_remaining. Negative = ended. */
  daysRemaining?: number | null;
}

/**
 * Canonical day-count phrasing for ADA-CIP surfaces.
 *
 * "combined" → "Day 14 of 30 · 16 days remaining" (project header, diagnostics card)
 * "short"    → "16 days remaining" / "Ends today" / "Ended" (dashboard cards, tight space)
 *
 * Falls back gracefully when only one half of the inputs is available. See
 * AI-049 for context — three surfaces were rendering three different
 * phrasings; this centralizes the rules.
 */
export function formatFlightDay(
  input: FlightDayInput,
  variant: FlightDayVariant = "combined",
): string {
  const { flightDay, flightTotalDays, daysRemaining } = input;

  // Retro-view case: looking at a flight whose end date is in the past.
  // flightDay continues counting forward from start, so it can exceed
  // flightTotalDays. In that case the combined variant keeps the "Day N of
  // M" anchor (so the user still knows where in the schedule they are) and
  // swaps the "X days remaining" half for "ended". Short variant stays
  // terse — the dashboard cards just need to signal the flight is over.
  if (
    flightDay != null &&
    flightTotalDays != null &&
    flightDay > flightTotalDays
  ) {
    if (variant === "short") return "Ended";
    return `Day ${flightDay} of ${flightTotalDays} · ended`;
  }

  // Terminal: campaign ended (negative days remaining).
  if (daysRemaining != null && daysRemaining < 0) {
    return variant === "short" ? "Ended" : "Campaign ended";
  }
  // Ends today (zero days remaining).
  if (daysRemaining === 0) {
    if (variant === "short") return "Ends today";
    if (flightDay != null && flightTotalDays != null) {
      return `Day ${flightDay} of ${flightTotalDays} · ends today`;
    }
    return "Ends today";
  }

  // Short variant: just the countdown, used on the dashboard cards.
  if (variant === "short") {
    if (daysRemaining != null && daysRemaining > 0) {
      return `${daysRemaining} days remaining`;
    }
    // Fallback when daysRemaining isn't available — derive from flight halves.
    if (flightDay != null && flightTotalDays != null) {
      const left = Math.max(0, flightTotalDays - flightDay);
      return `${left} days remaining`;
    }
    return "";
  }

  // Combined variant: prefer "Day N of M · X days remaining".
  if (flightDay != null && flightTotalDays != null) {
    const left =
      daysRemaining != null && daysRemaining > 0
        ? daysRemaining
        : Math.max(0, flightTotalDays - flightDay);
    return `Day ${flightDay} of ${flightTotalDays} · ${left} days remaining`;
  }
  // Combined but missing flight halves — fall back to short-form text.
  if (daysRemaining != null && daysRemaining > 0) {
    return `${daysRemaining} days remaining`;
  }
  return "";
}
