import { pacingStatus, pacingVar, formatPercent } from "@/lib/utils";
import { StatusPill } from "@/components/ui";

interface PacingBadgeProps {
  percentage: number | null | undefined;
  totalSpend?: number;
  lineStatus?: "not_started" | "pending" | "active" | "completed";
  size?: "sm" | "md";
  /**
   * "auto" (default) — show the percentage when available, fall back to the
   * categorical label only when percentage is null. Matches legacy behaviour
   * for project header, home cards, and the standalone pacing-tab pill.
   * "label" — always show the categorical status label (Over / On Track /
   * Under / etc.), never the percentage. Used by the LINE BREAKDOWN table
   * where a separate column already prints the numeric percentage and the
   * pill would otherwise just repeat it. See AI-013.
   */
  variant?: "auto" | "label";
}

export function PacingBadge({
  percentage,
  totalSpend = 0,
  lineStatus,
  size = "md",
  variant = "auto",
}: PacingBadgeProps) {
  const status = pacingStatus(percentage);
  // AI-001: the "spend without a percentage" fallback is a LINE-level
  // heuristic — a brand-new flight that's started spending before the daily
  // pacing engine has computed a percentage for it. It must NOT fire for
  // project-level callers (home cards, project header) that don't pass
  // `lineStatus`, otherwise an actively-spending project whose backend
  // pacing_percentage is null shows "Pending" instead of the honest "No Data".
  const isPending =
    lineStatus === "pending" ||
    lineStatus === "not_started" ||
    (lineStatus != null && percentage == null && totalSpend > 0);
  const isCompleted = lineStatus === "completed";

  // Status words converge on the Claude Design four-token system
  // (On pace / Drifting / Off pace / No signal); over vs under is kept as a
  // direction qualifier. Keys are unchanged — only the displayed text differs.
  const labels: Record<string, string> = {
    "critical-over": "Off pace · over",
    "warning-over": "Drifting · over",
    "on-track": "On pace",
    "warning-under": "Drifting · under",
    "critical-under": "Off pace · under",
    unknown: "No signal",
  };

  const label = isCompleted
    ? "Completed"
    : isPending
      ? lineStatus === "not_started"
        ? "Not Started"
        : "Pending"
      : variant === "label"
        ? labels[status]
        : percentage != null
          ? formatPercent(percentage)
          : labels[status];

  const color = isCompleted
    ? "var(--done)"
    : isPending
      ? "var(--info)"
      : pacingVar(status);

  return <StatusPill label={label} color={color} size={size} />;
}
