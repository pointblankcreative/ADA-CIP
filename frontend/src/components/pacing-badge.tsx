import { cn, pacingStatus, pacingBg, pacingColor, formatPercent } from "@/lib/utils";

interface PacingBadgeProps {
  percentage: number | null | undefined;
  totalSpend?: number;
  lineStatus?: "not_started" | "pending" | "active" | "completed";
  size?: "sm" | "md";
}

export function PacingBadge({ percentage, totalSpend = 0, lineStatus, size = "md" }: PacingBadgeProps) {
  const status = pacingStatus(percentage);
  // AI-001: the "spend without a percentage" fallback is a LINE-level
  // heuristic — a brand-new flight that's started spending before the daily
  // pacing engine has computed a percentage for it. It must NOT fire for
  // project-level callers (home cards, project header) that don't pass
  // `lineStatus`, otherwise an actively-spending project whose backend
  // pacing_percentage is null shows "Pending" instead of the honest "No Data".
  const isPending = lineStatus === "pending"
    || lineStatus === "not_started"
    || (lineStatus != null && percentage == null && totalSpend > 0);
  const isCompleted = lineStatus === "completed";

  const labels: Record<string, string> = {
    "critical-over": "Critical Over",
    "warning-over": "Over",
    "on-track": "On Track",
    "warning-under": "Under",
    "critical-under": "Critical Under",
    unknown: "No Data",
  };

  const label = isCompleted
    ? "Completed"
    : isPending
      ? lineStatus === "not_started"
        ? "Not Started"
        : "Pending"
      : percentage != null
        ? formatPercent(percentage)
        : labels[status];

  const dotColor = isCompleted
    ? "bg-slate-400"
    : isPending
      ? "bg-blue-400"
      : status === "on-track"
        ? "bg-emerald-400"
        : status.includes("over") || status.includes("under")
          ? status.includes("critical") ? "bg-red-400" : "bg-amber-400"
          : "bg-slate-500";

  const badgeBg = isCompleted
    ? "bg-slate-500/20 border-slate-500/30"
    : isPending ? "bg-blue-500/20 border-blue-500/30" : pacingBg(status);
  const badgeColor = isCompleted
    ? "text-slate-400"
    : isPending ? "text-blue-400" : pacingColor(status);

  return (
    <span
      className={cn(
        "inline-flex items-center gap-1 rounded-full border font-medium",
        badgeBg,
        badgeColor,
        size === "sm" ? "px-2 py-0.5 text-[10px]" : "px-2.5 py-1 text-xs"
      )}
    >
      <span
        className={cn(
          "inline-block rounded-full",
          dotColor,
          size === "sm" ? "h-1.5 w-1.5" : "h-2 w-2"
        )}
      />
      {label}
    </span>
  );
}
