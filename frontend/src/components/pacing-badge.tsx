import { cn, pacingStatus, pacingBg, pacingColor, formatPercent } from "@/lib/utils";

interface PacingBadgeProps {
  percentage: number | null | undefined;
  totalSpend?: number;
  size?: "sm" | "md";
}

export function PacingBadge({ percentage, totalSpend = 0, size = "md" }: PacingBadgeProps) {
  const status = pacingStatus(percentage);
  const isPending = percentage == null && totalSpend > 0;

  const labels: Record<string, string> = {
    "critical-over": "Critical Over",
    "warning-over": "Over",
    "on-track": "On Track",
    "warning-under": "Under",
    "critical-under": "Critical Under",
    unknown: "No Data",
  };

  const label = percentage != null
    ? formatPercent(percentage)
    : isPending
      ? "Pacing Pending"
      : labels[status];

  const dotColor = isPending
    ? "bg-blue-400"
    : status === "on-track"
      ? "bg-emerald-400"
      : status.includes("over") || status.includes("under")
        ? status.includes("critical") ? "bg-red-400" : "bg-amber-400"
        : "bg-slate-500";

  const badgeBg = isPending ? "bg-blue-500/20 border-blue-500/30" : pacingBg(status);
  const badgeColor = isPending ? "text-blue-400" : pacingColor(status);

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
