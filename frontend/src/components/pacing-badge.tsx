import { cn, pacingStatus, pacingBg, pacingColor, formatPercent } from "@/lib/utils";

interface PacingBadgeProps {
  percentage: number | null | undefined;
  size?: "sm" | "md";
}

export function PacingBadge({ percentage, size = "md" }: PacingBadgeProps) {
  const status = pacingStatus(percentage);
  const labels: Record<string, string> = {
    "critical-over": "Critical Over",
    "warning-over": "Over",
    "on-track": "On Track",
    "warning-under": "Under",
    "critical-under": "Critical Under",
    unknown: "No Data",
  };

  return (
    <span
      className={cn(
        "inline-flex items-center gap-1 rounded-full border font-medium",
        pacingBg(status),
        pacingColor(status),
        size === "sm" ? "px-2 py-0.5 text-[10px]" : "px-2.5 py-1 text-xs"
      )}
    >
      <span
        className={cn(
          "inline-block rounded-full",
          status === "on-track" ? "bg-emerald-400" : status.includes("over") || status.includes("under") ? (status.includes("critical") ? "bg-red-400" : "bg-amber-400") : "bg-slate-500",
          size === "sm" ? "h-1.5 w-1.5" : "h-2 w-2"
        )}
      />
      {percentage != null ? formatPercent(percentage) : labels[status]}
    </span>
  );
}
