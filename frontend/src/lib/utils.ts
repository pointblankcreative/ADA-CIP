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
