"use client";

/**
 * SyncStatus — "the data refreshes twice daily, you don't need to sit here."
 *
 * The pipeline runs at 2:30 AM and 2:30 PM America/Vancouver (5:30 ET per
 * the repo docs). This renders the last-synced stamp plus a live countdown
 * to the next run so nobody refreshes the page hoping for new numbers.
 * ADA is not real-time (yet) — this makes that legible instead of implicit.
 *
 * The countdown computes wall-clock minutes in America/Vancouver via Intl,
 * so it's correct for viewers in any timezone. (On the two DST changeover
 * nights a 2–3 AM reading can drift by an hour; acceptable for a hint.)
 */
import { useEffect, useState } from "react";
import { RefreshCw } from "lucide-react";
import { cn } from "@/lib/utils";

/** Sync times as minutes-since-midnight, America/Vancouver. */
const SYNC_MINUTES = [150, 870]; // 02:30, 14:30
const SYNC_TOOLTIP =
  "Data refreshes twice daily — 2:30 AM and 2:30 PM Pacific. ADA is not real-time (yet), so there's nothing new to see between syncs.";

function vancouverMinutesNow(): number {
  const parts = new Intl.DateTimeFormat("en-CA", {
    timeZone: "America/Vancouver",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  }).formatToParts(new Date());
  const h = Number(parts.find((p) => p.type === "hour")?.value ?? "0") % 24;
  const m = Number(parts.find((p) => p.type === "minute")?.value ?? "0");
  return h * 60 + m;
}

function minutesUntilNextSync(): number {
  const now = vancouverMinutesNow();
  for (const t of SYNC_MINUTES) {
    if (t > now) return t - now;
  }
  return SYNC_MINUTES[0] + 1440 - now;
}

function formatCountdown(mins: number): string {
  const h = Math.floor(mins / 60);
  const m = mins % 60;
  return h > 0 ? `${h}h ${m}m` : `${m}m`;
}

export function SyncStatus({
  lastUpdated,
  variant = "full",
  className,
}: {
  /** ISO timestamp of the freshest data (e.g. max updated_at). */
  lastUpdated?: string | null;
  /** "full" = synced stamp + countdown; "compact" = countdown only. */
  variant?: "full" | "compact";
  className?: string;
}) {
  // Computed client-side only (mounted guard avoids hydration mismatch).
  const [mins, setMins] = useState<number | null>(null);

  useEffect(() => {
    const tick = () => setMins(minutesUntilNextSync());
    tick();
    const id = setInterval(tick, 30_000);
    return () => clearInterval(id);
  }, []);

  if (mins === null) return null;

  const stamp = lastUpdated
    ? lastUpdated.slice(5, 16).replace("T", " ")
    : null;

  return (
    <span
      title={SYNC_TOOLTIP}
      className={cn(
        "inline-flex cursor-help items-center gap-1.5 whitespace-nowrap font-mono text-[10.5px] uppercase tracking-[0.04em] text-fg-faint",
        className
      )}
    >
      <RefreshCw className="h-3 w-3" />
      {variant === "full" && stamp && (
        <>
          <span>Synced {stamp}</span>
          <span className="opacity-50">·</span>
        </>
      )}
      <span>next sync {formatCountdown(mins)}</span>
    </span>
  );
}
