/**
 * BandScale — a health score drawn against its bands, not against 100.
 *
 * The engine's own definition of success is the STRONG band (70+), but a
 * filled progress bar made every score read as distance-from-perfect:
 * an 81 looked like "19 missing" when it's actually a campaign firing on
 * all cylinders. 100 requires every signal perfect at once, which live
 * campaigns essentially never achieve (and sometimes can't — no new
 * creative coming, fixed audience, locked landing page).
 *
 * So: a gauge, not a thermometer. Three quiet band zones, a slightly
 * firmer line where strong begins, and a marker showing where this
 * score sits. Crossing the line is the goal; the right-hand end of the
 * track is just where the scale stops.
 */
import { cn } from "@/lib/utils";

export function BandScale({
  score,
  color,
  className,
}: {
  /** 0–100 score, or null/undefined for no-data (zones render, no marker). */
  score: number | null | undefined;
  /** Marker color — the status color of whatever this scores. */
  color: string;
  className?: string;
}) {
  const pos = score != null ? Math.min(Math.max(score, 0), 100) : null;
  return (
    <div className={cn("relative h-[5px] w-full", className)} aria-hidden="true">
      <div
        className="absolute inset-y-0 left-0 rounded-l-pill"
        style={{
          width: "40%",
          background: "color-mix(in srgb, var(--danger) 10%, transparent)",
        }}
      />
      <div
        className="absolute inset-y-0"
        style={{
          left: "40%",
          width: "30%",
          background: "color-mix(in srgb, var(--warn) 10%, transparent)",
        }}
      />
      <div
        className="absolute inset-y-0 rounded-r-pill"
        style={{
          left: "70%",
          width: "30%",
          background: "color-mix(in srgb, var(--ok) 16%, transparent)",
        }}
      />
      {/* the line that matters: strong starts here */}
      <div
        className="absolute -inset-y-[2px] w-[1.5px]"
        style={{
          left: "70%",
          background: "color-mix(in srgb, var(--ok) 55%, transparent)",
        }}
      />
      {pos != null && (
        <div
          className="absolute -inset-y-[2.5px] w-[3px] rounded-[1px] transition-[left] duration-700 ease-snap"
          style={{ left: `calc(${pos}% - 1.5px)`, background: color }}
        />
      )}
    </div>
  );
}
