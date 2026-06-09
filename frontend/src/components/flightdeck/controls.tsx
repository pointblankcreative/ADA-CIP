"use client";

/**
 * Flightdeck list controls — segmented Active/Ended switch + sort select.
 * Ported from the prototype's flightdeck.jsx (Hidden segment dropped:
 * board visibility is managed in BigQuery via dismiss/archive flags).
 */
import { cn } from "@/lib/utils";

export interface SegmentOption {
  id: string;
  label: string;
  count: number;
}

export function Segmented({
  value,
  options,
  onChange,
}: {
  value: string;
  options: SegmentOption[];
  onChange: (id: string) => void;
}) {
  return (
    <div className="inline-flex rounded-sm border-2 border-line-soft bg-surface-sunken p-0.5">
      {options.map((o) => {
        const on = o.id === value;
        return (
          <button
            key={o.id}
            onClick={() => onChange(o.id)}
            className={cn(
              "inline-flex items-center gap-[7px] rounded-xs border-none px-[13px] py-1.5 font-mono text-[11.5px] font-bold uppercase tracking-[0.06em] transition-all duration-fast",
              on
                ? "bg-accent text-on-accent"
                : "bg-transparent text-fg-muted hover:text-fg"
            )}
          >
            {o.label}
            <span
              className={cn(
                "text-[10.5px]",
                on ? "text-on-accent opacity-70" : "text-fg-faint"
              )}
            >
              {o.count}
            </span>
          </button>
        );
      })}
    </div>
  );
}

export type SortKey = "attention" | "pace" | "budget" | "days" | "name";

const SORT_OPTIONS: Array<[SortKey, string]> = [
  ["attention", "Attention"],
  ["pace", "Pacing"],
  ["budget", "Budget"],
  ["days", "Days left"],
  ["name", "Name"],
];

export function SortSelect({
  value,
  onChange,
}: {
  value: SortKey;
  onChange: (v: SortKey) => void;
}) {
  return (
    <label className="inline-flex items-center gap-2">
      <span className="font-mono text-[10px] uppercase tracking-[0.12em] text-fg-faint">
        Sort
      </span>
      <select
        value={value}
        onChange={(e) => onChange(e.target.value as SortKey)}
        className="cursor-pointer appearance-none rounded-sm border-2 border-line-soft bg-surface-sunken py-1.5 pl-[11px] pr-[26px] font-mono text-[11.5px] text-fg-secondary outline-none"
        style={{
          backgroundImage:
            "url(\"data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' width='10' height='6' viewBox='0 0 10 6'><path fill='%239A9A9A' d='M0 0h10L5 6z'/></svg>\")",
          backgroundRepeat: "no-repeat",
          backgroundPosition: "right 9px center",
        }}
      >
        {SORT_OPTIONS.map(([v, l]) => (
          <option key={v} value={v}>
            {l}
          </option>
        ))}
      </select>
    </label>
  );
}
