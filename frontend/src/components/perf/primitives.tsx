/**
 * Performance primitives — Point Blank design system.
 *
 * Ported from the Claude Design prototype (app/perf-shared.jsx px family)
 * into typed, token-driven components for the Creative and Audiences
 * tabs. Everything consumes semantic tokens (light + dark safe); the
 * only inline styles are dynamic colours via CSS vars and dynamic
 * geometry (SVG coordinates, variable widths).
 */
import type { ReactNode } from "react";
import { cn, platformLabel } from "@/lib/utils";
import {
  quartileRead,
  resolveCell,
  type CreativeBenches,
  type CreativeVerdict,
  type Lens,
  type LensCellInput,
  type LensId,
  type QuartileBench,
} from "@/lib/creative";

/* ── QuartileBar — PB-history track with p25/p50/p75 ticks ───────── */

/**
 * The benchmark dialect: a quiet track for PB campaign history with
 * ticks at p25 / median / p75 and a dot for this campaign's value.
 */
export function QuartileBar({
  value,
  bench,
  width = 140,
  showWord = false,
  className,
}: {
  value: number | null | undefined;
  bench: QuartileBench | null | undefined;
  width?: number;
  showWord?: boolean;
  className?: string;
}) {
  const q = quartileRead(value, bench);
  if (!q) {
    return (
      <span
        className={cn(
          "inline-block font-mono text-[9.5px] text-fg-faint",
          className
        )}
        style={{ width }}
      >
        NO BENCHMARK
      </span>
    );
  }
  const h = 16;
  const y = 9;
  return (
    <div className={className} style={{ width }}>
      <svg width={width} height={h} className="block" aria-hidden="true">
        <line
          x1={3}
          y1={y}
          x2={width - 3}
          y2={y}
          stroke="var(--border-soft)"
          strokeWidth={4}
          strokeLinecap="round"
        />
        <line
          x1={q.tick25 * width}
          y1={y}
          x2={q.tick75 * width}
          y2={y}
          stroke="color-mix(in srgb, var(--text-muted) 38%, transparent)"
          strokeWidth={4}
          strokeLinecap="round"
        />
        {[q.tick25, q.tick50, q.tick75].map((t, i) => (
          <line
            key={i}
            x1={t * width}
            y1={y - (i === 1 ? 6 : 4)}
            x2={t * width}
            y2={y + (i === 1 ? 6 : 4)}
            stroke="var(--text-muted)"
            strokeWidth={i === 1 ? 1.6 : 1.1}
            opacity={i === 1 ? 0.9 : 0.55}
          />
        ))}
        <circle
          cx={q.pos * width}
          cy={y}
          r={4.4}
          fill={q.color}
          stroke="var(--surface-card)"
          strokeWidth={1.6}
        />
      </svg>
      {showWord && (
        <div
          className="mt-0.5 font-mono text-[8.5px] font-bold tracking-[0.1em]"
          style={{ color: q.color }}
        >
          {q.word}
        </div>
      )}
    </div>
  );
}

/* ── VerdictWord — the quartile read as a word, not a track ──────── */

export function VerdictWord({
  value,
  bench,
  size = 9.5,
  className,
}: {
  value: number | null | undefined;
  bench: QuartileBench | null | undefined;
  size?: number;
  className?: string;
}) {
  const q = quartileRead(value, bench);
  if (!q) {
    return (
      <span
        className={cn("font-mono tracking-[0.1em] text-fg-faint", className)}
        style={{ fontSize: size }}
      >
        NO BENCHMARK
      </span>
    );
  }
  return (
    <span
      className={cn(
        "whitespace-nowrap font-mono font-bold tracking-[0.12em]",
        className
      )}
      style={{ fontSize: size, color: q.color }}
    >
      {q.word}
    </span>
  );
}

/* ── Spark — minimal trend line with a last-value dot ────────────── */

export function Spark({
  data,
  width = 64,
  height = 18,
  color = "var(--text-muted)",
  strokeWidth = 1.5,
}: {
  data: number[] | null | undefined;
  width?: number;
  height?: number;
  /** CSS colour token. */
  color?: string;
  strokeWidth?: number;
}) {
  if (!data || data.length < 2) {
    return <span className="inline-block" style={{ width }} />;
  }
  const min = Math.min(...data);
  const max = Math.max(...data);
  const rng = max - min || 1;
  const pts = data.map((v, i) => [
    (i / (data.length - 1)) * (width - 4) + 2,
    height - 3 - ((v - min) / rng) * (height - 6),
  ]);
  const d = pts
    .map((p, i) => (i ? "L" : "M") + p[0].toFixed(1) + " " + p[1].toFixed(1))
    .join(" ");
  const last = pts[pts.length - 1];
  return (
    <svg
      width={width}
      height={height}
      className="block overflow-visible"
      aria-hidden="true"
    >
      <path
        d={d}
        fill="none"
        stroke={color}
        strokeWidth={strokeWidth}
        strokeLinecap="round"
        strokeLinejoin="round"
      />
      <circle cx={last[0]} cy={last[1]} r="2.2" fill={color} />
    </svg>
  );
}

/* ── CoverageLine — honesty about where a number comes from ──────── */

/** "from Meta + TikTok · not StackAdapt" — which platforms a number
 *  actually reports from, so a blended rate can't pose as universal. */
export function CoverageLine({
  label,
  reports,
  missing = [],
  broken = [],
  className,
}: {
  label?: string;
  /** Platform IDs that report the metric. */
  reports: string[];
  /** Active platform IDs that don't. */
  missing?: string[];
  /** Platform IDs whose tracking is visibly broken. */
  broken?: string[];
  className?: string;
}) {
  if (reports.length === 0 && missing.length === 0 && broken.length === 0) {
    return null;
  }
  return (
    <span
      className={cn(
        "font-mono text-[9px] leading-[1.5] tracking-[0.02em] text-fg-faint",
        className
      )}
    >
      {label && <span className="text-fg-muted">{label} </span>}
      from {reports.map(platformLabel).join(" + ")}
      {missing.length > 0 && <> · not {missing.map(platformLabel).join(", ")}</>}
      {broken.length > 0 && (
        <span className="text-warn">
          {" "}
          · {broken.map(platformLabel).join(", ")} broken
        </span>
      )}
    </span>
  );
}

/* ── WarnStrip — slim warning banner ─────────────────────────────── */

export function WarnStrip({
  kind,
  severity = "warning",
  children,
  className,
}: {
  /** Short tag, rendered uppercase (e.g. "fatigue", "tracking"). */
  kind: string;
  severity?: "critical" | "warning" | "info";
  children: ReactNode;
  className?: string;
}) {
  const c =
    severity === "critical"
      ? "var(--danger)"
      : severity === "info"
        ? "var(--info)"
        : "var(--warn)";
  return (
    <div
      className={cn("flex items-start gap-2.5 rounded-sm px-3.5 py-[9px]", className)}
      style={{
        border: `1px solid color-mix(in srgb, ${c} 35%, transparent)`,
        background: `color-mix(in srgb, ${c} 7%, transparent)`,
      }}
    >
      <span
        className="mt-[1.5px] whitespace-nowrap font-mono text-[10px] font-bold tracking-[0.1em]"
        style={{ color: c }}
      >
        {kind.toUpperCase()}
      </span>
      <span className="text-xs leading-relaxed text-fg-secondary">
        {children}
      </span>
    </div>
  );
}

/* ── ThumbFrame — placeholder creative frame, not fake imagery ───── */

/**
 * Abstract frame for a creative: glyph + type chip. `imageUrl` is the
 * slot for the later ad-thumbnails feature (Meta / StackAdapt stills);
 * when present it renders the still instead of the glyph.
 */
export function ThumbFrame({
  type,
  height = 104,
  imageUrl,
  metaRight,
  className,
}: {
  type: "video" | "static";
  height?: number;
  imageUrl?: string | null;
  /** Quiet bottom-right annotation (e.g. "5 wks live"). */
  metaRight?: string;
  className?: string;
}) {
  return (
    <div
      className={cn(
        "relative flex items-center justify-center overflow-hidden rounded-sm border border-line-soft bg-surface-sunken",
        className
      )}
      style={{ height }}
    >
      {imageUrl ? (
        // eslint-disable-next-line @next/next/no-img-element
        <img src={imageUrl} alt="" className="h-full w-full object-cover" />
      ) : (
        <span className="text-[26px] leading-none text-fg-faint" aria-hidden="true">
          {type === "video" ? "▶" : "▦"}
        </span>
      )}
      <span className="absolute left-2 top-2 whitespace-nowrap rounded-xs border border-line-soft bg-surface-card px-1.5 py-0.5 font-mono text-[8.5px] font-semibold tracking-[0.1em] text-fg-muted">
        {type === "video" ? "VIDEO" : "STATIC"}
      </span>
      {metaRight && (
        <span className="absolute bottom-2 right-2 font-mono text-[8.5px] text-fg-faint">
          {metaRight}
        </span>
      )}
    </div>
  );
}

/* ── CreativeVerdictChip — SCALE / HOLD / REFRESH / EARLY ────────── */

export function CreativeVerdictChip({
  verdict,
  size = "md",
  className,
}: {
  verdict: CreativeVerdict;
  size?: "sm" | "md";
  className?: string;
}) {
  const sm = size === "sm";
  const base = cn(
    "inline-flex items-center whitespace-nowrap rounded-xs font-mono font-bold",
    sm ? "px-1.5 py-[2px] text-[7.5px] tracking-[0.12em]" : "px-2.5 py-1 text-[10px] tracking-[0.14em]",
    className
  );
  if (verdict === "SCALE") {
    return <span className={cn(base, "bg-accent text-on-accent")}>SCALE</span>;
  }
  if (verdict === "REFRESH") {
    return (
      <span className={cn(base, "bg-warn text-pb-white")}>REFRESH</span>
    );
  }
  if (verdict === "EARLY") {
    return (
      <span className={cn(base, "border border-line-soft bg-surface-sunken text-fg-faint")}>
        EARLY
      </span>
    );
  }
  return (
    <span className={cn(base, "border border-line bg-transparent text-fg-muted")}>
      HOLD
    </span>
  );
}

/* ── MatrixCell — one cell of a resonance matrix, under a lens ───── */

/**
 * Quartile-tinted cell: value + verdict word, honest gap states, and a
 * FATIGUED HERE override when fatigue is localized to this cell.
 */
export function MatrixCell({
  cell,
  lens,
  benches,
  fatigued = false,
  emptyLabel,
  naTag,
  spend,
}: {
  cell: LensCellInput | null | undefined;
  lens: LensId;
  benches: CreativeBenches;
  /** FATIGUED HERE flag: danger wash + label override. */
  fatigued?: boolean;
  /** Label under the dash when the creative simply isn't running here. */
  emptyLabel?: string;
  /** Gap label for attention metrics (e.g. "STATIC · N/A"). */
  naTag?: string;
  /** Optional quiet spend annotation. */
  spend?: number | null;
}) {
  const r = resolveCell(cell, lens, benches, naTag);

  if (r.kind === "empty") {
    return (
      <div className="px-2 py-3 text-center">
        <div className="font-mono text-[10px] text-fg-faint">—</div>
        {emptyLabel && (
          <div className="mt-0.5 font-mono text-[7px] tracking-[0.06em] text-fg-faint">
            {emptyLabel}
          </div>
        )}
      </div>
    );
  }
  if (r.kind === "broken") {
    return (
      <div
        className="px-2 py-2.5 text-center"
        style={{ background: "color-mix(in srgb, var(--warn) 6%, transparent)" }}
      >
        <div className="font-mono text-[10.5px] font-bold text-warn">⚠</div>
        <div className="mt-0.5 font-mono text-[7.5px] text-warn">BROKEN</div>
      </div>
    );
  }
  if (r.kind === "na") {
    return (
      <div className="px-2 py-[11px] text-center">
        <div className="font-mono text-[10.5px] text-fg-faint">—</div>
        {r.tag && (
          <div className="mt-0.5 font-mono text-[7px] tracking-[0.06em] text-fg-faint">
            {r.tag}
          </div>
        )}
      </div>
    );
  }

  const q = r.read;
  const bg = fatigued
    ? "color-mix(in srgb, var(--danger) 9%, transparent)"
    : q == null
      ? "transparent"
      : q.rank >= 3
        ? "color-mix(in srgb, var(--ok) 11%, transparent)"
        : q.rank === 2
          ? "color-mix(in srgb, var(--ok) 5%, transparent)"
          : q.rank === 1
            ? "color-mix(in srgb, var(--warn) 7%, transparent)"
            : "color-mix(in srgb, var(--danger) 9%, transparent)";

  return (
    <div className="px-2 py-[9px] text-center" style={{ background: bg }}>
      <div
        className={cn(
          "tnum font-mono text-[12.5px] font-bold",
          fatigued ? "text-danger" : "text-fg"
        )}
      >
        {r.text}
      </div>
      <div
        className="mt-0.5 font-mono text-[7px] font-bold tracking-[0.08em]"
        style={{
          color: fatigued ? "var(--danger)" : q?.color ?? "var(--text-faint)",
        }}
      >
        {fatigued ? "FATIGUED HERE" : q?.word ?? "NO BENCHMARK"}
      </div>
      {spend != null && (
        <div className="mt-[3px] font-mono text-[7.5px] text-fg-faint">
          {spend >= 1000 ? `$${(spend / 1000).toFixed(1)}K` : `$${Math.round(spend)}`}
        </div>
      )}
    </div>
  );
}

/* ── LensSwitch — pick what number the matrix cells show ─────────── */

export function LensSwitch({
  lenses,
  lens,
  onLens,
  className,
}: {
  lenses: Lens[];
  lens: LensId;
  onLens: (id: LensId) => void;
  className?: string;
}) {
  return (
    <div className={cn("flex flex-wrap items-center gap-[5px]", className)}>
      {lenses.map((L) => {
        const active = lens === L.id;
        return (
          <button
            key={L.id}
            onClick={() => onLens(L.id)}
            title={L.explain}
            className={cn(
              "whitespace-nowrap rounded-xs border px-2.5 py-1 font-mono text-[9px] font-bold tracking-[0.08em] transition-colors duration-fast",
              active
                ? "border-accent-ink bg-tint-accent text-accent-ink"
                : "border-line bg-transparent text-fg-muted hover:text-fg"
            )}
          >
            {L.primary ? "✱ " : ""}
            {L.label}
          </button>
        );
      })}
    </div>
  );
}

/* ── SectionHead — label row shared by the Call Sheet sections ───── */

export function SectionHead({
  children,
  right,
  className,
}: {
  children: ReactNode;
  right?: ReactNode;
  className?: string;
}) {
  return (
    <div
      className={cn(
        "mb-3 flex flex-wrap items-baseline justify-between gap-3",
        className
      )}
    >
      <span className="label text-fg-secondary">{children}</span>
      {right && (
        <span className="text-right font-mono text-[9.5px] text-fg-faint">
          {right}
        </span>
      )}
    </div>
  );
}
