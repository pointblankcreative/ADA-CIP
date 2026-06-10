/**
 * ADA-CIP shared primitives — Point Blank design system.
 *
 * Ported from the Claude Design prototype (app/components.jsx) into
 * typed, token-driven components. Everything here consumes semantic
 * Tailwind tokens (surface-*, fg-*, line-*, accent, status colours) so
 * the same markup renders correctly in both light and dark contexts.
 */
import type { CSSProperties, ReactNode } from "react";
import { cn } from "@/lib/utils";

/* ── Label / Eyebrow ──────────────────────────────────────────────── */

export function Label({
  children,
  className,
}: {
  children: ReactNode;
  className?: string;
}) {
  return <div className={cn("label", className)}>{children}</div>;
}

/** Tracked-mono eyebrow with the ✱ logomark as punctuation. */
export function Eyebrow({
  children,
  className,
}: {
  children: ReactNode;
  className?: string;
}) {
  return <div className={cn("eyebrow", className)}>{children}</div>;
}

/* ── CodeChip — project / line codes ─────────────────────────────── */

export function CodeChip({
  children,
  accent = false,
  className,
}: {
  children: ReactNode;
  accent?: boolean;
  className?: string;
}) {
  return (
    <span
      className={cn(
        "inline-flex items-center rounded-xs border px-1.5 py-0.5 font-mono text-xs font-medium tracking-[0.04em]",
        accent
          ? "border-tint-accent bg-tint-accent text-accent-ink"
          : "border-line-soft bg-surface-sunken text-fg-muted",
        className
      )}
    >
      {children}
    </span>
  );
}

/* ── StatusPill — pacing / diagnostic status ─────────────────────── */

export function StatusPill({
  label,
  color,
  dot = true,
  size = "md",
  className,
}: {
  label: string;
  /** CSS colour value — pass a token, e.g. "var(--ok)". */
  color: string;
  dot?: boolean;
  size?: "sm" | "md";
  className?: string;
}) {
  const sm = size === "sm";
  return (
    <span
      className={cn(
        "inline-flex items-center whitespace-nowrap rounded-pill border font-mono font-semibold uppercase",
        sm ? "gap-[5px] px-2 py-0.5 text-[10.5px]" : "gap-1.5 px-2.5 py-1 text-[11.5px]",
        "tracking-[0.06em]",
        className
      )}
      style={{
        color,
        borderColor: `color-mix(in srgb, ${color} 40%, transparent)`,
        backgroundColor: `color-mix(in srgb, ${color} 13%, transparent)`,
        borderWidth: "1.5px",
      }}
    >
      {dot && (
        <span
          className={cn("rounded-full flex-shrink-0", sm ? "h-[5px] w-[5px]" : "h-1.5 w-1.5")}
          style={{ backgroundColor: color }}
        />
      )}
      {label}
    </span>
  );
}

/* ── Button ──────────────────────────────────────────────────────── */

export type BtnVariant = "primary" | "secondary" | "outline" | "ghost";
export type BtnSize = "sm" | "md" | "lg";

const BTN_VARIANTS: Record<BtnVariant, string> = {
  primary:
    "bg-accent text-on-accent border-accent hover:bg-accent-hover hover:border-accent-hover active:bg-accent-press active:border-accent-press",
  secondary:
    "bg-fg text-surface-page border-fg hover:opacity-90",
  outline:
    "bg-transparent text-fg border-line hover:border-line-strong",
  ghost:
    "bg-transparent text-fg-secondary border-transparent hover:text-fg hover:bg-surface-sunken",
};

const BTN_SIZES: Record<BtnSize, string> = {
  sm: "px-3 py-[7px] text-xs",
  md: "px-4 py-2.5 text-[13.5px]",
  lg: "px-[22px] py-[13px] text-[15px]",
};

export function Btn({
  children,
  variant = "primary",
  size = "md",
  icon,
  fullWidth = false,
  className,
  style,
  ...rest
}: {
  children: ReactNode;
  variant?: BtnVariant;
  size?: BtnSize;
  /** Optional leading icon node (lucide-react element). */
  icon?: ReactNode;
  fullWidth?: boolean;
  className?: string;
  style?: CSSProperties;
} & React.ButtonHTMLAttributes<HTMLButtonElement>) {
  return (
    <button
      className={cn(
        "inline-flex items-center justify-center gap-2 whitespace-nowrap rounded-sm border-2 font-bold tracking-[0.01em]",
        "transition-all duration-fast ease-snap disabled:cursor-not-allowed disabled:opacity-50",
        BTN_VARIANTS[variant],
        BTN_SIZES[size],
        fullWidth && "w-full",
        className
      )}
      style={style}
      {...rest}
    >
      {icon}
      {children}
    </button>
  );
}

/* ── IconBtn — square icon-only action ───────────────────────────── */

export function IconBtn({
  icon,
  label,
  active = false,
  className,
  ...rest
}: {
  icon: ReactNode;
  label: string;
  active?: boolean;
  className?: string;
} & React.ButtonHTMLAttributes<HTMLButtonElement>) {
  return (
    <button
      aria-label={label}
      title={label}
      className={cn(
        "inline-flex h-[34px] w-[34px] items-center justify-center rounded-sm border-2 transition-all duration-fast",
        active
          ? "border-accent bg-tint-accent text-accent-ink"
          : "border-line text-fg-secondary hover:border-line-strong hover:text-fg",
        className
      )}
      {...rest}
    >
      {icon}
    </button>
  );
}

/* ── PBMark — the Point Blank asterisk + underscore logomark ────── */

export function PBMark({
  size = 22,
  /* accent-ink: chartreuse on dark, darkened chartreuse on light — the raw
     accent disappears against light surfaces. */
  color = "var(--accent-ink)",
  className,
}: {
  size?: number;
  color?: string;
  className?: string;
}) {
  return (
    <svg
      width={size}
      height={size * (1200 / 866.42)}
      viewBox="0 0 866.42 1200"
      className={cn("block flex-shrink-0", className)}
      aria-hidden="true"
    >
      <g fill={color}>
        <rect x="120.27" y="854.75" width="625.82" height="224.97" rx="5.56" ry="5.56" />
        <path d="M745.76,419.4l-71.85-205.25c-1.3-3.71-5.43-5.6-9.1-4.16l-117.37,46.21c-4.79,1.89-9.9-1.9-9.47-7.02l10.23-121.41c.34-4.04-2.85-7.5-6.92-7.5h-216.1c-4.06,0-7.26,3.47-6.92,7.5l10.23,121.41c.43,5.12-4.68,8.91-9.47,7.02l-117.37-46.21c-3.67-1.44-7.8.44-9.1,4.16l-71.85,205.25c-1.3,3.72.77,7.78,4.56,8.91l113.12,33.85c4.63,1.39,6.46,6.95,3.55,10.8l-73.44,97.11c-2.31,3.06-1.7,7.4,1.37,9.7l173.88,130.42c3.13,2.35,7.59,1.65,9.85-1.55l73.96-104.91c2.76-3.92,8.59-3.92,11.36,0l73.96,104.91c2.26,3.2,6.72,3.9,9.85,1.55l173.88-130.42c3.07-2.3,3.68-6.65,1.37-9.7l-73.44-97.11c-2.91-3.85-1.08-9.42,3.55-10.8l113.12-33.85c3.79-1.13,5.86-5.19,4.56-8.91Z" />
      </g>
    </svg>
  );
}
