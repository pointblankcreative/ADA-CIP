/**
 * Platform icons for ad channels — Point Blank design system.
 *
 * Sharp tile with a brand-tinted mono glyph (design: charts.jsx
 * PlatformIcon). Replaces the react-icons brand logos: the tinted
 * letter tiles sit better with Chivo Mono labels and the flat,
 * hard-edged surface language, and read clearly at 24–34px.
 */
import { cn } from "@/lib/utils";

/**
 * Channel accent colours from the design prototype (data.js). Tuned for
 * tinted-tile use on both themes — these are identity hues, not raw
 * brand hexes, and only ever appear through color-mix tints.
 */
export const PLATFORM_COLORS: Record<string, string> = {
  meta: "#3b9aff",
  google_ads: "#7aa6ff",
  linkedin: "#3f9af0",
  tiktok: "#ff5a7a",
  snapchat: "#e0b50f",
  stackadapt: "#a78bfa",
  perion: "#fb923c",
  reddit: "#ff6a3d",
  pinterest: "#e60023",
};

const SHORT_LABELS: Record<string, string> = {
  meta: "M",
  google_ads: "G",
  linkedin: "in",
  tiktok: "TT",
  snapchat: "S",
  stackadapt: "SA",
  perion: "Pe",
  reddit: "R",
  pinterest: "Pi",
};

export function PlatformIcon({
  platformId,
  size = 32,
  className,
}: {
  platformId: string;
  /** Tile edge length in px (design default 30; legacy default was h-8 = 32). */
  size?: number;
  className?: string;
}) {
  const color = PLATFORM_COLORS[platformId] ?? "var(--text-faint)";
  const short =
    SHORT_LABELS[platformId] ?? platformId.charAt(0).toUpperCase();

  return (
    <div
      className={cn(
        "flex flex-shrink-0 items-center justify-center rounded-sm font-mono font-bold tracking-[0.02em]",
        className
      )}
      style={{
        width: size,
        height: size,
        fontSize: size * 0.36,
        color,
        backgroundColor: `color-mix(in srgb, ${color} 16%, var(--surface-sunken))`,
        border: `1.5px solid color-mix(in srgb, ${color} 38%, transparent)`,
      }}
    >
      {short}
    </div>
  );
}
