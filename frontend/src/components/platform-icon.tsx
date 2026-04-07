/**
 * Platform brand icons for ad channels.
 *
 * Uses react-icons/si (Simple Icons) for real brand logos.
 * StackAdapt and Perion fall back to styled text abbreviations.
 */

import {
  SiMeta,
  SiGoogleads,
  SiLinkedin,
  SiTiktok,
  SiSnapchat,
  SiReddit,
  SiPinterest,
} from "react-icons/si";
import type { IconType } from "react-icons";

const BRAND_COLORS: Record<string, string> = {
  meta: "#0081FB",
  google_ads: "#4285F4",
  linkedin: "#0A66C2",
  tiktok: "#FF004F",
  snapchat: "#FFFC00",
  reddit: "#FF4500",
  pinterest: "#E60023",
  stackadapt: "#7C3AED",
  perion: "#F97316",
};

const SI_ICONS: Record<string, IconType> = {
  meta: SiMeta,
  google_ads: SiGoogleads,
  linkedin: SiLinkedin,
  tiktok: SiTiktok,
  snapchat: SiSnapchat,
  reddit: SiReddit,
  pinterest: SiPinterest,
};

const TEXT_LABELS: Record<string, string> = {
  stackadapt: "SA",
  perion: "Pe",
};

export function PlatformIcon({
  platformId,
  className,
}: {
  platformId: string;
  className?: string;
}) {
  const SiIcon = SI_ICONS[platformId];
  const color = BRAND_COLORS[platformId] || "#64748b";
  const textLabel = TEXT_LABELS[platformId];

  return (
    <div
      className={
        className ??
        "flex h-8 w-8 flex-shrink-0 items-center justify-center rounded-md bg-slate-800/60"
      }
    >
      {SiIcon ? (
        <SiIcon size={16} color={color} />
      ) : textLabel ? (
        <span
          className="flex h-5 w-5 items-center justify-center rounded text-[9px] font-bold text-white"
          style={{ backgroundColor: color }}
        >
          {textLabel}
        </span>
      ) : (
        <span className="text-xs font-bold" style={{ color }}>
          {platformId.charAt(0).toUpperCase()}
        </span>
      )}
    </div>
  );
}
