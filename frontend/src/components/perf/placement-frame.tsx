"use client";

/**
 * PlacementFrame — wraps a creative still in a skeleton of the environment
 * where it actually runs, so the preview reads as a real ad in context
 * rather than a floating thumbnail.
 *
 *   phone → 9:16 Stories / Reels / TikTok chrome (progress bar, handle,
 *           caption, right-rail action buttons)
 *   feed  → 1:1 / 4:5 in-feed social card (avatar, handle, like/comment bar)
 *   web   → display/banner in a news-site skeleton (nav, article columns,
 *           a labelled ad slot sized to the creative's aspect ratio)
 *
 * Placement is resolved from the platform first (TikTok/Snap → phone,
 * programmatic display → web), then the still's measured aspect ratio for
 * Meta/unknown (vertical → phone, wide → web, else feed). The skeleton is
 * grey placeholder UI; only the creative carries colour.
 */
import { useState } from "react";
import { Heart, MessageCircle, Send } from "lucide-react";

type Placement = "phone" | "feed" | "web";

const SKEL = "color-mix(in srgb, var(--fg) 10%, transparent)";
const SKEL_STRONG = "color-mix(in srgb, var(--fg) 16%, transparent)";

function resolvePlacement(platforms: string[], ratio: number | null): Placement {
  const p = platforms.map((x) => x.toLowerCase());
  const has = (...keys: string[]) =>
    p.some((x) => keys.some((k) => x.includes(k)));
  if (has("tiktok", "snap")) return "phone";
  if (
    has(
      "stackadapt",
      "dv360",
      "trade",
      "programmatic",
      "display",
      "adform",
      "taboola",
      "outbrain",
      "yahoo",
      "amazon"
    )
  )
    return "web";
  // Meta / Instagram / unknown: shape decides.
  if (ratio != null && ratio < 0.72) return "phone"; // 9:16-ish
  if (ratio != null && ratio > 1.5) return "web"; // wide banner
  return "feed"; // 1:1 / 4:5
}

function Bar({
  w,
  h = 6,
  strong,
  className,
}: {
  w: number | string;
  h?: number;
  strong?: boolean;
  className?: string;
}) {
  return (
    <div
      className={className}
      style={{
        width: w,
        height: h,
        borderRadius: 999,
        background: strong ? SKEL_STRONG : SKEL,
      }}
    />
  );
}

function Dot({ size = 22, strong }: { size?: number; strong?: boolean }) {
  return (
    <div
      style={{
        width: size,
        height: size,
        borderRadius: 999,
        background: strong ? SKEL_STRONG : SKEL,
        flexShrink: 0,
      }}
    />
  );
}

/* ── phone: Stories / Reels / TikTok ─────────────────────────────── */

function PhoneSkeleton({ media }: { media: React.ReactNode }) {
  return (
    <div
      className="relative overflow-hidden rounded-[18px] border border-line-soft bg-surface-card shadow-hard"
      style={{ width: 126, height: 224 }}
    >
      <div className="absolute inset-0">{media}</div>
      {/* progress segments */}
      <div className="absolute left-2 right-2 top-2 flex gap-1">
        {[0, 1, 2].map((i) => (
          <div
            key={i}
            className="h-[3px] flex-1 rounded-full"
            style={{ background: i === 0 ? "rgba(255,255,255,0.9)" : "rgba(255,255,255,0.4)" }}
          />
        ))}
      </div>
      {/* handle */}
      <div className="absolute left-2 top-[14px] flex items-center gap-1.5">
        <div className="h-4 w-4 rounded-full bg-white/80" />
        <div className="h-[5px] w-12 rounded-full bg-white/70" />
      </div>
      {/* right-rail actions */}
      <div className="absolute bottom-3 right-1.5 flex flex-col items-center gap-3 text-white/90">
        <Heart className="h-[15px] w-[15px]" fill="currentColor" />
        <MessageCircle className="h-[15px] w-[15px]" fill="currentColor" />
        <Send className="h-[15px] w-[15px]" />
      </div>
      {/* caption */}
      <div className="absolute bottom-3 left-2 flex w-[78px] flex-col gap-1">
        <div className="h-[5px] w-full rounded-full bg-white/70" />
        <div className="h-[5px] w-2/3 rounded-full bg-white/50" />
      </div>
    </div>
  );
}

/* ── feed: 1:1 / 4:5 in-feed social card ─────────────────────────── */

function FeedSkeleton({ media }: { media: React.ReactNode }) {
  return (
    <div
      className="overflow-hidden rounded-md border border-line-soft bg-surface-card shadow-hard"
      style={{ width: 188 }}
    >
      {/* header: avatar + handle */}
      <div className="flex items-center gap-2 px-2.5 py-2">
        <Dot size={20} strong />
        <div className="flex flex-col gap-1">
          <Bar w={64} h={5} strong />
          <Bar w={40} h={4} />
        </div>
        <div className="ml-auto flex gap-[3px]">
          <Dot size={3} strong />
          <Dot size={3} strong />
          <Dot size={3} strong />
        </div>
      </div>
      {/* the creative, square */}
      <div className="aspect-square w-full overflow-hidden bg-surface-sunken">{media}</div>
      {/* action bar */}
      <div className="flex items-center gap-3 px-2.5 pt-2 text-fg-faint">
        <Heart className="h-[15px] w-[15px]" />
        <MessageCircle className="h-[15px] w-[15px]" />
        <Send className="h-[15px] w-[15px]" />
      </div>
      {/* caption lines */}
      <div className="flex flex-col gap-1 px-2.5 pb-2.5 pt-2">
        <Bar w="85%" h={5} />
        <Bar w="55%" h={5} />
      </div>
    </div>
  );
}

/* ── web: display banner in a news-site skeleton ─────────────────── */

function WebSkeleton({
  media,
  ratio,
}: {
  media: React.ReactNode;
  ratio: number | null;
}) {
  // Leaderboard-ish (very wide) sits across the top; everything else rides
  // the right rail like a medium rectangle / half-page unit.
  const leaderboard = ratio != null && ratio >= 2.5;
  const slotRatio = ratio ?? 1.2;
  const adSlot = (
    <div
      className="relative overflow-hidden rounded-[3px] border border-line-soft bg-surface-sunken"
      style={leaderboard ? { width: "100%", aspectRatio: String(slotRatio) } : { width: 96, aspectRatio: String(slotRatio) }}
    >
      {media}
      <span className="absolute left-1 top-1 rounded-[2px] bg-surface-card/85 px-1 font-mono text-[6px] uppercase tracking-[0.12em] text-fg-faint">
        Ad
      </span>
    </div>
  );

  return (
    <div
      className="flex w-[252px] flex-col overflow-hidden rounded-md border border-line-soft bg-surface-card shadow-hard"
      style={{ height: 224 }}
    >
      {/* browser bar */}
      <div className="flex items-center gap-1.5 border-b border-line-soft px-2.5 py-1.5">
        <Dot size={5} strong />
        <Dot size={5} strong />
        <Dot size={5} strong />
        <div className="ml-1.5 h-3 flex-1 rounded-full" style={{ background: SKEL }} />
      </div>
      {/* masthead */}
      <div className="flex items-center justify-between border-b border-line-soft px-3 py-2">
        <Bar w={54} h={9} strong />
        <div className="flex gap-2">
          <Bar w={18} h={5} />
          <Bar w={18} h={5} />
          <Bar w={18} h={5} />
        </div>
      </div>
      {/* leaderboard slot */}
      {leaderboard && <div className="px-3 pt-2.5">{adSlot}</div>}
      {/* body: article column + right rail */}
      <div className="flex flex-1 gap-3 px-3 py-2.5">
        <div className="flex flex-1 flex-col gap-1.5">
          <Bar w="90%" h={8} strong />
          <Bar w="70%" h={8} strong />
          <div className="mt-1.5 flex flex-col gap-[5px]">
            {[0, 1, 2, 3, 4].map((i) => (
              <Bar key={i} w={i === 4 ? "60%" : "100%"} h={4} />
            ))}
          </div>
        </div>
        {!leaderboard && <div className="flex flex-col items-center pt-0.5">{adSlot}</div>}
      </div>
    </div>
  );
}

/* ── the wrapper ─────────────────────────────────────────────────── */

export function PlacementFrame({
  imageUrl,
  type,
  platforms,
  alt = "",
}: {
  imageUrl?: string | null;
  type: "video" | "static";
  platforms: string[];
  alt?: string;
}) {
  const [ratio, setRatio] = useState<number | null>(null);
  const placement = resolvePlacement(platforms, ratio);

  const media = imageUrl ? (
    // eslint-disable-next-line @next/next/no-img-element
    <img
      src={imageUrl}
      alt={alt}
      onLoad={(e) => {
        const t = e.currentTarget;
        if (t.naturalWidth && t.naturalHeight) {
          setRatio(t.naturalWidth / t.naturalHeight);
        }
      }}
      className="h-full w-full object-cover"
    />
  ) : (
    <div className="flex h-full w-full items-center justify-center bg-surface-sunken">
      <span className="text-[22px] leading-none text-fg-faint" aria-hidden="true">
        {type === "video" ? "▶" : "▦"}
      </span>
    </div>
  );

  return (
    <div className="relative flex items-center justify-center rounded-md bg-surface-sunken px-3 py-3" style={{ minHeight: 224 }}>
      {placement === "phone" && <PhoneSkeleton media={media} />}
      {placement === "feed" && <FeedSkeleton media={media} />}
      {placement === "web" && <WebSkeleton media={media} ratio={ratio} />}
      {/* media-type chip, kept from the old frame */}
      <span className="absolute left-2 top-2 z-10 whitespace-nowrap rounded-xs border border-line-soft bg-surface-card px-1.5 py-0.5 font-mono text-[8.5px] font-semibold tracking-[0.1em] text-fg-muted">
        {type === "video" ? "VIDEO" : "STATIC"}
      </span>
    </div>
  );
}
