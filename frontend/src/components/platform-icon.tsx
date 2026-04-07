/**
 * Platform brand icons for ad channels.
 *
 * Simplified SVG marks designed for small (20–32 px) display on dark
 * backgrounds. Each icon uses the platform's brand color.
 */

const iconClass = "h-full w-full";

function Meta() {
  return (
    <svg viewBox="0 0 24 24" fill="none" className={iconClass}>
      <path
        d="M5.2 12c0-2.4 1.2-5 2.8-5 1.1 0 1.9 1.2 3.1 3.4l.9 1.6c1 1.8 2 3.4 3.6 3.4 2.4 0 3.8-3 3.8-5.4 0-3.6-2.6-6.4-7-6.4-5.2 0-9 4.2-9 9.4s3.8 9.4 9 9.4c3 0 5-1.2 6.4-3.4l-1.6-1.2c-1.2 1.6-2.6 2.4-4.8 2.4-3.6 0-6.2-3-6.2-6.6 0-.4 0-.8.1-1.2"
        stroke="#0081FB"
        strokeWidth="1.5"
        strokeLinecap="round"
      />
    </svg>
  );
}

function GoogleAds() {
  return (
    <svg viewBox="0 0 24 24" fill="none" className={iconClass}>
      <path d="M3.5 18.5 10 6.5l4 7-6.5 5z" fill="#FBBC04" />
      <path d="M14 13.5 20.5 18.5 14 6.5 10 13.5z" fill="#4285F4" />
      <circle cx="7" cy="18" r="2.5" fill="#34A853" />
    </svg>
  );
}

function StackAdapt() {
  return (
    <svg viewBox="0 0 24 24" fill="none" className={iconClass}>
      <rect x="4" y="6" width="16" height="3" rx="1" fill="#7C3AED" />
      <rect x="6" y="11" width="12" height="3" rx="1" fill="#A78BFA" />
      <rect x="4" y="16" width="16" height="3" rx="1" fill="#7C3AED" />
    </svg>
  );
}

function LinkedIn() {
  return (
    <svg viewBox="0 0 24 24" fill="none" className={iconClass}>
      <rect x="4" y="4" width="16" height="16" rx="2" fill="#0A66C2" />
      <path
        d="M8.5 10.5v5M8.5 8v.5M11 15.5v-3c0-1.1.9-2 2-2s2 .9 2 2v3"
        stroke="white"
        strokeWidth="1.5"
        strokeLinecap="round"
      />
    </svg>
  );
}

function TikTok() {
  return (
    <svg viewBox="0 0 24 24" fill="none" className={iconClass}>
      <path
        d="M16.5 4.5v7.5a5 5 0 1 1-3-4.6V10a2.5 2.5 0 1 0 1 2V4.5h2Z"
        fill="#FF004F"
      />
      <path
        d="M15.5 4.5v7.5a5 5 0 1 1-3-4.6V10a2.5 2.5 0 1 0 1 2V4.5h2Z"
        fill="#00F2EA"
        opacity="0.6"
      />
    </svg>
  );
}

function Snapchat() {
  return (
    <svg viewBox="0 0 24 24" fill="none" className={iconClass}>
      <path
        d="M12 4c-2.5 0-4 2-4 4.5 0 1 .2 2 .5 3-1 .3-2 .5-2 1s1.2.8 1.5 1c-.5 1-1.5 2-3 2.5.5.5 1.5.5 2.5.5.3.5.5 1.5 1 1.5s1.5-1 3.5-1 3 1 3.5 1 .7-1 1-1.5c1 0 2 0 2.5-.5-1.5-.5-2.5-1.5-3-2.5.3-.2 1.5-.5 1.5-1s-1-.7-2-1c.3-1 .5-2 .5-3C18 6 16.5 4 14 4h-2Z"
        fill="#FFFC00"
        stroke="#333"
        strokeWidth="0.5"
      />
    </svg>
  );
}

function Reddit() {
  return (
    <svg viewBox="0 0 24 24" fill="none" className={iconClass}>
      <circle cx="12" cy="13" r="7" fill="#FF4500" />
      <circle cx="9.5" cy="12" r="1" fill="white" />
      <circle cx="14.5" cy="12" r="1" fill="white" />
      <path
        d="M9 15.5c1 1 4 1 5 0"
        stroke="white"
        strokeWidth="0.8"
        strokeLinecap="round"
      />
      <circle cx="17" cy="6" r="1.5" fill="#FF4500" />
      <path d="M14 5.5 17 6" stroke="#FF4500" strokeWidth="1" />
    </svg>
  );
}

function Pinterest() {
  return (
    <svg viewBox="0 0 24 24" fill="none" className={iconClass}>
      <circle cx="12" cy="12" r="8" fill="#E60023" />
      <path
        d="M12 7c-2.5 0-4.5 2-4.5 4.5 0 1.5.8 2.8 2 3.5-.1-.5 0-1.2.1-1.8l.7-3s-.2-.4-.2-.9c0-.8.5-1.4 1.1-1.4.5 0 .8.4.8.9 0 .5-.3 1.3-.5 2 0 .6.5 1 1 1 1.3 0 2.2-1.3 2.2-3.2 0-1.7-1.2-2.8-2.9-2.8"
        fill="white"
      />
    </svg>
  );
}

function Perion() {
  return (
    <svg viewBox="0 0 24 24" fill="none" className={iconClass}>
      <rect x="4" y="8" width="5" height="12" rx="1" fill="#F97316" />
      <rect x="10" y="5" width="5" height="15" rx="1" fill="#FB923C" />
      <rect x="16" y="10" width="5" height="10" rx="1" fill="#F97316" />
    </svg>
  );
}

function DefaultIcon({ label }: { label: string }) {
  return (
    <span className="text-xs font-bold text-slate-400">
      {label.charAt(0).toUpperCase()}
    </span>
  );
}

const ICON_MAP: Record<string, () => JSX.Element> = {
  meta: Meta,
  google_ads: GoogleAds,
  stackadapt: StackAdapt,
  linkedin: LinkedIn,
  tiktok: TikTok,
  snapchat: Snapchat,
  reddit: Reddit,
  pinterest: Pinterest,
  perion: Perion,
};

/**
 * Renders a platform brand icon inside a rounded container.
 *
 * Usage: <PlatformIcon platformId="meta" />
 */
export function PlatformIcon({
  platformId,
  className,
}: {
  platformId: string;
  className?: string;
}) {
  const Icon = ICON_MAP[platformId];
  return (
    <div
      className={
        className ??
        "flex h-8 w-8 flex-shrink-0 items-center justify-center rounded-md bg-slate-800/60"
      }
    >
      {Icon ? <Icon /> : <DefaultIcon label={platformId} />}
    </div>
  );
}
