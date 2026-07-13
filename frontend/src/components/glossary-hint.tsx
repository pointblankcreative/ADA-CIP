"use client";

/**
 * One-time, dismissible hint that the dotted-underline / info-dot terms are
 * explainable (ADA 1216090177105300). The glossary affordance (components/
 * glossary.tsx) is easy to miss for a first-time user who doesn't know the
 * vocabulary is clickable — exactly the low-confidence user it helps most.
 * This nudges them once, then stays out of the way.
 *
 * Shown once per user: a localStorage flag survives navigation and reloads.
 * SSR-safe — renders nothing until the client effect has read localStorage,
 * so the server and first client paint agree (no hydration mismatch) and a
 * returning user never flashes the hint.
 */
import { useEffect, useState } from "react";
import { Info, X } from "lucide-react";
import { cn } from "@/lib/utils";

const STORAGE_KEY = "ada:glossary-hint-dismissed";

export function GlossaryHint({ className }: { className?: string }) {
  // null = undecided (pre-mount); false = show; true = dismissed/hidden.
  const [dismissed, setDismissed] = useState<boolean | null>(null);

  useEffect(() => {
    try {
      setDismissed(window.localStorage.getItem(STORAGE_KEY) === "1");
    } catch {
      // Storage blocked (private mode, etc.) — still show once this session.
      setDismissed(false);
    }
  }, []);

  if (dismissed !== false) return null;

  const dismiss = () => {
    try {
      window.localStorage.setItem(STORAGE_KEY, "1");
    } catch {
      /* ignore — the hint just won't persist as dismissed */
    }
    setDismissed(true);
  };

  return (
    <div
      role="note"
      className={cn(
        "flex items-center gap-2 rounded-sm border border-line bg-surface-up px-3 py-2 text-[12px] text-fg-muted",
        className
      )}
    >
      <Info aria-hidden className="h-3.5 w-3.5 shrink-0 opacity-70" />
      <span className="min-w-0">
        Underlined terms have plain-language definitions — hover or focus to
        read.
      </span>
      <button
        type="button"
        onClick={dismiss}
        aria-label="Dismiss hint"
        className="ml-auto shrink-0 rounded-xs p-0.5 text-fg-faint transition-colors hover:text-fg"
      >
        <X aria-hidden className="h-3.5 w-3.5" />
      </button>
    </div>
  );
}
