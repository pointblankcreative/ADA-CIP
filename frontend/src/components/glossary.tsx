"use client";

/**
 * Glossary, a hover/focus/click definition popover for a metric term.
 *
 * Wraps a label (or arbitrary children) in a discoverable trigger and,
 * when opened, shows a small fixed-position panel with the plain-language
 * definition from lib/glossary.ts. If the term has no entry the trigger
 * disappears entirely (plain children, no affordance, no tab stop), which
 * lets callers wrap many signal names by id and only the ones with
 * definitions light up.
 *
 * Positioning mirrors the clamp approach in components/signals/instrument.tsx
 * (SignalTooltip) but anchors to the viewport with position:fixed instead of
 * an absolute parent, because the panel must escape the cards' overflow-hidden,
 * so it uses fixed + window innerWidth/innerHeight, not absolute + parent bounds.
 */
import {
  useCallback,
  useEffect,
  useId,
  useLayoutEffect,
  useRef,
  useState,
} from "react";
import { Info } from "lucide-react";
import { lookupTerm } from "@/lib/glossary";
import { cn } from "@/lib/utils";

interface GlossaryProps {
  termKey: string;
  children?: React.ReactNode;
  variant?: "underline" | "icon" | "wrap";
  side?: "top" | "bottom";
  className?: string;
}

interface PanelPos {
  left: number;
  top: number;
}

const PAD = 8; // viewport padding for the clamp
const GAP = 8; // gap between trigger and panel

export function Glossary({
  termKey,
  children,
  variant = "underline",
  side = "top",
  className,
}: GlossaryProps) {
  const def = lookupTerm(termKey);

  const triggerRef = useRef<HTMLButtonElement>(null);
  const panelRef = useRef<HTMLDivElement>(null);
  const closeTimer = useRef<ReturnType<typeof setTimeout> | null>(null);
  const [open, setOpen] = useState(false);
  const [pos, setPos] = useState<PanelPos | null>(null);
  const panelId = useId();

  const clearCloseTimer = useCallback(() => {
    if (closeTimer.current) {
      clearTimeout(closeTimer.current);
      closeTimer.current = null;
    }
  }, []);

  const openNow = useCallback(() => {
    clearCloseTimer();
    setOpen(true);
  }, [clearCloseTimer]);

  // Close after a short grace so the cursor can travel from the trigger
  // onto the panel without the panel vanishing underneath it.
  const scheduleClose = useCallback(() => {
    clearCloseTimer();
    closeTimer.current = setTimeout(() => setOpen(false), 120);
  }, [clearCloseTimer]);

  const closeNow = useCallback(() => {
    clearCloseTimer();
    setOpen(false);
  }, [clearCloseTimer]);

  // Measure + place the panel after it renders, reading layout only inside
  // the effect (never during render) so SSR never touches window.
  useLayoutEffect(() => {
    if (!open) return;
    const trigger = triggerRef.current;
    const panel = panelRef.current;
    if (!trigger || !panel) return;
    const r = trigger.getBoundingClientRect();
    const pw = panel.offsetWidth;
    const ph = panel.offsetHeight;
    const vw = window.innerWidth;
    const vh = window.innerHeight;

    // Center horizontally over the trigger, then clamp into the viewport.
    let left = r.left + r.width / 2 - pw / 2;
    left = Math.min(Math.max(left, PAD), Math.max(PAD, vw - pw - PAD));

    // Prefer the requested side, flip if there isn't room, then clamp.
    let top: number;
    if (side === "bottom") {
      top = r.bottom + GAP;
      if (top + ph > vh - PAD) top = r.top - GAP - ph;
    } else {
      top = r.top - GAP - ph;
      if (top < PAD) top = r.bottom + GAP;
    }
    top = Math.min(Math.max(top, PAD), Math.max(PAD, vh - ph - PAD));

    setPos((p) => (p && p.left === left && p.top === top ? p : { left, top }));
  }, [open, side]);

  // Listeners only while open: Escape (return focus to trigger) + scroll.
  useEffect(() => {
    if (!open) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") {
        closeNow();
        triggerRef.current?.focus();
      }
    };
    const onScroll = () => closeNow();
    window.addEventListener("keydown", onKey);
    window.addEventListener("scroll", onScroll, true);
    return () => {
      window.removeEventListener("keydown", onKey);
      window.removeEventListener("scroll", onScroll, true);
    };
  }, [open, closeNow]);

  useEffect(() => () => clearCloseTimer(), [clearCloseTimer]);

  // No entry → plain children, no trigger, no affordance, no tab stop.
  if (!def) return <>{children}</>;

  const body = children ?? def.label;

  const handleClick = (e: React.MouseEvent) => {
    e.stopPropagation();
    if (open) closeNow();
    else openNow();
  };

  return (
    <>
      <button
        ref={triggerRef}
        type="button"
        aria-label={`Definition of ${def.label}`}
        aria-describedby={open ? panelId : undefined}
        onClick={handleClick}
        onMouseEnter={openNow}
        onMouseLeave={scheduleClose}
        onFocus={openNow}
        onBlur={scheduleClose}
        className={cn(
          "cursor-help",
          variant === "underline" &&
            "border-b border-dotted border-current/60",
          (variant === "icon" || variant === "wrap") &&
            "inline-flex items-center gap-[3px]",
          className
        )}
      >
        {body}
        {(variant === "icon" || variant === "wrap") && (
          <Info aria-hidden className="h-3 w-3 opacity-70" />
        )}
      </button>
      {open && (
        <div
          ref={panelRef}
          id={panelId}
          role="tooltip"
          onMouseEnter={openNow}
          onMouseLeave={scheduleClose}
          className={cn(
            "fixed z-50 max-w-[280px] min-w-[180px] rounded-sm border-2 border-line bg-surface-up px-[11px] py-[9px] shadow-soft",
            "motion-safe:transition-opacity motion-safe:duration-fast motion-safe:ease-snap"
          )}
          style={{
            left: pos?.left ?? 0,
            top: pos?.top ?? 0,
            visibility: pos ? "visible" : "hidden",
          }}
        >
          <div className="text-fg text-[12px] font-semibold">{def.label}</div>
          <div className="text-fg-muted text-[12px] leading-snug mt-1">
            {def.definition}
          </div>
          {def.how && (
            <>
              <div className="font-mono text-[9px] tracking-wide text-fg-faint uppercase mt-2">
                How ADA reads it
              </div>
              <div className="text-fg-muted text-[11px] leading-snug">
                {def.how}
              </div>
            </>
          )}
          {def.unit && (
            <div className="text-[10px] text-fg-faint mt-1">{def.unit}</div>
          )}
        </div>
      )}
    </>
  );
}
