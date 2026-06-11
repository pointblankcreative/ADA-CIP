"use client";

/**
 * Shared Signal-instrument plumbing: the Orbit driver hook, the opt-in
 * sound hook, the Listen button, and the cursor-following readout card.
 *
 * Sound is strictly opt-in: silent until the Listen button is pressed
 * (tape click on/off), hovering a body solos its voice, and it always
 * switches itself off after a short countdown. The work tool never sings
 * uninvited.
 */
import {
  useCallback,
  useEffect,
  useLayoutEffect,
  useRef,
  useState,
  type MutableRefObject,
  type RefObject,
} from "react";
import { Volume2, VolumeX } from "lucide-react";
import { HealthAudio } from "@/lib/viz/audio-engine";
import {
  COLORS,
  STATUS_WORD,
  fmtMoneyShort,
  syncThemeFromElement,
  type SignalItem,
} from "@/lib/viz/health-core";
import { createOrbit, type OrbitInstance } from "@/lib/viz/viz-orbit";
import { cn } from "@/lib/utils";

const SIGNALS_VOICE = "tuneup";

/* Drive an Orbit on a canvas — staged entrance: a quiet beat after load,
   the shells bloom outward from the core, then the bodies glide in slowly
   enough to read as orbital motion rather than arrival. */
export function useOrbitInstrument(
  canvasRef: RefObject<HTMLCanvasElement>,
  itemsRef: MutableRefObject<SignalItem[]>,
  hoverRef: MutableRefObject<string | null>,
  audioRef: MutableRefObject<HealthAudio>,
  compact: boolean
): MutableRefObject<OrbitInstance | null> {
  const vizRef = useRef<OrbitInstance | null>(null);

  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;
    syncThemeFromElement(canvas);
    const viz = createOrbit(canvas, { compact, gentle: true });
    vizRef.current = viz;
    viz.resize();
    viz.setItems(itemsRef.current);
    const ro = new ResizeObserver(() => viz.resize());
    ro.observe(canvas);
    const reduce = window.matchMedia("(prefers-reduced-motion: reduce)").matches;
    let raf = 0;
    const t0 = performance.now();
    let last = t0;
    const BEAT = 0.6; // nothing happens for a beat
    const BODIES_AT = BEAT + 1.05; // rings bloom during the gap, then bodies
    const loop = (now: number) => {
      const t = (now - t0) / 1000;
      const dt = (now - last) / 1000;
      last = now;
      let ringT: number | null = t - BEAT;
      let bootP = (i: number) => {
        const u = Math.max(0, Math.min(1, (t - BODIES_AT - i * 0.16) / 1.7));
        return u * u * (3 - 2 * u);
      };
      if (reduce) {
        ringT = 99;
        bootP = () => 1; // no choreography under reduced motion
      }
      viz.frame({
        t,
        dt,
        bootP,
        ringT,
        sched: null,
        focusId: null,
        hoverId: hoverRef.current,
        reduce,
        audio: audioRef.current.levels(itemsRef.current),
      });
      raf = requestAnimationFrame(loop);
    };
    raf = requestAnimationFrame(loop);
    return () => {
      cancelAnimationFrame(raf);
      ro.disconnect();
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  return vizRef;
}

/* Opt-in sound with auto-off. */
export function useOptInSound(
  audioRef: MutableRefObject<HealthAudio>,
  itemsRef: MutableRefObject<SignalItem[]>,
  autoOffSecs: number
) {
  const [soundOn, setSoundOn] = useState(false);
  const [offAt, setOffAt] = useState<number | null>(null);
  const [, tick] = useState(0);

  useEffect(() => {
    if (offAt == null) return;
    const iv = setInterval(() => {
      if (performance.now() >= offAt) {
        audioRef.current.setMuted(true, true);
        setSoundOn(false);
        setOffAt(null);
      } else tick((n) => n + 1);
    }, 200);
    return () => clearInterval(iv);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [offAt]);

  // leaving the view silences and tears down the voices
  useEffect(
    () => () => {
      try {
        audioRef.current.setMuted(true, false);
        audioRef.current.stopAll();
      } catch {
        /* already torn down */
      }
    },
    // eslint-disable-next-line react-hooks/exhaustive-deps
    []
  );

  const toggle = useCallback(() => {
    const a = audioRef.current;
    if (soundOn) {
      a.setMuted(true, true);
      setSoundOn(false);
      setOffAt(null);
    } else {
      a.engage();
      a.setVoicing(SIGNALS_VOICE);
      a.cruise(itemsRef.current); // straight to the cruise — no boot overture in the tool
      a.setMuted(false, true);
      setSoundOn(true);
      setOffAt(performance.now() + autoOffSecs * 1000);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [soundOn, autoOffSecs]);

  const secsLeft =
    offAt == null ? null : Math.max(0, Math.ceil((offAt - performance.now()) / 1000));
  return { soundOn, toggle, secsLeft };
}

export function SignalSoundButton({
  soundOn,
  toggle,
  secsLeft,
}: {
  soundOn: boolean;
  toggle: () => void;
  secsLeft: number | null;
}) {
  return (
    <button
      onClick={toggle}
      title={
        soundOn
          ? `Stops itself in ${secsLeft}s — click to stop now`
          : "Hear the signals — on pace runs steady, trouble sounds off"
      }
      className={cn(
        "inline-flex flex-shrink-0 items-center gap-[7px] rounded-sm border-2 px-2.5 py-[5px] font-mono text-[10px] font-bold uppercase tracking-[0.1em] transition-colors duration-fast",
        soundOn
          ? "border-accent bg-accent text-on-accent"
          : "border-line-soft bg-transparent text-fg-muted hover:border-line"
      )}
    >
      {soundOn ? (
        <Volume2 className="h-[13px] w-[13px]" />
      ) : (
        <VolumeX className="h-[13px] w-[13px]" />
      )}
      {soundOn ? "0:" + String(secsLeft ?? 0).padStart(2, "0") : "Listen"}
    </button>
  );
}

/* Cursor-following readout card for a hovered body. Measured after render
   and CLAMPED inside the stage rather than naively flipped — on short
   stages (Pacing Signal is 210px) a flip-above pushed the card past the
   panel's overflow-hidden top edge and clipped it. */
export function SignalTooltip({
  item,
  pos,
  bounds,
}: {
  item: SignalItem | undefined;
  pos: { x: number; y: number } | null;
  bounds: { w: number; h: number };
}) {
  const ref = useRef<HTMLDivElement>(null);
  const [size, setSize] = useState({ w: 200, h: 110 });
  useLayoutEffect(() => {
    const el = ref.current;
    if (!el) return;
    const w = el.offsetWidth;
    const h = el.offsetHeight;
    setSize((s) => (s.w === w && s.h === h ? s : { w, h }));
  });
  if (!item || !pos) return null;
  const c = COLORS[item.sev];
  const PAD = 6;
  const GAP = 15;
  // prefer below-right of the cursor; flip a side only if it overflows,
  // then clamp so the card always stays fully inside the stage
  let left = pos.x + GAP;
  if (left + size.w > bounds.w - PAD) left = pos.x - GAP - size.w;
  left = Math.min(Math.max(left, PAD), Math.max(PAD, bounds.w - size.w - PAD));
  let top = pos.y + GAP;
  if (top + size.h > bounds.h - PAD) top = pos.y - GAP - size.h;
  top = Math.min(Math.max(top, PAD), Math.max(PAD, bounds.h - size.h - PAD));
  return (
    <div
      ref={ref}
      className="pointer-events-none absolute z-[5] min-w-[185px] max-w-[240px] rounded-sm border-2 border-line bg-surface-up px-[11px] py-[9px] shadow-soft"
      style={{ left, top }}
    >
      <div className="flex items-center gap-2">
        <span className="font-mono text-[10px] font-bold" style={{ color: c }}>
          {item.code}
        </span>
        <span
          className="font-mono text-[8.5px] uppercase tracking-[0.12em]"
          style={{ color: c }}
        >
          {STATUS_WORD[item.sev]}
        </span>
      </div>
      <div className="mt-1 text-[12.5px] font-bold leading-tight text-fg">
        {item.label}
      </div>
      {item.sub ? (
        <div className="mt-0.5 font-mono text-[9.5px] uppercase tracking-[0.06em] text-fg-faint">
          {item.sub}
        </div>
      ) : null}
      <div className="mt-[7px] flex flex-wrap gap-x-3 gap-y-0.5 font-mono text-[10.5px] text-fg-muted">
        <span className="font-bold" style={{ color: c }}>
          {item.pct != null ? item.pct.toFixed(1) + "% paced" : "no data yet"}
        </span>
        <span>
          {fmtMoneyShort(item.spend)} / {fmtMoneyShort(item.budget)}
        </span>
        {item.days != null && item.days > 0 ? <span>{item.days}d left</span> : null}
      </div>
    </div>
  );
}
