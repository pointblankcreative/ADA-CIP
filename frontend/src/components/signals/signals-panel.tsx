"use client";

/**
 * SignalsPanel — the full-width Orbit instrument on the Flightdeck.
 * Every active campaign orbits the platform core: healthy bodies hold
 * their shell, drifters wobble, critical ones judder off-orbit. Hover a
 * body to read it (and light up its flight row below); click to open it.
 * Sound is opt-in via the Listen button and always turns itself off.
 */
import { useEffect, useMemo, useRef, useState } from "react";
import type { Project } from "@/lib/api";
import { HealthAudio } from "@/lib/viz/audio-engine";
import { campaignSignalItems } from "@/lib/viz/health-core";
import {
  SignalSoundButton,
  SignalTooltip,
  useOptInSound,
  useOrbitInstrument,
} from "@/components/signals/instrument";
import { Label } from "@/components/ui";
import { cn } from "@/lib/utils";

export function SignalsPanel({
  projects,
  onOpen,
  onHover,
}: {
  /** Active campaigns (the Flightdeck's active list). */
  projects: Project[];
  onOpen: (code: string) => void;
  /** Bubbles the hovered campaign code so the board can glow its row. */
  onHover?: (code: string | null) => void;
}) {
  const items = useMemo(() => campaignSignalItems(projects), [projects]);
  const itemsRef = useRef(items);
  itemsRef.current = items;
  const audioRef = useRef<HealthAudio | null>(null);
  if (!audioRef.current) audioRef.current = new HealthAudio();
  const safeAudioRef = audioRef as React.MutableRefObject<HealthAudio>;
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const [hoverId, setHoverId] = useState<string | null>(null);
  const [pos, setPos] = useState<{ x: number; y: number } | null>(null);
  const [bounds, setBounds] = useState({ w: 0, h: 0 });
  const hoverRef = useRef<string | null>(null);
  hoverRef.current = hoverId;
  const vizRef = useOrbitInstrument(canvasRef, itemsRef, hoverRef, safeAudioRef, false);
  const { soundOn, toggle, secsLeft } = useOptInSound(safeAudioRef, itemsRef, 30);

  // hovering a body solos its voice — the stethoscope
  useEffect(() => {
    safeAudioRef.current.solo(soundOn ? hoverId : null);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [hoverId, soundOn]);
  // …and lights up its flight row below
  useEffect(() => {
    onHover?.(hoverId);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [hoverId]);

  const onMove = (e: React.MouseEvent<HTMLCanvasElement>) => {
    const canvas = canvasRef.current;
    const viz = vizRef.current;
    if (!canvas || !viz) return;
    const r = canvas.getBoundingClientRect();
    const x = e.clientX - r.left;
    const y = e.clientY - r.top;
    setHoverId(viz.hitTest(x, y));
    setPos({ x, y });
    setBounds({ w: r.width, h: r.height });
  };
  const onClick = (e: React.MouseEvent<HTMLCanvasElement>) => {
    const canvas = canvasRef.current;
    const viz = vizRef.current;
    if (!canvas || !viz) return;
    const r = canvas.getBoundingClientRect();
    const id = viz.hitTest(e.clientX - r.left, e.clientY - r.top);
    const it = items.find((x) => x.id === id);
    if (it) onOpen(it.code);
  };

  const flags = items.filter((i) => i.sev === "critical").length;
  const drifts = items.filter((i) => i.sev === "watch").length;
  const summary =
    [flags && flags + " off pace", drifts && drifts + " drifting"]
      .filter(Boolean)
      .join(" · ") || "All on pace";
  const hov = items.find((i) => i.id === hoverId);

  if (items.length === 0) return null;

  return (
    <div className="mt-7 overflow-hidden rounded-md border-2 border-line-soft bg-surface-sunken">
      <div className="flex items-center gap-3.5 border-b-2 border-line-soft px-4 py-[11px]">
        <Label className="text-xs text-fg-secondary">Signals</Label>
        <span
          className={cn(
            "font-mono text-[10.5px] uppercase tracking-[0.08em]",
            flags ? "text-danger" : drifts ? "text-warn" : "text-accent-ink"
          )}
        >
          {summary}
        </span>
        <span className="ml-auto truncate font-mono text-[10.5px] text-fg-faint">
          {hov
            ? `${hov.code} · ${hov.label}${hov.pct != null ? ` · ${hov.pct.toFixed(1)}%` : " · no data"}`
            : "Hover to read · click to open"}
        </span>
        <SignalSoundButton soundOn={soundOn} toggle={toggle} secsLeft={secsLeft} />
      </div>
      <div className="relative">
        <canvas
          ref={canvasRef}
          onMouseMove={onMove}
          onMouseLeave={() => setHoverId(null)}
          onClick={onClick}
          className="block w-full"
          style={{ height: 380, cursor: hoverId ? "pointer" : "default" }}
        />
        <SignalTooltip item={hov} pos={pos} bounds={bounds} />
      </div>
    </div>
  );
}
