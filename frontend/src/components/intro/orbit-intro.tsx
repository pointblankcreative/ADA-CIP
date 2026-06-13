"use client";

/**
 * OrbitIntro — the Signal instrument coming online as the app's cold-load
 * splash. Grey region-dots fall in from beyond the rim and spiral onto their
 * orbits, the core pulses and the reference rings bloom, then each region pops
 * grey -> green in random order with its label sliding out of it; the wordmark
 * resolves and the curtain lifts to reveal the app.
 *
 * Phases are GATED so the splash genuinely covers load time and never reveals
 * an empty screen:
 *   - arrival starts on mount (the JS shell is live)
 *   - orbits bloom once webfonts are ready (canvas labels need Chivo Mono)
 *   - the green pops + reveal wait for `dataReady` — the app's real readiness,
 *     raised by the first page whose primary fetch resolves (see IntroProvider)
 * A hard ceiling (REVEAL_BY_MS) guarantees it always completes, even if a
 * fetch hangs or errors.
 *
 * Nods to the live Orbit instrument (lib/viz/viz-orbit) in palette and
 * geometry but does NOT depend on it. The dots are Canada's provinces and
 * territories, deliberately NOT campaign signals — a green dot here can never
 * contradict a red campaign inside the tool. Province -> orbit assignment is
 * reshuffled every load, so none is reliably innermost or outermost.
 *
 * All timings live in TUNE below — shorten/lengthen the splash from one place.
 */

import { useEffect, useRef } from "react";

type RGB = [number, number, number];
const GREEN: RGB = [202, 255, 40]; // matches health-core dark `ok` (#CAFF28)
const GREY: RGB = [150, 148, 142];
const INK = "#1a1818"; // PB black ground

const PROVINCES = [
  "BC", "AB", "SK", "MB", "ON", "QC", "NB",
  "NS", "PE", "NL", "YT", "NT", "NU",
];

const TUNE = {
  arriveBase: 0.5, // seconds a single dot takes to settle in
  arrGapMin: 0.035,
  arrGapMax: 0.1, // gap between successive arrivals (random)
  popBase: 0.36, // seconds for a single grey->green pop
  popGapMin: 0.03,
  popGapMax: 0.09,
  ringStagger: 0.04, // per-rank delay on shell bloom
  orbitHold: 0.5, // extra dwell after the last shell blooms
  revealHold: 0.45, // pause on the full wordmark before lifting
  liftDur: 0.7, // curtain lift
  fontsFallbackMs: 1200, // open `orbits` even if fonts.ready never resolves
  revealByMs: 9000, // hard ceiling: force the whole thing to completion
};

const rgba = (c: RGB, a: number) => `rgba(${c[0]},${c[1]},${c[2]},${a})`;
const lerpC = (a: RGB, b: RGB, t: number): RGB => [
  a[0] + (b[0] - a[0]) * t,
  a[1] + (b[1] - a[1]) * t,
  a[2] + (b[2] - a[2]) * t,
];
const clamp01 = (v: number) => Math.max(0, Math.min(1, v));
const smooth = (u: number) => {
  u = clamp01(u);
  return u * u * (3 - 2 * u);
};
const easeK = (p: number, k: number) => {
  p = clamp01(p);
  return 1 - Math.pow(1 - p, k);
};
const rin = (a: number, b: number) => a + Math.random() * (b - a);
const hash = (n: number) => {
  const s = Math.sin(n * 127.1) * 43758.5453;
  return s - Math.floor(s);
};
function shuffle<T>(a: T[]): T[] {
  for (let i = a.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [a[i], a[j]] = [a[j], a[i]];
  }
  return a;
}

interface INode {
  code: string;
  rank: number;
  ang0: number;
  rfrac: number;
  size: number;
  arrRel: number;
  arrExtra: number;
  arrLead: number;
  arrDur: number;
  arrK: number;
  arrWobF: number;
  arrWobA: number;
  popRel: number;
  popDur: number;
  popPulseMax: number;
  popPulseDur: number;
}

type Phase =
  | "arriving"
  | "arrived"
  | "orbiting"
  | "orbited"
  | "activating"
  | "activated"
  | "revealing"
  | "done";

export function OrbitIntro({
  dataReady,
  onFinished,
}: {
  dataReady: boolean;
  onFinished: () => void;
}) {
  const rootRef = useRef<HTMLDivElement>(null);
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const scrimRef = useRef<HTMLDivElement>(null);
  const markRef = useRef<HTMLDivElement>(null);
  const dataReadyRef = useRef(dataReady);
  dataReadyRef.current = dataReady;
  const onFinishedRef = useRef(onFinished);
  onFinishedRef.current = onFinished;

  useEffect(() => {
    const canvas = canvasRef.current;
    const root = rootRef.current;
    const scrim = scrimRef.current;
    const markEl = markRef.current;
    if (!canvas || !root) return;
    const g = canvas.getContext("2d");
    if (!g) return;

    const reduce =
      typeof window !== "undefined" &&
      window.matchMedia?.("(prefers-reduced-motion: reduce)").matches;
    const monoFamily =
      getComputedStyle(document.documentElement)
        .getPropertyValue("--font-chivo")
        .trim() || "ui-monospace, monospace";

    let W = 0;
    let H = 0;
    let dpr = 1;
    let stars: { x: number; y: number; a: number; s: number }[] = [];
    const size = () => {
      dpr = Math.min(window.devicePixelRatio || 1, 2);
      W = canvas.clientWidth;
      H = canvas.clientHeight;
      canvas.width = W * dpr;
      canvas.height = H * dpr;
      g.setTransform(dpr, 0, 0, dpr, 0, 0);
      stars = [];
      for (let k = 0; k < 110; k++)
        stars.push({
          x: hash(k * 7.3) * W,
          y: hash(k * 3.7 + 11) * H,
          a: 0.03 + hash(k * 1.9) * 0.05,
          s: hash(k * 5.1) > 0.85 ? 1.6 : 1,
        });
    };

    // build nodes: random province -> rank, golden-angle phyllotaxis
    const ranks = shuffle(PROVINCES.map((_, i) => i));
    const n = PROVINCES.length;
    const nodes: INode[] = new Array(n);
    let lastArr = 0;
    {
      const arrOrder = shuffle(PROVINCES.map((_, i) => i));
      let ca = 0.15;
      const popOrder = shuffle(PROVINCES.map((_, i) => i));
      let cp = 0.08;
      const arrRelByItem: number[] = [];
      const arrDurByItem: number[] = [];
      arrOrder.forEach((idx) => {
        arrRelByItem[idx] = ca;
        arrDurByItem[idx] = TUNE.arriveBase * rin(0.82, 1.32);
        lastArr = Math.max(lastArr, ca + arrDurByItem[idx]);
        ca += TUNE.arrGapMin + Math.random() * (TUNE.arrGapMax - TUNE.arrGapMin);
      });
      const popRelByItem: number[] = [];
      popOrder.forEach((idx) => {
        popRelByItem[idx] = cp;
        cp += TUNE.popGapMin + Math.random() * (TUNE.popGapMax - TUNE.popGapMin);
      });
      ranks.forEach((itemIdx, rank) => {
        nodes[itemIdx] = {
          code: PROVINCES[itemIdx],
          rank,
          ang0: rank * 2.39996,
          rfrac: n > 1 ? rank / (n - 1) : 0.5,
          size: rin(5, 8.5),
          arrRel: arrRelByItem[itemIdx],
          arrExtra: rin(0.5, 1.05),
          arrLead: rin(0.55, 1.7),
          arrDur: arrDurByItem[itemIdx],
          arrK: rin(2.5, 3.7),
          arrWobF: rin(2, 4.2),
          arrWobA: rin(2.5, 7),
          popRel: popRelByItem[itemIdx],
          popDur: TUNE.popBase * rin(0.78, 1.45),
          popPulseMax: rin(34, 52),
          popPulseDur: rin(0.6, 0.95),
        };
      });
    }

    let phase: Phase = "arriving";
    let fontsReady = false;
    let raf = 0;
    let t0 = performance.now() / 1000;
    let tO = 0;
    let tAct = 0;
    let tRev = 0;
    let lifted = false;
    let markShown = false;

    if (document.fonts?.ready) {
      document.fonts.ready.then(() => {
        fontsReady = true;
      });
    } else {
      fontsReady = true;
    }

    const draw = () => {
      const now = performance.now() / 1000;
      const T = now - t0;
      const elapsedMs = T * 1000;
      g.clearRect(0, 0, W, H);
      const cx = W / 2;
      const cy = H / 2;
      const rMin = Math.min(W, H) * 0.15;
      const rMax = Math.min(W, H) * 0.4;
      const globalRot = T * 0.05;

      if (!reduce) {
        g.fillStyle = "#fff";
        stars.forEach((st) => {
          g.globalAlpha = st.a * (0.6 + 0.4 * Math.sin(T * 1.3 + st.x));
          g.fillRect(st.x, st.y, st.s, st.s);
        });
        g.globalAlpha = 1;
      }

      const showCore =
        phase === "orbiting" ||
        phase === "orbited" ||
        phase === "activating" ||
        phase === "activated" ||
        phase === "revealing" ||
        phase === "done";
      const oT = showCore ? T - tO : -1;
      if (showCore) {
        const rms = 0.3 + 0.16 * Math.sin(T * 1.6);
        const coreA = smooth(clamp01((oT + 0.1) / 0.5));
        const coreR = 9 + rms * 9;
        g.beginPath();
        g.arc(cx, cy, coreR, 0, Math.PI * 2);
        g.strokeStyle = rgba(GREEN, 0.85 * coreA);
        g.lineWidth = 1.5;
        g.stroke();
        g.beginPath();
        g.arc(cx, cy, coreR * 0.45, 0, Math.PI * 2);
        g.fillStyle = rgba(GREEN, (0.6 + rms * 0.4) * coreA);
        g.fill();
        g.beginPath();
        g.arc(cx, cy, coreR + 13, 0, Math.PI * 2);
        g.setLineDash([3, 7]);
        g.lineDashOffset = -T * 9;
        g.strokeStyle = "rgba(255,255,255,0.22)";
        g.globalAlpha = coreA;
        g.lineWidth = 1;
        g.stroke();
        g.setLineDash([]);
        g.globalAlpha = 1;
        if (oT >= 0 && oT < 1.2) {
          const u = oT / 1.2;
          g.beginPath();
          g.arc(cx, cy, coreR + u * rMax * 1.1, 0, Math.PI * 2);
          g.strokeStyle = rgba(GREEN, (1 - u) * 0.32);
          g.lineWidth = 1.5;
          g.stroke();
        }
      }

      nodes.forEach((nd) => {
        const rr = rMin + nd.rfrac * (rMax - rMin);
        const ang = nd.ang0 + globalRot;

        if (showCore) {
          const ringU = smooth(clamp01((oT - nd.rank * TUNE.ringStagger) / 0.8));
          if (ringU > 0.001) {
            g.beginPath();
            g.arc(cx, cy, rr * ringU, 0, Math.PI * 2);
            g.strokeStyle = "rgba(255,255,255,0.05)";
            g.lineWidth = 1;
            g.stroke();
          }
        }

        let x: number;
        let y: number;
        let da = ang;
        let trailP = 0;
        if (phase === "arriving") {
          if (T < nd.arrRel) return;
          const p = clamp01((T - nd.arrRel) / nd.arrDur);
          const e = easeK(p, nd.arrK);
          const rEnter = rr + nd.arrExtra * rMax * (1 - e);
          const wob = Math.sin(p * Math.PI * nd.arrWobF) * nd.arrWobA * (1 - p);
          da = ang - nd.arrLead * (1 - e);
          const r = rEnter + wob;
          x = cx + Math.cos(da) * r;
          y = cy + Math.sin(da) * r;
          trailP = 1 - p;
          if (trailP > 0.02) {
            const ex = cx + Math.cos(ang - nd.arrLead) * (rr + nd.arrExtra * rMax);
            const ey = cy + Math.sin(ang - nd.arrLead) * (rr + nd.arrExtra * rMax);
            g.beginPath();
            g.moveTo(x, y);
            g.lineTo(x + (ex - x) * 0.13 * trailP, y + (ey - y) * 0.13 * trailP);
            g.strokeStyle = rgba(GREY, 0.3 * trailP);
            g.lineWidth = 1.4;
            g.lineCap = "round";
            g.stroke();
            g.lineCap = "butt";
          }
        } else {
          x = cx + Math.cos(ang) * rr;
          y = cy + Math.sin(ang) * rr + Math.sin(T * 1.1 + nd.rank) * 1.5;
        }

        const popped =
          phase === "activating" ||
          phase === "activated" ||
          phase === "revealing" ||
          phase === "done";
        const popAge = popped ? T - tAct - nd.popRel : -1;
        const popProg = popAge > 0 ? clamp01(popAge / nd.popDur) : 0;
        const col = lerpC(GREY, GREEN, popProg);
        const s = nd.size;

        if (
          (phase === "activating" || phase === "activated") &&
          popAge > 0 &&
          popAge < nd.popPulseDur
        ) {
          const pu = popAge / nd.popPulseDur;
          g.beginPath();
          g.arc(x, y, 4 + pu * nd.popPulseMax, 0, Math.PI * 2);
          g.strokeStyle = rgba(GREEN, (1 - pu) * 0.6);
          g.lineWidth = 1.5;
          g.stroke();
        }

        let bump = 1;
        if (popAge > 0 && popAge < 0.5)
          bump = 1 + 0.4 * Math.sin(clamp01(popAge / 0.3) * Math.PI);
        const s2 = s * bump;
        g.save();
        g.translate(x, y);
        g.rotate(da + Math.PI / 4);
        if (popProg > 0) {
          g.shadowColor = rgba(GREEN, 1);
          g.shadowBlur = 4 + popProg * 11;
        }
        g.fillStyle = rgba(col, 0.96);
        g.fillRect(-s2 / 2, -s2 / 2, s2, s2);
        g.restore();

        if (popAge > 0.12) {
          const lab = clamp01((popAge - 0.12) / 0.42);
          const le = easeK(lab, 2.6);
          const lr = rr + 15 + s;
          const lrx = cx + Math.cos(ang) * lr;
          const lry = cy + Math.sin(ang) * lr;
          const lx = x + (lrx - x) * le;
          const ly = y + (lry - y) * le;
          g.globalAlpha = lab;
          g.fillStyle = rgba(lerpC([255, 255, 255], GREEN, popProg), 1);
          g.font = `500 ${(9.5 + le * 0.5).toFixed(1)}px ${monoFamily}`;
          g.textAlign = "center";
          g.textBaseline = "middle";
          g.fillText(nd.code, lx, ly);
          g.globalAlpha = 1;
        }
      });

      // gate-driven transitions
      const gateOrbits = fontsReady || elapsedMs > TUNE.fontsFallbackMs;
      const gateActivate = dataReadyRef.current || elapsedMs > TUNE.revealByMs;
      if (phase === "arriving" && T > lastArr + 0.2) phase = "arrived";
      if (phase === "arrived" && gateOrbits) {
        tO = T;
        phase = "orbiting";
      }
      if (phase === "orbiting" && oT > n * TUNE.ringStagger + TUNE.orbitHold)
        phase = "orbited";
      if (phase === "orbited" && gateActivate) {
        tAct = T;
        phase = "activating";
      }
      if (phase === "activating") {
        const allDone = nodes.every((nd) => T - tAct - nd.popRel >= nd.popDur + 0.05);
        if (allDone) phase = "activated";
      }
      if (phase === "activated") {
        tRev = T;
        phase = "revealing";
      }
      if (phase === "revealing") {
        if (!markShown && scrim && markEl) {
          markShown = true;
          scrim.style.opacity = "1";
          markEl.style.opacity = "1";
        }
        const rT = T - tRev;
        if (rT >= TUNE.revealHold && !lifted) {
          lifted = true;
          root.style.transition = `transform ${TUNE.liftDur}s cubic-bezier(0.76,0,0.24,1)`;
          root.style.transform = "translateY(-103%)";
        }
        if (rT >= TUNE.revealHold + TUNE.liftDur + 0.1) {
          phase = "done";
          onFinishedRef.current();
          return; // stop the loop; component will unmount
        }
      }

      raf = requestAnimationFrame(draw);
    };

    size();
    const onResize = () => size();
    window.addEventListener("resize", onResize);
    t0 = performance.now() / 1000;
    raf = requestAnimationFrame(draw);

    return () => {
      cancelAnimationFrame(raf);
      window.removeEventListener("resize", onResize);
    };
  }, []);

  return (
    <div
      ref={rootRef}
      role="status"
      aria-live="polite"
      style={{
        position: "fixed",
        inset: 0,
        zIndex: 2147483646,
        background: INK,
        overflow: "hidden",
      }}
    >
      <span
        style={{
          position: "absolute",
          width: 1,
          height: 1,
          overflow: "hidden",
          clip: "rect(0 0 0 0)",
        }}
      >
        Loading ADA Campaign Intelligence
      </span>
      <canvas
        ref={canvasRef}
        style={{ position: "absolute", inset: 0, width: "100%", height: "100%" }}
      />
      <div
        ref={scrimRef}
        style={{
          position: "absolute",
          inset: 0,
          opacity: 0,
          transition: "opacity .6s ease",
          background:
            "radial-gradient(circle at 50% 50%, rgba(26,24,24,0.55) 0%, rgba(26,24,24,0) 55%)",
        }}
      />
      <div
        ref={markRef}
        aria-hidden="true"
        style={{
          position: "absolute",
          inset: 0,
          display: "flex",
          flexDirection: "column",
          alignItems: "center",
          justifyContent: "center",
          textAlign: "center",
          color: "#f2f2f2",
          opacity: 0,
          transition: "opacity .5s ease",
          pointerEvents: "none",
        }}
      >
        <div
          style={{
            fontFamily: "var(--font-display)",
            fontSize: 56,
            lineHeight: 1,
            letterSpacing: "0.01em",
            textShadow: "0 0 24px rgba(26,24,24,0.7)",
          }}
        >
          ADA
        </div>
        <div
          style={{
            fontFamily: "var(--font-mono)",
            fontSize: 10,
            letterSpacing: "0.28em",
            color: "rgba(242,242,242,0.6)",
            marginTop: 10,
            textTransform: "uppercase",
          }}
        >
          Campaign Intelligence
        </div>
      </div>
    </div>
  );
}
