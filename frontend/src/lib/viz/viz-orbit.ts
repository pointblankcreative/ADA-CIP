/**
 * Orbit — the Signal instrument renderer (Signal Lab option 03, refined).
 *
 * Each campaign (or line) is a body on its own orbital shell around a
 * breathing core. Healthy bodies hold their shell; drifting ones wobble;
 * critical ones judder off-orbit with a dispersing wake; no-data bodies
 * ping like sonar. Hovering a body dilates time around it — it eases to a
 * near-stop so you can read and click it.
 *
 * Ported from the Claude Design export (app/viz/viz-orbit.js) with the
 * window globals replaced by module imports. Canvas 2D, no dependencies.
 */
import { COLORS, ink, themeBoost, type SignalItem } from "@/lib/viz/health-core";

export interface OrbitAudioLevels {
  per: number[];
  rms: number;
}

export interface OrbitFrameInput {
  t: number;
  dt: number;
  /** Per-item boot progress 0..1 (entrance choreography). */
  bootP: (i: number) => number;
  /** Shell bloom progress driver (null = fully entered). */
  ringT: number | null;
  /** Boot schedule (Lab only) — embeds pass null. */
  sched: { enter: number[]; convergeAt: number; settleAt: number } | null;
  focusId: string | null;
  hoverId: string | null;
  reduce: boolean;
  audio: OrbitAudioLevels | null;
}

export interface OrbitInstance {
  setItems: (items: SignalItem[]) => void;
  resize: () => void;
  frame: (f: OrbitFrameInput) => void;
  hitTest: (x: number, y: number) => string | null;
}

interface OrbitNode {
  it: SignalItem;
  i: number;
  rank: number;
  th: number;
  trail: Array<[number, number, number, number, number]>;
  x: number;
  y: number;
  lx: number;
  ly: number;
  slow: number;
}

function hash(n: number): number {
  const s = Math.sin(n * 127.1) * 43758.5453;
  return s - Math.floor(s);
}
const clamp01 = (v: number) => Math.max(0, Math.min(1, v));
const smooth = (u: number) => {
  u = clamp01(u);
  return u * u * (3 - 2 * u);
};

export function createOrbit(
  canvas: HTMLCanvasElement,
  opts?: { compact?: boolean; gentle?: boolean }
): OrbitInstance {
  const cm = !!opts?.compact; // compact: embedded instrument — no starfield/rim, tighter geometry
  const gentle = !!opts?.gentle; // gentle: slow, short glide-in (embeds) instead of the Lab's deep-space fall
  const g = canvas.getContext("2d") as CanvasRenderingContext2D;
  let items: SignalItem[] = [];
  let nodes: OrbitNode[] = [];
  let stars: Array<{ x: number; y: number; a: number; s: number }> = [];
  let W = 0;
  let H = 0;
  let dpr = 1;
  let lastHit: string | null = null;

  function resize() {
    dpr = Math.min(window.devicePixelRatio || 1, 2);
    W = canvas.clientWidth;
    H = canvas.clientHeight;
    canvas.width = W * dpr;
    canvas.height = H * dpr;
    g.setTransform(dpr, 0, 0, dpr, 0, 0);
    stars = [];
    for (let k = 0; k < 110; k++) {
      stars.push({
        x: hash(k * 7.3) * W,
        y: hash(k * 3.7 + 11) * H,
        a: 0.02 + hash(k * 1.9) * 0.05,
        s: hash(k * 5.1) > 0.85 ? 1.6 : 1,
      });
    }
  }

  function setItems(arr: SignalItem[]) {
    items = arr;
    const order = arr.map((it, i) => ({ it, i }));
    order.sort((a, b) => b.it.weight - a.it.weight);
    nodes = new Array(arr.length);
    order.forEach((o, rank) => {
      nodes[o.i] = {
        it: o.it,
        i: o.i,
        rank,
        th: rank * 2.39996,
        trail: [],
        x: 0,
        y: 0,
        lx: 0,
        ly: 0,
        slow: 1,
      };
    });
    lastHit = null;
  }

  /* How much of each signal's deviation is VISIBLE right now —
     0 while tuning (slightly off), snaps true at convergence,
     grows back in step with the cruise voices fading up. */
  function devFactor(t: number, sched: OrbitFrameInput["sched"]): number {
    if (!sched) return 1;
    const pull = smooth((t - sched.convergeAt) / 0.7);
    const grow = smooth((t - (sched.settleAt - 0.3)) / 2.2);
    return (
      0.35 * (1 - pull) * (t < sched.convergeAt + 0.7 ? 1 : 0) + grow
    );
  }

  function frame(f: OrbitFrameInput) {
    g.clearRect(0, 0, W, H);
    const cx = W / 2;
    const cy = H / 2;
    const t = f.t;
    const dt = Math.min(f.dt || 0.016, 0.05);
    const jr = f.reduce ? 0.35 : 1;
    const n = items.length;
    const rMin = Math.min(W, H) * (cm ? 0.18 : 0.13);
    const rMax = Math.min(W, H) * (cm ? 0.33 : 0.4);
    const rms = f.audio ? f.audio.rms : 0.3;
    const anyBad = items.some((it) => it.sev === "critical");
    const dF = devFactor(t, f.sched);
    const em = themeBoost(); // light mode: severity-coloured marks need more weight on paper

    // starfield (full-stage only)
    if (!cm) {
      g.fillStyle = ink(1);
      stars.forEach((st) => {
        g.globalAlpha = st.a;
        g.fillRect(st.x, st.y, st.s, st.s);
      });
      g.globalAlpha = 1;
    }

    /* ---- the core: the platform itself, breathing with the actual mix ---- */
    const coreA = f.ringT == null ? 1 : smooth(clamp01((f.ringT + 0.3) / 0.55));
    const coreCol = anyBad ? COLORS.critical : COLORS.ok;
    const coreR = cm ? 4.5 + rms * 4 : 9 + rms * 9;
    g.beginPath();
    g.arc(cx, cy, coreR, 0, Math.PI * 2);
    g.strokeStyle = coreCol;
    g.globalAlpha = 0.85 * coreA;
    g.lineWidth = 1.5;
    g.stroke();
    g.beginPath();
    g.arc(cx, cy, coreR * 0.45, 0, Math.PI * 2);
    g.fillStyle = coreCol;
    g.globalAlpha = (0.6 + rms * 0.4) * coreA;
    g.fill();
    // rotating instrument ring
    g.beginPath();
    g.arc(cx, cy, coreR + (cm ? 7 : 13), 0, Math.PI * 2);
    g.setLineDash([3, 7]);
    g.lineDashOffset = -t * 9;
    g.strokeStyle = ink(0.22);
    g.globalAlpha = coreA;
    g.lineWidth = 1;
    g.stroke();
    g.setLineDash([]);
    g.globalAlpha = 1;

    // convergence pulse — the chord pulling everyone true
    if (f.sched && t > f.sched.convergeAt) {
      const u = (t - f.sched.convergeAt) / 1.2;
      if (u < 1) {
        g.beginPath();
        g.arc(cx, cy, coreR + u * rMax * 1.05, 0, Math.PI * 2);
        g.strokeStyle = COLORS.ok;
        g.globalAlpha = (1 - u) * 0.3;
        g.lineWidth = 1.5;
        g.stroke();
        g.globalAlpha = 1;
      }
    }

    // rim ticks (full-stage only)
    if (!cm) {
      g.strokeStyle = ink(0.06);
      g.lineWidth = 1;
      for (let k = 0; k < 72; k++) {
        const a = (k / 72) * Math.PI * 2 + t * 0.018;
        const r1 = rMax + 16;
        const r2 = rMax + (k % 6 === 0 ? 23 : 19);
        g.beginPath();
        g.moveTo(cx + Math.cos(a) * r1, cy + Math.sin(a) * r1);
        g.lineTo(cx + Math.cos(a) * r2, cy + Math.sin(a) * r2);
        g.stroke();
      }
    }

    nodes.forEach((node) => {
      const it = node.it;
      const i = node.i;
      const shellR =
        n > 1 ? rMin + (node.rank / (n - 1)) * (rMax - rMin) : (rMin + rMax) / 2;
      const focused = f.focusId === it.id;
      const dimmed = f.focusId != null && !focused;
      const hovered = f.hoverId === it.id;
      const col = COLORS[it.sev];
      const d = Math.abs(it.dev);

      // reference shell — blooms outward from the core on embed entrance
      const ringU =
        f.ringT == null ? 1 : smooth(clamp01((f.ringT - node.rank * 0.1) / 0.8));
      if (ringU <= 0.001) {
        node.trail.length = 0;
        return;
      }
      g.beginPath();
      g.arc(cx, cy, shellR * ringU, 0, Math.PI * 2);
      g.strokeStyle = hovered || focused ? col : ink(0.045);
      g.globalAlpha = (hovered || focused ? 0.16 : 1) * ringU;
      g.lineWidth = 1;
      g.stroke();
      g.globalAlpha = 1;

      const bp = f.bootP(i);
      if (bp <= 0.001) {
        node.trail.length = 0;
        return;
      }
      const lvl = f.audio ? f.audio.per[i] : 0.5;

      /* time dilation: a watched orbit slows to a near-stop */
      const slowTarget = hovered || focused ? 0.06 : 1;
      node.slow += (slowTarget - node.slow) * Math.min(1, dt * 6);

      // angular motion — inner shells run faster, like real orbits
      const w = 0.22 * Math.sqrt(rMax / shellR) * jr;
      const judder =
        it.sev === "critical" ? 1 + Math.sin(t * 2.7 + i) * 0.5 * d * dF : 1;
      node.th += w * dt * node.slow * judder;
      let th = node.th;

      let dr = 0;
      if (it.sev === "ok") {
        dr = Math.sin(t * 1.1 + i) * 1.4 * (0.4 + lvl * 0.8);
      } else if (it.sev === "watch") {
        dr =
          (Math.sin(t * (2 + d * 3) + i) * (6 + 10 * d) * (0.55 + lvl * 0.7) +
            Math.sin(th * 2 + i) * 4 * d) *
          dF;
      } else if (it.sev === "critical") {
        const cell = Math.floor(t * 8);
        const spike =
          (hash(cell * 13.7 + i * 311) - 0.5) * (10 + 22 * d) * (0.5 + lvl) * jr;
        dr = (it.dev * 20 + Math.sin(th + node.rank) * 16 * d + spike) * dF;
      }

      // boot: bodies glide in onto their shell
      if (cm) dr *= 0.55; // compact: keep wobble inside the small frame
      let r = shellR + dr;
      if (bp < 1) {
        r += rMax * (gentle ? 0.5 : 0.85) * (1 - bp);
        th -= (1 - bp) * (gentle ? 0.85 : 1.7);
      }

      const x = cx + Math.cos(th) * r;
      const y = cy + Math.sin(th) * r;
      node.x = x;
      node.y = y;

      // trail — a true wake, left IN WORLD SPACE; points age out and
      // disperse off the orbit line as they die
      const TRAIL_AGE = 2.1;
      const lastPt = node.trail[node.trail.length - 1];
      if (!lastPt || t - lastPt[2] > 0.045) {
        const ja = hash(i * 31.7 + node.trail.length * 7.3) * Math.PI * 2;
        node.trail.push([x, y, t, Math.cos(ja), Math.sin(ja)]);
      }
      while (node.trail.length && t - node.trail[0][2] > TRAIL_AGE)
        node.trail.shift();
      if (node.trail.length > 2) {
        g.strokeStyle = col;
        g.lineCap = "round";
        let px = 0;
        let py = 0;
        for (let k = 0; k < node.trail.length; k++) {
          const pt = node.trail[k];
          const u = clamp01((t - pt[2]) / TRAIL_AGE); // 0 fresh → 1 gone
          const life = 1 - u;
          const hyp = Math.max(1, Math.hypot(pt[0] - cx, pt[1] - cy));
          const ox = (pt[0] - cx) / hyp;
          const oy = (pt[1] - cy) / hyp;
          const dsp = u * u * 9;
          const qx = pt[0] + ox * dsp * 0.6 + pt[3] * u * 4;
          const qy = pt[1] + oy * dsp * 0.6 + pt[4] * u * 4;
          if (k > 0 && life > 0.01) {
            g.beginPath();
            g.moveTo(px, py);
            g.lineTo(qx, qy);
            g.globalAlpha =
              Math.min(
                1,
                life * life * (dimmed ? 0.06 : focused ? 0.5 : 0.36) * em
              ) * bp;
            g.lineWidth = 0.3 + life * life * (focused ? 2.4 : 1.8);
            g.stroke();
          }
          px = qx;
          py = qy;
        }
        g.globalAlpha = 1;
        g.lineCap = "butt";
      }

      const s =
        (4 + it.weight * 7 + lvl * 2.5) *
        (focused || hovered ? 1.35 : 1) *
        (cm ? 0.78 : 1);

      // landing flash — exactly when this signal's note lands
      if (f.sched) {
        const tl = t - (f.sched.enter[i] + 0.88);
        if (tl > 0 && tl < 0.7) {
          g.beginPath();
          g.arc(x, y, 6 + tl * 38, 0, Math.PI * 2);
          g.strokeStyle = col;
          g.globalAlpha = (1 - tl / 0.7) * 0.55;
          g.lineWidth = 1.5;
          g.stroke();
          g.globalAlpha = 1;
        }
      }

      // nodata ping ring — sonar pulse expanding off the body
      if (it.sev === "nodata" && !dimmed) {
        const u = (t * 0.28 + i * 0.37) % 1;
        g.beginPath();
        g.arc(x, y, 4 + u * 26, 0, Math.PI * 2);
        g.globalAlpha = Math.min(1, (1 - u) * 0.35 * em) * bp;
        g.strokeStyle = col;
        g.lineWidth = 1;
        g.stroke();
        g.globalAlpha = 1;
      }

      // the body — a diamond riding its orbit, sized by budget
      g.save();
      g.translate(x, y);
      g.rotate(th + Math.PI / 4);
      g.globalAlpha = (dimmed ? 0.12 : 0.95) * bp;
      g.fillStyle = col;
      if (!dimmed) {
        g.shadowColor = col;
        g.shadowBlur = (focused || hovered ? 16 : 7) + lvl * 7;
      }
      g.fillRect(-s / 2, -s / 2, s, s);
      if (it.sev === "critical" && !dimmed) {
        g.globalAlpha = 0.22 * bp;
        g.fillRect(-s, -s, s * 2, s * 2);
      }
      g.restore();
      if (focused || hovered) {
        g.beginPath();
        g.arc(x, y, s * 1.4, 0, Math.PI * 2);
        g.strokeStyle = col;
        g.globalAlpha = 0.7 * bp;
        g.lineWidth = 1.5;
        g.stroke();
        g.globalAlpha = 1;
      }

      // label rides just outside the body
      const lOff = (cm ? 10 : 24) + s;
      const lx = cx + Math.cos(th) * (r + lOff);
      const ly = cy + Math.sin(th) * (r + lOff);
      node.lx = lx;
      node.ly = ly;
      g.globalAlpha = (dimmed ? 0.15 : hovered || focused ? 1 : 0.62) * bp;
      g.fillStyle = focused || hovered ? col : ink(0.72);
      g.font =
        (focused ? "700 " : "400 ") +
        (cm ? "8.5px" : "10px") +
        " 'Chivo Mono', monospace";
      g.textAlign = "center";
      g.textBaseline = "middle";
      g.fillText(it.code, lx, ly);
      if (it.sev !== "ok" && it.sev !== "nodata" && !dimmed && it.pct != null) {
        g.globalAlpha = 0.55 * bp;
        g.fillStyle = col;
        g.fillText(Math.round(it.pct) + "%", lx, ly + (cm ? 10 : 12));
      }
      g.globalAlpha = 1;
    });
  }

  function hitTest(x: number, y: number): string | null {
    // sticky: once you're on a body, it takes a real move to lose it
    if (lastHit) {
      const node = nodes.find((nd) => nd.it.id === lastHit);
      if (
        node &&
        (Math.hypot(x - node.x, y - node.y) < 46 ||
          Math.hypot(x - node.lx, y - node.ly) < 38)
      )
        return lastHit;
    }
    let best: string | null = null;
    let bd = 30;
    for (const node of nodes) {
      const dBody = Math.hypot(x - node.x, y - node.y);
      const dLabel = Math.hypot(x - node.lx, y - node.ly);
      const dist = Math.min(dBody, dLabel);
      if (dist < bd) {
        bd = dist;
        best = node.it.id;
      }
    }
    lastHit = best;
    return best;
  }

  return { setItems, resize, frame, hitTest };
}
