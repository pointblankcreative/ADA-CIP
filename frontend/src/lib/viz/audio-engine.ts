/**
 * Signal audio engine — strictly opt-in sound for the Orbit instruments.
 *
 * The platform calibrates on boot (each signal finds its frequency),
 * settles into a low cruise, and unhealthy signals audibly waver/rattle.
 * Pure WebAudio synthesis — no samples. Everything is quiet by design,
 * and the embeds only ever start it from an explicit user click.
 *
 * VOICINGS — the same health grammar spoken in different voices
 * (tuneup, heatbug, cricket, moose, bullfrog, peeper). In every voicing:
 * ok = clean/regular · watch = beating/irregular · critical = wobble +
 * rattle/growl · nodata = a lonely ping.
 *
 * Ported from the Claude Design export (app/viz/audio-engine.js).
 */
import type { SignalItem } from "@/lib/viz/health-core";

const RATIOS = [1, 2, 1.5, 3, 2.5, 4, 3, 5, 4.5, 6, 2.25, 8];

interface Voicing {
  label: string;
  root: number;
  glide: number;
  attack: number;
  air: [number, number, number];
  desc: string;
  pop?: "tick" | "chirp" | "gulp";
  centers: (sev: string, f: number) => number[];
}

export const VOICINGS: Record<string, Voicing> = {
  tuneup: {
    label: "Hum", root: 110, glide: 0.8, attack: 0.22, air: [220, 900, 260],
    desc: "The original — every signal settles into a low, steady hum.",
    centers: (sev, f) => (sev === "critical" ? [f, f * 1.5, 2100] : sev === "nodata" ? [720] : [f]),
  },
  heatbug: {
    label: "Heat bug", root: 196, glide: 0.5, attack: 0.16, air: [600, 2600, 700], pop: "tick",
    desc: "Crackly and buzzing — cicadas in the hydro lines, an Ontario July.",
    centers: (sev, f) => (sev === "critical" ? [f * 1.5, 3800] : sev === "nodata" ? [5200] : [f * 2, f]),
  },
  cricket: {
    label: "Cricket", root: 294, glide: 0.4, attack: 0.14, air: [400, 1800, 500], pop: "chirp",
    desc: "Poppy — dusk at the cottage. Healthy signals chirp on time; trouble trills.",
    centers: (sev, f) => (sev === "critical" ? [f * 1.5] : sev === "nodata" ? [5200] : [f * 2]),
  },
  moose: {
    label: "Moose", root: 49, glide: 1.8, attack: 0.6, air: [90, 340, 110],
    desc: "Low and slow — a bellow across the bog. Trouble growls and knocks.",
    centers: (sev, f) => (sev === "critical" ? [f, 300] : sev === "nodata" ? [130] : [f]),
  },
  bullfrog: {
    label: "Bullfrog", root: 65.4, glide: 1.2, attack: 0.35, air: [120, 460, 140], pop: "gulp",
    desc: "Low and round — the pond at night. Croaks keep the time; trouble drones.",
    centers: (sev, f) => (sev === "critical" ? [f, 230] : sev === "nodata" ? [110] : [f]),
  },
  peeper: {
    label: "Peeper", root: 98, glide: 0.6, attack: 0.14, air: [320, 1100, 380], pop: "gulp",
    desc: "Marsh frog — round, warm peeps across the water.",
    centers: (sev, f) => (sev === "critical" ? [f * 1.5, f] : sev === "nodata" ? [2200] : [f * 1.5]),
  },
};

export interface AudioLevels {
  per: number[];
  rms: number;
  wave: Float32Array;
}

export interface BootSchedule {
  preroll: number;
  enter: number[];
  convergeAt: number;
  settleAt: number;
}

interface Voice {
  mix: GainNode;
  timers: Array<() => void>;
  stops: Array<OscillatorNode | AudioBufferSourceNode>;
}

export class HealthAudio {
  private ctx: AudioContext | null = null;
  private master: GainNode | null = null;
  private comp: DynamicsCompressorNode | null = null;
  private bedGroup: GainNode | null = null;
  private voiceGroup: GainNode | null = null;
  private tap: GainNode | null = null;
  private analyser: AnalyserNode | null = null;
  private freqBuf: Float32Array<ArrayBuffer> | null = null;
  private timeBuf: Float32Array<ArrayBuffer> | null = null;
  private vizPeak = 1e-4;
  private wavePeak = 0.02;
  private bootNodes: Array<{ stop: () => void }> = [];
  private bedNodes: Array<AudioNode & { stop?: (when?: number) => void }> = [];
  private bedTimers: Array<() => void> = [];
  private voices = new Map<string, Voice>();
  private volume = 0.65;
  private muted = false;
  private soloId: string | null = null;
  private cruising = false;
  private voicingId = "tuneup";
  private lastItems: SignalItem[] = [];

  private V(): Voicing {
    return VOICINGS[this.voicingId];
  }
  private freqFor(i: number): number {
    return this.V().root * RATIOS[i % RATIOS.length];
  }
  private rnd(a: number, b: number): number {
    return a + Math.random() * (b - a);
  }

  private ensureCtx() {
    if (this.ctx) return;
    const Ctor =
      window.AudioContext ??
      (window as unknown as { webkitAudioContext: typeof AudioContext })
        .webkitAudioContext;
    const ctx = new Ctor();
    this.ctx = ctx;
    this.comp = ctx.createDynamicsCompressor();
    this.comp.threshold.value = -22;
    this.comp.ratio.value = 8;
    this.master = ctx.createGain();
    this.master.gain.value = this.muted ? 0 : this.volume;
    this.master.connect(this.comp);
    this.comp.connect(ctx.destination);
    this.bedGroup = ctx.createGain();
    this.bedGroup.gain.value = 1;
    this.bedGroup.connect(this.master);
    this.voiceGroup = ctx.createGain();
    this.voiceGroup.gain.value = 1;
    this.voiceGroup.connect(this.master);
    // pre-master tap → analyser: visuals read the true mix, independent of mute & volume
    this.tap = ctx.createGain();
    this.tap.gain.value = 1;
    this.analyser = ctx.createAnalyser();
    this.analyser.fftSize = 4096;
    this.analyser.smoothingTimeConstant = 0.7;
    this.tap.connect(this.analyser);
    this.bedGroup.connect(this.tap);
    this.voiceGroup.connect(this.tap);
  }

  private noiseBuffer(seconds: number, brown: boolean): AudioBuffer {
    const ctx = this.ctx!;
    const len = Math.floor(ctx.sampleRate * seconds);
    const buf = ctx.createBuffer(1, len, ctx.sampleRate);
    const d = buf.getChannelData(0);
    let last = 0;
    for (let i = 0; i < len; i++) {
      const w = Math.random() * 2 - 1;
      if (brown) {
        last = (last + 0.02 * w) / 1.02;
        d[i] = last * 3.5;
      } else d[i] = w;
    }
    return buf;
  }

  /* ---------- tiny builders ---------- */
  private mkOsc(type: OscillatorType, f: number): OscillatorNode {
    const o = this.ctx!.createOscillator();
    o.type = type;
    o.frequency.value = f;
    return o;
  }
  private mkGain(v: number): GainNode {
    const g = this.ctx!.createGain();
    g.gain.value = v;
    return g;
  }
  private mkFilter(type: BiquadFilterType, f: number, q?: number): BiquadFilterNode {
    const x = this.ctx!.createBiquadFilter();
    x.type = type;
    x.frequency.value = f;
    if (q != null) x.Q.value = q;
    return x;
  }

  // one-shot tone blip, optional pitch glide + amplitude buzz (AM)
  private blip(
    dest: AudioNode,
    {
      type = "sine" as OscillatorType,
      f0,
      f1,
      dur = 0.2,
      peak = 0.01,
      at,
      attack = 0.012,
      amHz = 0,
      hold = 0,
      lp = 0,
      lpQ = 0.7,
    }: {
      type?: OscillatorType;
      f0: number;
      f1?: number;
      dur?: number;
      peak?: number;
      at?: number;
      attack?: number;
      amHz?: number;
      hold?: number;
      lp?: number;
      lpQ?: number;
    }
  ) {
    const ctx = this.ctx!;
    const t = at != null ? at : ctx.currentTime;
    const o = this.mkOsc(type, f0);
    if (f1 && f1 !== f0) {
      o.frequency.setValueAtTime(f0, t);
      o.frequency.exponentialRampToValueAtTime(f1, t + dur);
    }
    const g = this.mkGain(0.0001);
    g.gain.setValueAtTime(0.0001, t);
    g.gain.exponentialRampToValueAtTime(peak, t + attack);
    if (hold > 0) g.gain.setValueAtTime(peak, t + attack + hold);
    g.gain.exponentialRampToValueAtTime(0.0001, t + dur);
    o.connect(g);
    if (lp > 0) {
      const flt = this.mkFilter("lowpass", lp, lpQ); // warmth: roll the crisp highs off
      g.connect(flt);
      flt.connect(dest);
    } else g.connect(dest);
    o.start(t);
    o.stop(t + dur + 0.06);
    if (amHz > 0) {
      // the croak/buzz: square-wave tremolo riding the envelope
      const am = this.mkOsc("square", amHz);
      const ag = this.mkGain(peak * 0.8);
      am.connect(ag);
      ag.connect(g.gain);
      am.start(t);
      am.stop(t + dur + 0.06);
    }
  }

  // one-shot filtered-noise pop / crackle
  private pop(
    dest: AudioNode,
    { f = 3000, q = 1.5, dur = 0.03, peak = 0.01, at }: { f?: number; q?: number; dur?: number; peak?: number; at?: number }
  ) {
    const ctx = this.ctx!;
    const t = at != null ? at : ctx.currentTime;
    const n = ctx.createBufferSource();
    n.buffer = this.noiseBuffer(Math.max(0.05, dur + 0.02), false);
    const flt = this.mkFilter("bandpass", f, q);
    const g = this.mkGain(0.0001);
    g.gain.setValueAtTime(0.0001, t);
    g.gain.exponentialRampToValueAtTime(peak, t + 0.004);
    g.gain.exponentialRampToValueAtTime(0.0001, t + dur);
    n.connect(flt);
    flt.connect(g);
    g.connect(dest);
    n.start(t);
    n.stop(t + dur + 0.06);
  }

  // self-rescheduling random-interval timer; pushes a canceller into `list`
  private every(
    list: Array<() => void>,
    fn: () => void,
    minMs: number,
    maxMs: number,
    firstMs?: number
  ) {
    let id = setTimeout(
      () => {
        const run = () => {
          if (!this.ctx || this.ctx.state === "closed") return;
          fn();
          id = setTimeout(run, this.rnd(minMs, maxMs));
        };
        run();
      },
      firstMs != null ? firstMs : this.rnd(minMs, maxMs)
    );
    list.push(() => clearTimeout(id));
  }

  // sustained buzzing partial: saw → bandpass, square-wave AM (the cicada cell)
  private buzz(
    v: Voice,
    dest: AudioNode,
    fr: number,
    {
      amHz,
      base,
      depth = 0.6,
      bpMul = 2,
      q = 2.2,
      ramp = 0.7,
    }: { amHz: number; base: number; depth?: number; bpMul?: number; q?: number; ramp?: number }
  ): OscillatorNode {
    const o = this.mkOsc("sawtooth", fr);
    const bp = this.mkFilter("bandpass", fr * bpMul, q);
    const g = this.mkGain(0.0001);
    g.gain.setTargetAtTime(base, this.ctx!.currentTime, ramp);
    const lfo = this.mkOsc("square", amHz);
    const lg = this.mkGain(base * depth);
    lfo.connect(lg);
    lg.connect(g.gain);
    o.connect(bp);
    bp.connect(g);
    g.connect(dest);
    o.start();
    lfo.start();
    v.stops.push(o, lfo);
    return o;
  }

  /* ---------- public surface ---------- */

  engage() {
    this.ensureCtx();
    if (this.ctx!.state === "suspended") void this.ctx!.resume();
  }

  cruise(items: SignalItem[]) {
    this.ensureCtx();
    if (this.cruising) return;
    this.cruising = true;
    this.lastItems = items;
    this.makeBed(this.ctx!.currentTime);
    items.forEach((it, i) => this.makeVoice(it, i));
    this.applySolo(0.05);
  }

  refresh(items: SignalItem[]) {
    if (!this.ctx) return;
    this.lastItems = items;
    const wasCruising = this.cruising;
    this.stopVoices();
    if (wasCruising) items.forEach((it, i) => this.makeVoice(it, i));
    this.applySolo(0.1);
  }

  solo(id: string | null) {
    this.soloId = id;
    this.applySolo();
  }

  setVoicing(id: string) {
    if (!VOICINGS[id] || id === this.voicingId) return;
    this.voicingId = id;
    this.vizPeak = 1e-4;
    this.wavePeak = 0.02; // re-find levels at the new register
    if (this.ctx && this.cruising) {
      // swap the whole soundscape in place
      this.stopAll();
      this.cruise(this.lastItems);
    }
  }

  get voicing(): string {
    return this.voicingId;
  }

  setVolume(v: number) {
    this.volume = v;
    if (this.master && !this.muted)
      this.master.gain.setTargetAtTime(v, this.ctx!.currentTime, 0.05);
  }

  setMuted(b: boolean, withClick?: boolean) {
    this.muted = b;
    if (!this.ctx) return;
    if (withClick) this.tapeClick(!b);
    this.master!.gain.cancelScheduledValues(this.ctx.currentTime);
    this.master!.gain.setTargetAtTime(
      b ? 0 : this.volume,
      this.ctx.currentTime,
      b ? 0.045 : 0.3
    );
  }

  get running(): boolean {
    return !!this.ctx;
  }

  stopAll = () => {
    this.cruising = false;
    this.stopVoices();
    this.bootNodes.forEach((n) => n.stop && n.stop());
    this.bootNodes = [];
    this.bedTimers.forEach((c) => c());
    this.bedTimers = [];
    this.bedNodes.forEach((n) => {
      try {
        n.stop?.();
      } catch {
        /* already stopped */
      }
      try {
        n.disconnect();
      } catch {
        /* already disconnected */
      }
    });
    this.bedNodes = [];
  };

  /* ---------- cruise beds — one per voicing ---------- */
  private makeBed(now: number) {
    const ctx = this.ctx!;
    const bedGroup = this.bedGroup!;
    const keep = (...nodes: Array<AudioNode & { stop?: (when?: number) => void }>) =>
      nodes.forEach((n) => this.bedNodes.push(n));

    if (this.voicingId === "tuneup") {
      const hum = ctx.createBufferSource();
      hum.buffer = this.noiseBuffer(3, true);
      hum.loop = true;
      const humF = this.mkFilter("lowpass", 150, 0.4);
      const humLFO = this.mkOsc("sine", 0.07);
      const humLFOg = this.mkGain(35);
      humLFO.connect(humLFOg);
      humLFOg.connect(humF.frequency);
      const humG = this.mkGain(0.0001);
      humG.gain.exponentialRampToValueAtTime(0.11, now + 2.8);
      hum.connect(humF);
      humF.connect(humG);
      humG.connect(bedGroup);
      hum.start(now);
      humLFO.start(now);
      const sub = this.mkOsc("sine", 55);
      const subG = this.mkGain(0.0001);
      subG.gain.exponentialRampToValueAtTime(0.045, now + 2.8);
      sub.connect(subG);
      subG.connect(bedGroup);
      sub.start(now);
      const fifth = this.mkOsc("sine", 82.5);
      const fifthG = this.mkGain(0.0001);
      fifthG.gain.exponentialRampToValueAtTime(0.014, now + 3.2);
      fifth.connect(fifthG);
      fifthG.connect(bedGroup);
      fifth.start(now);
      keep(hum, humLFO, sub, fifth, humG, subG, fifthG);
    } else if (this.voicingId === "heatbug") {
      const sh = ctx.createBufferSource();
      sh.buffer = this.noiseBuffer(3, false);
      sh.loop = true;
      const shF = this.mkFilter("bandpass", 4800, 0.8);
      const shG = this.mkGain(0.0001);
      shG.gain.setTargetAtTime(0.0045, now, 1.5);
      const shLFO = this.mkOsc("sine", 0.11);
      const shLG = this.mkGain(0.0021);
      shLFO.connect(shLG);
      shLG.connect(shG.gain);
      sh.connect(shF);
      shF.connect(shG);
      shG.connect(bedGroup);
      sh.start(now);
      shLFO.start(now);
      const grd = this.mkOsc("sine", 98);
      const grdG = this.mkGain(0.0001);
      grdG.gain.setTargetAtTime(0.016, now, 1.5);
      grd.connect(grdG);
      grdG.connect(bedGroup);
      grd.start(now);
      keep(sh, shLFO, grd, shG, grdG);
      this.every(
        this.bedTimers,
        () =>
          this.pop(bedGroup, {
            f: this.rnd(2200, 5200),
            q: this.rnd(1, 3),
            dur: this.rnd(0.012, 0.035),
            peak: this.rnd(0.002, 0.008),
          }),
        90,
        420,
        300
      );
    } else if (this.voicingId === "cricket") {
      const night = ctx.createBufferSource();
      night.buffer = this.noiseBuffer(3, true);
      night.loop = true;
      const nF = this.mkFilter("lowpass", 520, 0.5);
      const nLFO = this.mkOsc("sine", 0.07);
      const nLG = this.mkGain(120);
      nLFO.connect(nLG);
      nLG.connect(nF.frequency);
      const nG = this.mkGain(0.0001);
      nG.gain.setTargetAtTime(0.05, now, 1.6);
      night.connect(nF);
      nF.connect(nG);
      nG.connect(bedGroup);
      night.start(now);
      nLFO.start(now);
      const sh = ctx.createBufferSource();
      sh.buffer = this.noiseBuffer(3, false);
      sh.loop = true;
      const shF = this.mkFilter("bandpass", 3600, 1.2);
      const shG = this.mkGain(0.0001);
      shG.gain.setTargetAtTime(0.002, now, 2);
      sh.connect(shF);
      shF.connect(shG);
      shG.connect(bedGroup);
      sh.start(now);
      keep(night, nLFO, sh, nG, shG);
      this.every(
        this.bedTimers,
        () =>
          this.blip(bedGroup, {
            f0: this.rnd(2800, 4200),
            dur: 0.05,
            peak: this.rnd(0.001, 0.0035),
          }),
        900,
        3200,
        1200
      );
    } else if (this.voicingId === "moose") {
      const bog = ctx.createBufferSource();
      bog.buffer = this.noiseBuffer(3, true);
      bog.loop = true;
      const bF = this.mkFilter("lowpass", 80, 0.4);
      const bLFO = this.mkOsc("sine", 0.05);
      const bLG = this.mkGain(18);
      bLFO.connect(bLG);
      bLG.connect(bF.frequency);
      const bG = this.mkGain(0.0001);
      bG.gain.setTargetAtTime(0.12, now, 2.2);
      bog.connect(bF);
      bF.connect(bG);
      bG.connect(bedGroup);
      bog.start(now);
      bLFO.start(now);
      const sub = this.mkOsc("sine", 36.75);
      const subG = this.mkGain(0.0001);
      subG.gain.setTargetAtTime(0.05, now, 2.2);
      sub.connect(subG);
      subG.connect(bedGroup);
      sub.start(now);
      const harm = this.mkOsc("sine", 73.5);
      const hG = this.mkGain(0.0001);
      hG.gain.setTargetAtTime(0.013, now, 2.6);
      harm.connect(hG);
      hG.connect(bedGroup);
      harm.start(now);
      keep(bog, bLFO, sub, harm, bG, subG, hG);
      this.every(
        this.bedTimers,
        () => this.blip(bedGroup, { f0: 110, f1: 62, dur: 2.4, peak: 0.007, attack: 0.6 }),
        11000,
        20000,
        8000
      );
    } else if (this.voicingId === "bullfrog") {
      const pond = ctx.createBufferSource();
      pond.buffer = this.noiseBuffer(3, true);
      pond.loop = true;
      const pF = this.mkFilter("lowpass", 150, 0.5);
      const pLFO = this.mkOsc("sine", 0.06);
      const pLG = this.mkGain(40);
      pLFO.connect(pLG);
      pLG.connect(pF.frequency);
      const pG = this.mkGain(0.0001);
      pG.gain.setTargetAtTime(0.07, now, 1.8);
      pond.connect(pF);
      pF.connect(pG);
      pG.connect(bedGroup);
      pond.start(now);
      pLFO.start(now);
      const sub = this.mkOsc("sine", 49);
      const subG = this.mkGain(0.0001);
      subG.gain.setTargetAtTime(0.02, now, 2);
      sub.connect(subG);
      subG.connect(bedGroup);
      sub.start(now);
      const sh = ctx.createBufferSource();
      sh.buffer = this.noiseBuffer(3, false);
      sh.loop = true;
      const shF = this.mkFilter("bandpass", 4200, 1.5);
      const shG = this.mkGain(0.0001);
      shG.gain.setTargetAtTime(0.0012, now, 2.5);
      sh.connect(shF);
      shF.connect(shG);
      shG.connect(bedGroup);
      sh.start(now);
      keep(pond, pLFO, sub, sh, pG, subG, shG);
      this.every(
        this.bedTimers,
        () =>
          this.blip(bedGroup, {
            f0: this.rnd(180, 260),
            f1: this.rnd(70, 110),
            dur: 0.22,
            peak: 0.0035,
          }),
        3500,
        9000,
        2500
      );
    } else if (this.voicingId === "peeper") {
      const grd = ctx.createBufferSource();
      grd.buffer = this.noiseBuffer(3, true);
      grd.loop = true;
      const gF = this.mkFilter("lowpass", 320, 0.5);
      const gLFO = this.mkOsc("sine", 0.06);
      const gLG = this.mkGain(80);
      gLFO.connect(gLG);
      gLG.connect(gF.frequency);
      const gG = this.mkGain(0.0001);
      gG.gain.setTargetAtTime(0.038, now, 1.6);
      grd.connect(gF);
      gF.connect(gG);
      gG.connect(bedGroup);
      grd.start(now);
      gLFO.start(now);
      const sh = ctx.createBufferSource();
      sh.buffer = this.noiseBuffer(3, false);
      sh.loop = true;
      const shF = this.mkFilter("bandpass", 2200, 1.0);
      const shG = this.mkGain(0.0001);
      shG.gain.setTargetAtTime(0.002, now, 2);
      const shLFO = this.mkOsc("sine", 0.13);
      const shLG = this.mkGain(0.0011);
      shLFO.connect(shLG);
      shLG.connect(shG.gain);
      sh.connect(shF);
      shF.connect(shG);
      shG.connect(bedGroup);
      sh.start(now);
      shLFO.start(now);
      keep(grd, gLFO, sh, shLFO, gG, shG);
      this.every(
        this.bedTimers,
        () => {
          const fr = this.rnd(420, 720);
          this.blip(bedGroup, {
            type: "triangle",
            f0: fr,
            f1: fr * 1.15,
            dur: 0.08,
            peak: this.rnd(0.0014, 0.0036),
            attack: 0.025,
            lp: fr * 2.2,
            lpQ: 0.6,
          });
        },
        1400,
        4200,
        1000
      );
    }
  }

  /* ---------- per-signal cruise voices, per voicing ---------- */
  private makeVoice(it: SignalItem, i: number) {
    const ctx = this.ctx!;
    const f = this.freqFor(i);
    const mix = this.mkGain(1);
    mix.connect(this.voiceGroup!);
    const v: Voice = { mix, timers: [], stops: [] };
    const now = ctx.currentTime;
    const d = Math.abs(it.dev);

    if (this.voicingId === "tuneup") {
      if (it.sev === "ok") {
        const o = this.mkOsc("sine", f);
        const g = this.mkGain(0.0001);
        g.gain.exponentialRampToValueAtTime(0.005, now + 1.2);
        o.connect(g);
        g.connect(mix);
        o.start(now);
        v.stops.push(o);
      } else if (it.sev === "watch") {
        const detune = 1.006 + d * 0.01;
        [f, f * detune].forEach((freq) => {
          const o = this.mkOsc("sine", freq);
          const g = this.mkGain(0.0001);
          g.gain.exponentialRampToValueAtTime(0.0085, now + 1.2);
          o.connect(g);
          g.connect(mix);
          o.start(now);
          v.stops.push(o);
        });
      } else if (it.sev === "critical") {
        const o = this.mkOsc("sawtooth", f);
        const wob = this.mkOsc("sine", 4.2 + d * 2.5);
        const wobG = this.mkGain(14 + d * 26);
        wob.connect(wobG);
        wobG.connect(o.detune);
        const bp = this.mkFilter("bandpass", f * 1.5, 7);
        const g = this.mkGain(0.0001);
        g.gain.exponentialRampToValueAtTime(0.021, now + 1.2);
        o.connect(bp);
        bp.connect(g);
        g.connect(mix);
        o.start(now);
        wob.start(now);
        v.stops.push(o, wob);
        const n = ctx.createBufferSource();
        n.buffer = this.noiseBuffer(1.5, false);
        n.loop = true;
        const nf = this.mkFilter("bandpass", 2100, 5);
        const gate = this.mkGain(0);
        const gateLFO = this.mkOsc("square", 5.5 + d * 4);
        const gateAmt = this.mkGain(0.0075);
        gateLFO.connect(gateAmt);
        gateAmt.connect(gate.gain);
        n.connect(nf);
        nf.connect(gate);
        gate.connect(mix);
        n.start(now);
        gateLFO.start(now);
        v.stops.push(n, gateLFO);
      } else {
        this.every(
          v.timers,
          () => this.blip(mix, { f0: 720, dur: 0.55, peak: 0.011, attack: 0.04 }),
          3400,
          3800,
          800
        );
      }
    } else if (this.voicingId === "heatbug") {
      if (it.sev === "ok") {
        this.buzz(v, mix, f, { amHz: 24 + (i % 5) * 3, base: 0.003, depth: 0.55 });
      } else if (it.sev === "watch") {
        const detune = 1.007 + d * 0.01;
        this.buzz(v, mix, f, { amHz: 19, base: 0.0044, depth: 0.7 });
        this.buzz(v, mix, f * detune, { amHz: 19, base: 0.0044, depth: 0.7 });
      } else if (it.sev === "critical") {
        const o = this.buzz(v, mix, f, {
          amHz: 54 + d * 18,
          base: 0.011,
          depth: 0.95,
          bpMul: 1.5,
          q: 6,
        });
        const wob = this.mkOsc("sine", 5);
        const wobG = this.mkGain(18 + d * 30);
        wob.connect(wobG);
        wobG.connect(o.detune);
        wob.start();
        v.stops.push(wob);
        const n = ctx.createBufferSource();
        n.buffer = this.noiseBuffer(1.5, false);
        n.loop = true;
        const nf = this.mkFilter("bandpass", 3800, 5);
        const gate = this.mkGain(0);
        const gateLFO = this.mkOsc("square", 9 + d * 5);
        const gateAmt = this.mkGain(0.005);
        gateLFO.connect(gateAmt);
        gateAmt.connect(gate.gain);
        n.connect(nf);
        nf.connect(gate);
        gate.connect(mix);
        n.start(now);
        gateLFO.start(now);
        v.stops.push(n, gateLFO);
      } else {
        this.every(
          v.timers,
          () => this.pop(mix, { f: 5200, q: 3, dur: 0.02, peak: 0.005 }),
          3000,
          4500,
          900
        );
      }
    } else if (this.voicingId === "cricket") {
      const chirp = (fr: number, count: number, spacing: number, peak: number) => {
        const t0 = ctx.currentTime;
        for (let k = 0; k < count; k++)
          this.blip(mix, { f0: fr, dur: 0.035, peak, at: t0 + k * spacing });
      };
      if (it.sev === "ok") {
        this.every(v.timers, () => chirp(f * 2, 3, 0.052, 0.0095), 1400, 2600, this.rnd(300, 1500));
      } else if (it.sev === "watch") {
        const detune = 1.012 + d * 0.012;
        this.every(
          v.timers,
          () => {
            chirp(f * 2, 4, 0.06, 0.008);
            chirp(f * 2 * detune, 4, 0.06, 0.0055);
          },
          700,
          3400,
          this.rnd(300, 1500)
        );
      } else if (it.sev === "critical") {
        const o = this.buzz(v, mix, f * 1.5, {
          amHz: 16 + d * 8,
          base: 0.011,
          depth: 0.95,
          bpMul: 1,
          q: 5,
        });
        const wob = this.mkOsc("sine", 3);
        const wobG = this.mkGain(25);
        wob.connect(wobG);
        wobG.connect(o.detune);
        wob.start();
        v.stops.push(wob);
      } else {
        this.every(
          v.timers,
          () => this.pop(mix, { f: 5200, q: 3, dur: 0.02, peak: 0.004 }),
          4000,
          4600,
          1000
        );
      }
    } else if (this.voicingId === "moose") {
      if (it.sev === "ok") {
        const o = this.mkOsc("sine", f);
        const g = this.mkGain(0.0001);
        g.gain.setTargetAtTime(0.008, now, 1.2);
        const vib = this.mkOsc("sine", 0.13);
        const vibG = this.mkGain(5);
        vib.connect(vibG);
        vibG.connect(o.detune);
        o.connect(g);
        g.connect(mix);
        o.start(now);
        vib.start(now);
        v.stops.push(o, vib);
      } else if (it.sev === "watch") {
        const detune = 1.0045 + d * 0.004; // sub-Hz beating at this register — slow waves
        [f, f * detune].forEach((freq) => {
          const o = this.mkOsc("sine", freq);
          const g = this.mkGain(0.0001);
          g.gain.setTargetAtTime(0.0075, now, 1.4);
          o.connect(g);
          g.connect(mix);
          o.start(now);
          v.stops.push(o);
        });
      } else if (it.sev === "critical") {
        const o = this.mkOsc("sawtooth", f);
        const lp = this.mkFilter("lowpass", f * 3.2, 1.2);
        const g = this.mkGain(0.0001);
        g.gain.setTargetAtTime(0.018, now, 1.2);
        const wob = this.mkOsc("sine", 0.8 + d * 0.8);
        const wobG = this.mkGain(25 + d * 45);
        wob.connect(wobG);
        wobG.connect(o.detune);
        o.connect(lp);
        lp.connect(g);
        g.connect(mix);
        o.start(now);
        wob.start(now);
        v.stops.push(o, wob);
        const n = ctx.createBufferSource();
        n.buffer = this.noiseBuffer(1.5, false);
        n.loop = true;
        const nf = this.mkFilter("bandpass", 290, 7);
        const gate = this.mkGain(0);
        const gateLFO = this.mkOsc("square", 2.2 + d);
        const gateAmt = this.mkGain(0.0075);
        gateLFO.connect(gateAmt);
        gateAmt.connect(gate.gain);
        n.connect(nf);
        nf.connect(gate);
        gate.connect(mix);
        n.start(now);
        gateLFO.start(now);
        v.stops.push(n, gateLFO);
      } else {
        this.every(
          v.timers,
          () => this.blip(mix, { f0: 155, f1: 86, dur: 1.5, peak: 0.009, attack: 0.4 }),
          5500,
          8000,
          1500
        );
      }
    } else if (this.voicingId === "bullfrog") {
      const croak = (fr: number, dur: number, peak: number, at?: number) =>
        this.blip(mix, {
          type: "sawtooth",
          f0: fr,
          dur,
          peak,
          at,
          attack: 0.05,
          amHz: 21,
          hold: dur * 0.5,
        });
      if (it.sev === "ok") {
        this.every(v.timers, () => croak(f, 0.4, 0.011), 2000, 3600, this.rnd(400, 1800));
      } else if (it.sev === "watch") {
        const detune = 1.03 + d * 0.03;
        this.every(
          v.timers,
          () => {
            const t0 = ctx.currentTime;
            croak(f, 0.32, 0.009, t0);
            croak(f * detune, 0.32, 0.0065, t0 + 0.18);
          },
          1100,
          4200,
          this.rnd(400, 1800)
        );
      } else if (it.sev === "critical") {
        const o = this.mkOsc("sawtooth", f);
        const lp = this.mkFilter("lowpass", 330, 1.5);
        const g = this.mkGain(0.0001);
        g.gain.setTargetAtTime(0.014, now, 0.9);
        const am = this.mkOsc("square", 12 + d * 6);
        const amG = this.mkGain(0.012);
        am.connect(amG);
        amG.connect(g.gain);
        const wob = this.mkOsc("sine", 1.3);
        const wobG = this.mkGain(35);
        wob.connect(wobG);
        wobG.connect(o.detune);
        o.connect(lp);
        lp.connect(g);
        g.connect(mix);
        o.start(now);
        am.start(now);
        wob.start(now);
        v.stops.push(o, am, wob);
        const n = ctx.createBufferSource();
        n.buffer = this.noiseBuffer(1.5, false);
        n.loop = true;
        const nf = this.mkFilter("bandpass", 210, 6);
        const gate = this.mkGain(0);
        const gateLFO = this.mkOsc("square", 3);
        const gateAmt = this.mkGain(0.005);
        gateLFO.connect(gateAmt);
        gateAmt.connect(gate.gain);
        n.connect(nf);
        nf.connect(gate);
        gate.connect(mix);
        n.start(now);
        gateLFO.start(now);
        v.stops.push(n, gateLFO);
      } else {
        this.every(
          v.timers,
          () => this.blip(mix, { f0: 200, f1: 84, dur: 0.3, peak: 0.005, attack: 0.03 }),
          5000,
          7000,
          1200
        );
      }
    } else if (this.voicingId === "peeper") {
      const peep = (fr: number, dur: number, peak: number, at?: number) =>
        this.blip(mix, {
          type: "triangle",
          f0: fr * 0.97,
          f1: fr,
          dur,
          peak,
          at,
          attack: 0.03,
          lp: fr * 2.2,
          lpQ: 0.6,
        });
      if (it.sev === "ok") {
        this.every(v.timers, () => peep(f * 1.5, 0.11, 0.01), 1100, 2000, this.rnd(200, 1400));
      } else if (it.sev === "watch") {
        const detune = 1.02 + d * 0.02;
        this.every(
          v.timers,
          () => {
            const t0 = ctx.currentTime;
            peep(f * 1.5, 0.1, 0.0085, t0);
            peep(f * 1.5 * detune, 0.1, 0.006, t0 + 0.12);
          },
          800,
          3200,
          this.rnd(200, 1400)
        );
      } else if (it.sev === "critical") {
        const o = this.buzz(v, mix, f * 1.5, {
          amHz: 18 + d * 9,
          base: 0.0095,
          depth: 0.95,
          bpMul: 1,
          q: 6,
        });
        const wob = this.mkOsc("sine", 4);
        const wobG = this.mkGain(28);
        wob.connect(wobG);
        wobG.connect(o.detune);
        wob.start();
        v.stops.push(wob);
      } else {
        this.every(
          v.timers,
          () => this.blip(mix, { f0: 2200, dur: 0.05, peak: 0.004 }),
          4200,
          4800,
          1000
        );
      }
    }

    this.voices.set(it.id, v);
  }

  /* ---------- isolate ---------- */
  private applySolo(ramp?: number) {
    if (!this.ctx) return;
    const t = this.ctx.currentTime;
    this.voices.forEach((v, id) => {
      const target = this.soloId == null ? 1 : id === this.soloId ? 2.4 : 0.04;
      v.mix.gain.cancelScheduledValues(t);
      v.mix.gain.setTargetAtTime(target, t, ramp || 0.18);
    });
    if (this.bedGroup)
      this.bedGroup.gain.setTargetAtTime(
        this.soloId == null ? 1 : 0.3,
        t,
        ramp || 0.25
      );
  }

  private stopVoices() {
    this.voices.forEach((v) => {
      v.timers.forEach((tm) => tm());
      v.stops.forEach((n) => {
        try {
          n.stop();
        } catch {
          /* already stopped */
        }
      });
      try {
        v.mix.disconnect();
      } catch {
        /* already disconnected */
      }
    });
    this.voices.clear();
  }

  /* ---------- tape-deck click — "sound stopped on purpose" ---------- */
  private tapeClick(on: boolean) {
    this.ensureCtx();
    const ctx = this.ctx!;
    if (ctx.state === "suspended") void ctx.resume();
    const t = ctx.currentTime + 0.01;
    const v = 0.45 + this.volume * 0.55;
    const n = ctx.createBufferSource();
    n.buffer = this.noiseBuffer(0.06, false);
    const bp = this.mkFilter("bandpass", on ? 2800 : 1600, 1.4);
    const g = this.mkGain(0.0001);
    g.gain.setValueAtTime(0.0001, t);
    g.gain.exponentialRampToValueAtTime((on ? 0.16 : 0.24) * v, t + 0.004);
    g.gain.exponentialRampToValueAtTime(0.0001, t + (on ? 0.035 : 0.05));
    const o = this.mkOsc("sine", on ? 300 : 210);
    o.frequency.setValueAtTime(on ? 300 : 210, t);
    o.frequency.exponentialRampToValueAtTime(65, t + 0.07);
    const og = this.mkGain(0.0001);
    og.gain.setValueAtTime(0.0001, t);
    og.gain.exponentialRampToValueAtTime((on ? 0.09 : 0.14) * v, t + 0.008);
    og.gain.exponentialRampToValueAtTime(0.0001, t + 0.1);
    n.connect(bp);
    bp.connect(g);
    g.connect(this.comp!);
    o.connect(og);
    og.connect(this.comp!);
    n.start(t);
    n.stop(t + 0.08);
    o.start(t);
    o.stop(t + 0.12);
  }

  /* ---------- analysis — visuals read the actual signal ----------
     Every item owns a unique note (per voicing), so the FFT bins at its
     centers ARE that item's live loudness. Auto-gain keeps it lively. */
  levels(items: SignalItem[]): AudioLevels | null {
    if (!this.ctx || !this.analyser) return null;
    const binHz = this.ctx.sampleRate / this.analyser.fftSize;
    if (!this.freqBuf)
      this.freqBuf = new Float32Array(this.analyser.frequencyBinCount);
    if (!this.timeBuf) this.timeBuf = new Float32Array(this.analyser.fftSize);
    this.analyser.getFloatFrequencyData(this.freqBuf);
    this.analyser.getFloatTimeDomainData(this.timeBuf);
    const freqBuf = this.freqBuf;
    const timeBuf = this.timeBuf;

    const lin = items.map((it, i) => {
      const f = this.freqFor(i);
      const centers = this.V().centers(it.sev, f);
      let best = -Infinity;
      centers.forEach((c) => {
        const b = Math.round(c / binHz);
        for (let k = Math.max(1, b - 1); k <= b + 1; k++)
          if (freqBuf[k] > best) best = freqBuf[k];
      });
      return Math.pow(10, best / 20);
    });
    const pk = Math.max(1e-5, ...lin);
    this.vizPeak = Math.max(pk, this.vizPeak * 0.996);
    const per = lin.map((v) => Math.min(1, Math.pow(v / this.vizPeak, 0.55)));

    let s = 0;
    for (let k = 0; k < timeBuf.length; k += 4) s += timeBuf[k] * timeBuf[k];
    const rmsLin = Math.sqrt(s / (timeBuf.length / 4));
    this.wavePeak = Math.max(rmsLin * 2.8, this.wavePeak * 0.995, 0.012);
    const N = 256;
    const wave = new Float32Array(N);
    const stride = Math.floor(timeBuf.length / N);
    for (let k = 0; k < N; k++)
      wave[k] = Math.max(-1, Math.min(1, timeBuf[k * stride] / this.wavePeak));
    return { per, rms: Math.min(1, (rmsLin / this.wavePeak) * 1.4), wave };
  }
}
