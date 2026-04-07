/**
 * Oscilloscope wave generation for pacing health visualization.
 *
 * Three channels represent campaign pacing health:
 *   Ch0: Highest-pacing line (overspend risk)
 *   Ch1: Overall project pacing
 *   Ch2: Lowest-pacing line (underspend risk)
 *
 * Health score H (0–1) drives wave behavior:
 *   H ≈ 1 → three distinct sine waves at odd harmonics (alive, healthy)
 *   H → 0  → waves converge to same frequency/phase (flat, unhealthy)
 */

import type { PacingLine } from "./api";

// ── Health Score ────────────────────────────────────────────────────

export function computeHealthScore(lines: PacingLine[]): number {
  if (lines.length === 0) return 0.5;
  const devs = lines.map((l) => Math.abs(l.pacing_percentage - 100) / 100);
  const avg = devs.reduce((a, b) => a + b, 0) / devs.length;
  const max = Math.max(...devs);
  return Math.max(0, Math.min(1, 1 - (0.7 * avg + 0.3 * max)));
}

// ── Channel Extraction ──────────────────────────────────────────────

export interface ChannelInfo {
  pct: number;
  label: string;
}

export function extractChannels(
  lines: PacingLine[],
  overallPct: number
): [ChannelInfo, ChannelInfo, ChannelInfo] {
  if (lines.length === 0) {
    return [
      { pct: overallPct, label: "High" },
      { pct: overallPct, label: "Overall" },
      { pct: overallPct, label: "Low" },
    ];
  }
  const sorted = [...lines].sort(
    (a, b) => b.pacing_percentage - a.pacing_percentage
  );
  const high = sorted[0];
  const low = sorted[sorted.length - 1];
  return [
    {
      pct: high.pacing_percentage,
      label: high.audience_name || high.platform_id || "High",
    },
    { pct: overallPct, label: "Overall" },
    {
      pct: low.pacing_percentage,
      label: low.audience_name || low.platform_id || "Low",
    },
  ];
}

// ── Color from Pacing Percentage ────────────────────────────────────

export function pacingToColor(pct: number): string {
  if (pct >= 85 && pct <= 115) return "#34d399"; // emerald-400
  if (pct >= 70 && pct <= 130) return "#fbbf24"; // amber-400
  return "#f87171"; // red-400
}

// ── Wave Path Generation ────────────────────────────────────────────

const BASE_FREQ = [3, 5, 7]; // odd harmonics per channel
const PHASE_SPEED = [0.8, 1.1, 1.4]; // animation speed multipliers

/**
 * Generate an SVG path string for one oscilloscope channel.
 *
 * @param channel  0, 1, or 2
 * @param health   0–1 health score
 * @param t        elapsed seconds (from requestAnimationFrame)
 * @param width    SVG viewBox width
 * @param height   SVG viewBox height
 * @param yCenter  vertical center for this channel's band
 * @param bandH    height of the band allocated to this channel
 */
export function generateWavePath(
  channel: number,
  health: number,
  t: number,
  width: number,
  yCenter: number,
  bandH: number
): string {
  const h = health;
  const freq = BASE_FREQ[channel] * h + 4.0 * (1 - h);
  const amp = bandH * (0.35 * h + 0.08 * (1 - h));
  const phaseBase = t * PHASE_SPEED[channel] * h + t * 0.3 * (1 - h);
  const noiseAmp = bandH * (1 - h) * 0.12;

  const steps = 80;
  const dx = width / steps;
  const points: string[] = [];

  for (let i = 0; i <= steps; i++) {
    const x = i * dx;
    const nx = i / steps; // normalized 0–1

    // Primary wave
    const primary = Math.sin(2 * Math.PI * freq * nx + phaseBase);
    // Secondary harmonic for organic shape
    const secondary =
      0.3 * Math.sin(2 * Math.PI * freq * 2.1 * nx + phaseBase * 1.7);
    // Deterministic noise (seeded by position + time)
    const noise =
      noiseAmp *
      (Math.sin(nx * 47.3 + t * 2.1) * 0.5 +
        Math.sin(nx * 31.7 + t * 3.3) * 0.5);

    const y = yCenter + amp * (primary + secondary) + noise;
    points.push(`${i === 0 ? "M" : "L"}${x.toFixed(1)},${y.toFixed(1)}`);
  }

  return points.join(" ");
}
