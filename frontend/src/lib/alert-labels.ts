/**
 * Friendly UI labels for alert source identifiers (Alert.alert_type).
 *
 * Background — see AI-052. The raw `alert_type` is a stable backend contract
 * used for dedup keys, BigQuery indexing, `alert_id` construction, Slack
 * dispatch, and 24-hour dedup matching, so we don't want to humanize it
 * server-side. But the raw snake_case string was bleeding through to the UI
 * pill on the Alerts page and the per-project Alerts tab. This module is the
 * single source of truth for the human label; the raw key is kept in a native
 * `title=` tooltip at the render site so ops can still grep/dedup-debug.
 *
 * Sync notes:
 * - Pacing / staleness alert_types come from `backend/services/pacing.py` and
 *   `backend/services/daily_job.py`.
 * - Diagnostic alert_types are always `diagnostic_<alert.type>` — see
 *   `backend/services/diagnostics/engine.py` (`_fire_alerts`) +
 *   `backend/services/diagnostics/shared/alerts.py`. The signal-name table
 *   below mirrors the `name="..."` arguments at the SignalResult construction
 *   sites in `backend/services/diagnostics/persuasion/*.py` and
 *   `backend/services/diagnostics/conversion/*.py`. When a new signal ships,
 *   add it here; the helper falls back to "Signal <id> alert" so a stale
 *   table degrades gracefully.
 */

/** Static labels for non-signal alert_types (pacing + staleness + campaign-level diagnostic). */
const STATIC_LABELS: Record<string, string> = {
  pacing_under: "Under-pacing alert",
  pacing_over: "Over-pacing alert",
  data_stale: "Stale data alert",
  diagnostic_health_regression: "Health regression alert",
};

/** Signal-id → human name. Mirrors backend SignalResult `name=` arguments. */
const SIGNAL_NAMES: Record<string, string> = {
  // Funnel pillar (conversion)
  F1: "Click-Through Rate",
  F2: "Landing Page Load Rate",
  F3: "Scroll & Form Discovery",
  F4: "Form Completion Rate",
  F5: "Post-Conversion Activation",
  // Acquisition pillar (conversion)
  C1: "CPA vs Target",
  C2: "Volume Trajectory",
  C3: "CPA Trend",
  // Attention pillar (persuasion)
  A1: "Video Completion Quality",
  A3: "Viewability",
  A4: "Focused View",
  A5: "Creative Fatigue",
  // Resonance pillar (persuasion)
  R1: "Engagement Quality Ratio",
  R3: "Landing Page Depth",
  // Distribution pillar (persuasion)
  D1: "Reach Attainment",
  D2: "Frequency Adequacy",
  D3: "Frequency Distribution",
  D4: "Incremental Reach",
  D5: "Delivery Cadence",
};

/**
 * Return a human-readable label for an Alert.alert_type pill.
 *
 * Resolution order:
 *  1. STATIC_LABELS lookup (pacing / staleness / health regression)
 *  2. `diagnostic_signal_<id>` → `Signal <ID> alert — <Name>` (or
 *     `Signal <ID> alert` if the id isn't in SIGNAL_NAMES yet)
 *  3. Generic fallback: replace underscores, sentence-case, append " alert"
 *     (so a future `pacing_xyz` lands as "Pacing xyz alert" rather than as
 *     raw snake_case).
 */
export function formatAlertSource(key: string): string {
  const staticHit = STATIC_LABELS[key];
  if (staticHit) return staticHit;

  const m = key.match(/^diagnostic_signal_([a-z][0-9]+)$/);
  if (m) {
    const id = m[1].toUpperCase();
    return SIGNAL_NAMES[id]
      ? `Signal ${id} alert — ${SIGNAL_NAMES[id]}`
      : `Signal ${id} alert`;
  }

  const human = key.replace(/_/g, " ").trim();
  if (!human) return key;
  return human.charAt(0).toUpperCase() + human.slice(1) + " alert";
}
