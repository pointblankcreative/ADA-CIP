# Diagnostic Alert Rules

**Status:** Active (interim — revisit after team rollout)
**Owner:** Frazer
**Last reviewed:** 2026-04-17

---

## Purpose

Define the rules that govern when the diagnostic engine fires Slack
alerts, how loud those alerts are, and how they're worded. This is the
source-of-truth for both human intuition ("should I expect an alert
here?") and code behavior. When thresholds change, edit this doc and
the code in the same commit.

The engine already produces `DiagnosticOutput` objects on every run —
the question here is: **which of those outputs rise to the level of
"page somebody"?**

---

## Design principles

1. **Dashboards for state, alerts for change.** The campaign health
   dashboard shows which campaigns are currently WATCH/ACTION. A
   Slack alert should mean "something *new* happened" — typically a
   regression or a concrete signal breakage — not "this campaign is
   still red, same as yesterday."
2. **Conservative firing.** An alert that fires too often trains the
   team to ignore the channel. Prefer false negatives over false
   positives in the first few weeks of rollout; loosen thresholds
   once we see how noisy the real data is.
3. **Mixed campaigns fire per campaign_type.** A project running both
   persuasion and conversion lines has two independent health scores;
   each can alert independently. Alert IDs are namespaced by
   campaign_type so they don't collide.
4. **Dedup like pacing.** Reuse the 24h
   `(project_code, alert_type, severity)` dedup window from
   `services/pacing.py` so the two subsystems behave consistently.

---

## What fires

Two alert families, both at **critical** severity:

### 1. Health regression — state transition to ACTION

Fires when a campaign's health score crossed into the ACTION band
(< 40) since the last evaluation. Specifically:

- Previous evaluation (same `project_code` + `campaign_type`, most
  recent row in `fact_diagnostic_signals` before today) had
  `health_status` in (`STRONG`, `WATCH`, or NULL).
- Today's evaluation has `health_status == ACTION`.

`alert_type` = `diagnostic_health_regression`

Does **not** fire when:
- Today's status equals yesterday's status (ACTION → ACTION stays
  silent — that's the dashboard's job).
- No prior row exists (campaign's first evaluation — fire a *launch*
  alert in future, not in this release).
- Today's health score is None / unscorable.

### 2. Signal-level ACTION — per-signal failure

Fires for each individual signal that:

- Has `guard_passed == True` (we measured it cleanly), AND
- Has `status == ACTION` (score < 40), AND
- Is in a *scored* pillar (Quality pillar is deferred; its signals
  can't fire).

One alert per failing signal. `alert_type` =
`diagnostic_signal_<signal_id_lower>` (e.g.,
`diagnostic_signal_f1`). That's intentional — it gives dedup a
granular key so F1 firing today doesn't suppress F4 firing tomorrow.

Signal-level alerts fire **independently** of health-regression
alerts. A campaign that regresses to ACTION because F1 and F4 both
collapsed will produce one regression alert + two signal alerts on
that day. Dedup keeps subsequent runs quiet until they naturally
expire.

---

## What does *not* fire (deliberately)

These are visible on the dashboard but do not alert — at least not
yet. Revisit once we have a feel for the channel's noise floor.

- **WATCH band entries.** A campaign dropping STRONG → WATCH is
  noteworthy but not urgent. Shown on dashboard; no alert.
- **Recovery events.** ACTION → STRONG is good news, not a page.
  Visible on the dashboard's state history.
- **Specific signal regressions short of ACTION.** A signal sliding
  STRONG → WATCH is monitoring-tier, not alert-tier.
- **Pacing-combined alerts.** Pacing emits its own alerts. A separate
  "burning money" product that correlates pacing × health could be
  built later, but is explicitly out of scope for this release.
- **R2 guard failures.** R2 (earned impressions) is expected to
  guard-fail everywhere until Phase 3 connectors land. A guard-fail
  is never an alert anyway; this is belt-and-suspenders.

---

## Severity mapping

| Rule                              | Severity   |
|-----------------------------------|------------|
| Health regression to ACTION       | `critical` |
| Signal-level ACTION (any signal)  | `critical` |

Both rules produce `critical` severity. Warning-tier diagnostic
alerts may be added later (e.g., for WATCH regressions); this release
keeps it binary.

---

## Dedup

Applied in `engine._fire_alerts` before the INSERT. Matches the
existing pacing pattern in `services/pacing._deduplicate_alerts`:

- Window: **24 hours**
- Key: `(project_code, alert_type, severity)`
- Only suppresses alerts where `resolved_at IS NULL` in the window
  (resolved alerts don't count against re-firing)

Same key structure as pacing, so future work can promote the helper
to a shared module.

---

## Message format

Each alert row written to the `alerts` table has:

- `alert_id` — deterministic:
  `diag-{project_code}-{campaign_type}-{alert_type}-{evaluation_date}`
  Note: campaign_type and alert_type together already uniquely
  identify the alert, but the ID also embeds the date so dedup can
  distinguish today's from yesterday's for auditing.
- `alert_type` — as listed above
  (`diagnostic_health_regression` or `diagnostic_signal_<id>`)
- `severity` — `critical`
- `title` — human-readable summary (see below)
- `message` — full body text, rendered into Slack by the existing
  dispatcher

### Title format

- Health regression:
  `{project_code} [{campaign_type}] · Health dropped to ACTION ({score})`
  Example: `26009 [conversion] · Health dropped to ACTION (34)`

- Signal-level:
  `{project_code} [{campaign_type}] · {signal_id} {signal_name} — ACTION ({score})`
  Example: `26009 [conversion] · F1 Click-to-Landing-Page — ACTION (22)`

### Message body

Health-regression messages include the top 2 failing signals
(lowest score, guard_passed, from scored pillars) to give the
on-call reader enough to triage without opening the dashboard:

```
Health score dropped from {prev_score} ({prev_status}) to {score} (ACTION).

Top failing signals:
 • F1 Click-to-Landing-Page — 22 (ACTION) — {diagnostic text}
 • C2 CPA Trend — 31 (ACTION) — {diagnostic text}

Flight day {flight_day} of {flight_total_days}. Review on dashboard.
```

Signal-level messages use the signal's own `diagnostic` field:

```
{signal_name} ({signal_id}) scored {score} (ACTION).

{diagnostic text from the signal}

Flight day {flight_day} of {flight_total_days}.
```

The Slack dispatcher is responsible for any further formatting
(blocks, buttons, dashboard links). This module just writes to the
alerts table.

---

## Data requirements

State-transition detection needs to query
`fact_diagnostic_signals` for the most recent row before today
matching `(project_code, campaign_type)`. That table is already
clustered on `(project_code, campaign_type)` so this is a cheap
lookup. The query pattern:

```sql
SELECT health_status, health_score, evaluation_date
FROM `{project}.{dataset}.fact_diagnostic_signals`
WHERE project_code = @project_code
  AND campaign_type = @campaign_type
  AND evaluation_date < @evaluation_date
ORDER BY evaluation_date DESC
LIMIT 1
```

If no row exists, skip the regression alert.

---

## Revisit checklist

After ~2 weeks of team usage, review:

- **Volume.** How many alerts/day is the channel getting? Target
  range: 0–5/day across all active campaigns. >10/day is noise.
- **False-positive rate.** How often does the team react to an alert
  and find it wasn't actionable? >30% is a loosening signal.
- **Missed events.** Did anyone catch something the engine should
  have flagged? Add the rule.
- **WATCH-tier demand.** If the team asks "why didn't we get a
  warning before it went red?" — add WATCH-regression as a
  `warning`-severity rule.
- **Recovery.** Would an ACTION → STRONG recovery alert be useful
  (celebratory + audit trail)? Currently skipped; trivial to add.
