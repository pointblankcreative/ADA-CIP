# Quality Pillar — Deferred Pending CRM Integration

**Status:** Deferred indefinitely
**Decision date:** 2026-04-16
**Owner:** Frazer
**Unblocker:** Per-client CRM ingestion into the warehouse

---

## TL;DR

The original diagnostic spec defined a Conversion-side Quality pillar
(Q1–Q3) to answer "are the leads we're buying actually worth anything?"
We are **not** building it right now. Any Quality score built on the
signals available to us today (GA4 `key_events`, platform lead-form
counts, submit-to-engagement ratios, etc.) would be dishonest — it
would *look* like a lead-quality score while measuring something
closer to downstream engagement friction. That violates the
"under-promise / over-deliver" posture of the diagnostic engine.

Instead:

- Quality's original **0.30 pillar weight** has been redistributed
  proportionally to the two scored Conversion pillars:
  - **Acquisition: 0.30 → 0.43** (0.30 / 0.70 share of active weight)
  - **Funnel:      0.40 → 0.57** (0.40 / 0.70 share of active weight)
- The scored Conversion health score therefore reflects only what we
  can *actually* measure today.
- When CRM integration lands (see "Unblocking requirements" below),
  revisit this doc, re-introduce the Q1–Q3 signals, and restore the
  original weight split.

---

## Why defer

### 1. No reliable disposition data

Lead **quality** is a disposition question: of the leads we delivered,
how many were contactable, qualified, pursued, and converted?
Answering that requires CRM data — specifically, the state each lead
progressed to after it landed in the client's system.

PB's clients use a long tail of different CRMs (HubSpot, Salesforce,
NGP VAN, ActionKit, custom Airtable setups, etc.). We do not today
ingest disposition data from any of them consistently. Some clients
don't report disposition at all; some report it weekly via spreadsheet;
some never share it.

### 2. The proxies we *do* have are not quality signals

A Quality pillar built from currently-available data would have to
lean on:

- GA4 `key_events` (e.g., "engaged_submit", "scroll_depth_90")
- Platform-reported lead-form counts (Meta Instant Forms, LinkedIn
  Lead Gen, TikTok Lead Gen)
- Submit-to-engagement ratios from GA4
- Post-submit dwell time / thank-you page pings

None of these answer "was the lead valuable." They measure how
friction-heavy or engagement-rich the submit path was — which Funnel
already covers. A "Quality" pillar built from these would duplicate
Funnel under a misleading label.

### 3. Time-to-build is ~6 months, landscape will shift

Wiring per-client CRM ingestion is a non-trivial project (auth per
client, schema normalization per CRM, field mapping per engagement
type, back-pressure on stale updates). Realistic delivery is 6+
months. The ad platforms, privacy landscape, and our own reporting
needs will have moved significantly by then. Holding a scoring slot
"in purgatory" until that work is done creates a permanent "the
health score is missing a chunk of its weight" caveat that hurts more
than it helps.

---

## What "deferred" means in code

Grep for `Quality (Q1-Q3) is deferred` to find the canonical comment.

### Current state

- `CONVERSION_PILLAR_WEIGHTS` in `backend/services/diagnostics/shared/benchmarks.py`
  contains only `acquisition: 0.43` and `funnel: 0.57`.
- `backend/services/diagnostics/conversion/health.py` assembles the
  pillar list as `[acquisition, funnel]` — no Quality placeholder is
  emitted. Downstream consumers never see an "unscorable Quality
  pillar"; it simply does not exist in the output.
- Pillar module docstrings in `__init__.py`, `models.py`,
  `conversion/health.py`, `conversion/acquisition.py`, and
  `conversion/funnel.py` all point to this document for reasoning.

### What we did **not** do

- We did **not** keep Quality as a placeholder pillar with
  `score=None, guard_failed=True`. That would have preserved the
  0.30 weight slot but produced an unscorable pillar on every
  Conversion run forever, which is operational noise.
- We did **not** implement Q1–Q3 with proxy signals and a "caveat
  emptor" label. A labelled-but-dishonest score is worse than no
  score.

---

## Unblocking requirements

For Quality to move off the shelf, all four of the following need to
be true:

1. **Disposition ingestion for ≥3 major CRM types** used by PB
   clients (a reasonable first cut: HubSpot, Salesforce, NGP VAN).
   Minimum field set per lead: `external_id`, `received_at`,
   `current_stage`, `stage_updated_at`, `is_disqualified`,
   `disqualification_reason` (free-text OK).
2. **Mapping layer** from client-specific stage names → a shared
   set of funnel-agnostic states: `new → contacted → qualified →
   pursued → converted → disqualified`. This probably lives as a
   per-client config table keyed on `project_code`.
3. **Freshness SLA.** Disposition state must land in the warehouse
   within ≤72 hours of the CRM update to be usable in a running-campaign
   diagnostic. Historical-only backfills are useful for retrospectives
   but not for in-flight scoring.
4. **Minimum volume floors** per signal. Quality ratios (e.g.,
   qualified-rate) are meaningless on <30 leads; guards will need to
   be tuned. This is a tuning problem, not a blocker, but must be
   addressed in the rebuild.

---

## Candidate Q-signals (when we come back to this)

These are the signals we had scoped in the original spec. They are
preserved here for continuity — do not treat them as committed
definitions. When we revisit, the CRM data we actually have may
suggest a different decomposition.

### Q1 — Qualified Rate

> "Of the leads we delivered, how many made it past the
> client's first-pass qualification?"

Ratio: `qualified_leads / total_leads_delivered`

Guard: `total_leads_delivered >= 30` AND disposition available for
≥70% of leads.

Benchmark: TBD — will vary by client/sector. Needs 90 days of
cross-client data to calibrate.

### Q2 — Disqualification Reason Concentration

> "Are disqualifications clustered on a specific reason we can act
> on (e.g., wrong geo, spam, duplicate)?"

This is more of a diagnostic hint than a numeric score — if >40%
of disqualifications share a single reason, surface it as the
primary callout. Otherwise no signal emitted.

### Q3 — Pursue Efficiency / Contactability

> "Of the qualified leads, what share did the client actually
> pursue (and how fast)?"

Ratio: `pursued / qualified`, with a time-to-first-touch component
(median hours from `received_at` to `stage_updated_at` leaving
`new`).

This one is arguably more of a *client behavior* signal than a *our
ads* signal. Worth revisiting whether it belongs in the health score
or as a separate client-facing diagnostic.

---

## Revisit checklist

When the unblocking requirements above are satisfied, the rebuild
work is:

- [ ] Re-add `quality` to `CONVERSION_PILLAR_WEIGHTS` with weight
      0.30; adjust Acquisition back to 0.30 and Funnel to 0.40.
- [ ] Build Q1–Q3 signal modules under
      `backend/services/diagnostics/conversion/quality/`.
- [ ] Implement `compute_quality_pillar(data: CampaignData)` following
      the Funnel/Acquisition pattern.
- [ ] Extend `CampaignData` with a `LeadDisposition` sub-object
      sourced from the CRM-ingestion pipeline.
- [ ] Update `compute_conversion_health` to include the quality
      pillar.
- [ ] Add `TestQualityPillar` covering each signal's guards,
      benchmarks, and edge cases.
- [ ] Update all pillar-list docstrings (`__init__.py`, `models.py`,
      `conversion/health.py`, this file).
- [ ] Decide: keep this file as a historical artifact or delete it
      with a note in the commit message. I'd lean toward keeping it
      — the *why we waited* context is useful.
