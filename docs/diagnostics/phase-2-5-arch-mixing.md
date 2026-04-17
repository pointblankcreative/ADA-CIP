# Phase 2.5 — Within-a-Line Ad-Set Architecture Mixing

**Status:** Documented, deferred
**Decision date:** 2026-04-17
**Owner:** Frazer
**Unblocker:** Ad-set-grain architecture classification + ad-set-grain
FFS collection (see "What 'fixed' looks like" below)

---

## TL;DR

The mixed-campaign engine (Build Plan §12, shipped on
`feat/engine-mixed-campaigns`) classifies campaigns two different ways:

1. **Campaign type** — persuasion vs conversion — classified per **media
   plan line** by `line_classifier.classify_line()`.
2. **Funnel architecture** — Arch A (landing-page flow) vs Arch B
   (in-platform form) — classified per **media plan line** by
   `conversion/funnel._classify_line_architecture()`.

Both classifications are at *line* grain. A media plan line may back a
platform campaign that contains multiple ad sets, and in practice ad
sets under a single line can mix architectures. When they do, the
pillar scoring silently attributes all of that line's traffic to
whichever architecture the line is tagged with.

We are **not** closing that gap right now. This note records what the
limitation looks like, when it actually matters, and what a fix would
require so the next person to pick it up (likely future-Frazer) has
the design context.

---

## The limitation, concretely

### Where it lives

- `backend/services/diagnostics/conversion/funnel.py`:
  `_classify_line_architecture(line)` returns `"arch_a"` or `"arch_b"`.
  The classifier reads `line.ffs_inputs.is_platform_form` (if set by
  the FFS wizard) and falls back to objective-keyword matching
  (`instant form`, `lead gen form`, `on-platform`, etc.).
- `backend/services/diagnostics/conversion/funnel.py`:
  `_compute_arch_mix(data)` splits the line list into two buckets and
  blends F1–F5 scores pro-rata by planned spend share.
- `backend/services/diagnostics/engine.py`:
  `_query_platform_metrics_by_type()` aggregates
  `fact_digital_daily` and `fact_adset_daily` to
  `(campaign_type, platform_id)` grain. **Ad-set identity is
  discarded** before the data reaches the Funnel pillar.

### Why it breaks down

A single media plan line often corresponds to one platform *campaign*
that contains several *ad sets*. Media planners routinely split ad
sets within a campaign by creative concept, audience segment, or
form type — including, occasionally, by form type.

Real-ish example:

> Line: `25042 - Meta - Lead Gen - Flight 2` (planned budget $8,000)
> ↳ Ad set A: Retargeting — drives to LP form (Arch A)
> ↳ Ad set B: Prospecting — uses Meta Instant Form (Arch B)

Today's classifier looks at the line's objective text ("Lead Gen")
and its `is_platform_form` FFS flag (one value for the whole line,
set once in the wizard) and picks a single architecture. Every
click, form_start, and form_submit for that line gets scored under
that single architecture's weights. In the example above, if the
line is tagged Arch B, Ad set A's landing-page traffic will be
scored against in-platform-form benchmarks (and vice versa).

### How loud is the error

Modest in practice but directionally wrong when it fires. Within-line
mixing is the minority case — most lines are one architecture end to
end. When it does happen, the pillar blend is still reasonable at the
*campaign* level because the share-weighted blend at the line grain
dominates, but the per-signal diagnostic messages (F2/F3/F4 specifically)
can name the wrong failure mode: "landing page load rate is low" when
the real problem is a friction-heavy Instant Form, or vice versa.

This is exactly the failure mode we want to avoid long-term, because
the diagnostic is supposed to tell PB *which part of the funnel* is
broken. Attributing form friction to an LP or vice versa undermines
the whole point.

---

## Why defer

### 1. Fixing it properly needs ad-set-grain FFS

The Form Friction Score is collected via a dashboard wizard that runs
**per line**. `ffs_inputs` is a JSON blob on `media_plan_lines`
(fields: `field_count`, `required_count`, `field_types`,
`clicks_to_submit`, `form_position`, `has_autofill`,
`is_platform_form`). If two ad sets in the same line use different
form architectures, a single FFS value is definitionally wrong —
you'd need two.

Moving FFS to ad-set grain means:

- A new table `ffs_inputs_by_adset` keyed on
  `(project_code, platform_id, campaign_id, ad_set_id)`, or a
  JSON map on `media_plan_lines` keyed by `ad_set_id`.
- A redesigned wizard UX that walks the user through each ad set
  under a line instead of treating the line as one form.
- Backfill logic: existing single-value FFS entries apply to all
  ad sets under the line until the wizard is re-run.

That's a non-trivial UX change for a minority case. We don't want to
land it under Phase 2 scope.

### 2. The data *is* at ad-set grain — the engine just drops it

`fact_digital_daily` has `ad_set_id` and `ad_set_name` and all the
conversion-relevant columns (`clicks`, `landing_page_views`,
`leads`, `on_platform_leads`, `outbound_clicks`). `fact_adset_daily`
has reach/frequency at ad-set grain.

The *engine* aggregates these to `(type, platform_id)` grain in
`_query_platform_metrics_by_type()` — see the bucket key
`(ctype, platform_id)`. Preserving ad-set grain through the pillar
computation is a plumbing change, not a data-ingestion change. That's
the cheap half of the fix; the expensive half is FFS collection (above).

### 3. Meta-specific split already buys us most of the value

For Meta specifically, `fact_digital_daily` has a
`leads` vs `on_platform_leads` column pair. F4 (Form Completion Rate)
already uses this split to refine its scoring even when line-level
classification is coarse. In practice, Meta is where within-line
arch mixing happens most often (Instant Forms alongside LP forms),
and the leads/on_platform_leads split catches the dominant case
without ad-set-grain plumbing. The residual error is on
non-Meta platforms (LinkedIn, TikTok Lead Gen) where within-line
mixing is rarer.

---

## What "fixed" looks like

A full ad-set-grain architecture classifier requires all four:

1. **Ad-set identity preserved through the engine.** Modify
   `_query_platform_metrics_by_type()` (and its conversion path) to
   emit per-ad-set `PlatformMetrics` or introduce an
   `AdSetMetrics` sub-shape on `PlatformMetrics`. Aggregation to
   platform grain happens inside the pillar after arch-split scoring,
   not before.

2. **Per-ad-set architecture classification.** Either:
   - Extend the FFS wizard to collect per-ad-set inputs (preferred;
     gives us the rest of FFS at ad-set grain for free), OR
   - Heuristic-classify each ad set by `ad_set_name` keyword match
     using the `_ARCH_B_KEYWORDS` list (cheap fallback; matches the
     existing campaign-name heuristic pattern).

3. **Arch-aware pillar API.** `compute_funnel_pillar()` takes a
   per-ad-set view rather than `ArchMix` over the line list. The
   share-weighted blend is computed at ad-set grain; `ArchMix`
   becomes a derived summary, not the input.

4. **Diagnostic message fidelity.** Per-signal diagnostic strings
   need to name the ad sets they apply to when a line is mixed.
   Today they say "landing pages are slow" — in a fixed world they'd
   say "3 of 5 ad sets under this line run to a landing page; LP
   load rate on those is 42%."

The cheapest viable cut is (2) via the heuristic + (1) for plumbing,
which gets us directionally-correct pillar scoring without touching
the FFS wizard. Full fidelity needs all four.

---

## What we're doing in the interim

- **Leave the code honest about the limitation.** The `KNOWN
  LIMITATION` comment block at the top of
  `backend/services/diagnostics/conversion/funnel.py` points here.
  Do not remove it until the fix lands.
- **Trust the Meta `on_platform_leads` column.** F4 already uses the
  in-platform vs website leads split. This absorbs the dominant
  within-line arch-mixing case on the platform where it's most
  common.
- **Conservative default favours Arch A.** When `is_platform_form`
  is unset and no keyword hits,
  `_classify_line_architecture()` returns `"arch_a"`. LP-flow
  benchmarks are stricter than Instant-Form benchmarks, so
  misclassifying an Arch B line as Arch A biases the health score
  downward (toward under-promise), not upward — matches the
  conservative-estimates ethos.
- **Do not invent an ad-set-level classifier today.** A
  keyword-based heuristic on `ad_set_name` is tempting, but without
  the plumbing in (1) above it would have no place to feed into.
  Partial fixes here would create more surface area than they
  remove.

---

## Detecting it in the wild

If we start seeing diagnostic false positives that pattern-match
"F2 reports landing-page slowness for a campaign that doesn't run
to a landing page" (or the F4 mirror: "in-platform form friction on
a campaign that doesn't use in-platform forms"), that's this bug.

A quick detection query:

```sql
-- Per-project: does any line's ad sets span multiple arch classes?
-- Heuristic: ad_set_name keyword match for Arch B.
WITH adset_arch AS (
  SELECT
    d.project_code,
    d.line_code,
    d.ad_set_id,
    CASE
      WHEN LOWER(d.ad_set_name) LIKE '%instant form%'
        OR LOWER(d.ad_set_name) LIKE '%lead gen form%'
        OR LOWER(d.ad_set_name) LIKE '%on-platform%'
        OR LOWER(d.ad_set_name) LIKE '%on platform%'
      THEN 'arch_b'
      ELSE 'arch_a'
    END AS adset_arch
  FROM `point-blank-ada.cip.fact_digital_daily` d
  WHERE d.project_code = @project_code
    AND d.ad_set_id IS NOT NULL
)
SELECT line_code, COUNT(DISTINCT adset_arch) AS arch_classes
FROM adset_arch
WHERE line_code IS NOT NULL
GROUP BY line_code
HAVING arch_classes > 1
ORDER BY arch_classes DESC;
```

Lines returned are candidates for within-line arch mixing. Treat the
result as a hint, not a verdict — the heuristic is the same coarse
keyword match the line-level classifier would make, so it can miss
the same things.

---

## Revisit checklist

When we come back to this:

- [ ] Decide on the classification-source strategy: ad-set-grain FFS
      wizard vs `ad_set_name` heuristic vs hybrid. If FFS is going
      multi-row anyway for other reasons, hitch this to that work.
- [ ] Define `AdSetMetrics` (or equivalent) inside
      `backend/services/diagnostics/models.py`.
- [ ] Refactor `_query_platform_metrics_by_type()` and its companion
      daily/GA4 queries to preserve `ad_set_id`. Platform-grain
      aggregation moves into the pillar after arch-split scoring.
- [ ] Rewrite `_compute_arch_mix()` to take ad-set inputs and
      compute the share-weighted blend per ad set. `ArchMix`
      becomes derived.
- [ ] Update F1–F5 diagnostic message templates to name the ad-set
      subset they apply to when a line is mixed.
- [ ] Add `TestWithinLineArchMixing` — at minimum: a mixed line
      produces different pillar scores than either pure-Arch-A or
      pure-Arch-B classification of the same traffic; diagnostic
      strings name the right subset.
- [ ] Remove the `KNOWN LIMITATION` block at the top of
      `backend/services/diagnostics/conversion/funnel.py`.
- [ ] Decide: keep this doc as historical context (recommended) or
      delete it with a note in the commit. Lean toward keep — the
      reasoning for *why* we deferred is the expensive part to
      reconstruct.
