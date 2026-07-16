---
name: ada-propose-fixes
description: >-
  Take the friction a UAT run surfaced on the ADA Campaign Intelligence Platform
  and turn it into vetted, ready-to-build fix proposals — without building or
  deploying anything. For each flagged issue it works out the root cause and the
  exact change, then hardens the proposal through a red-team of negative
  reviewers who try to poke holes, a code-debt-and-fragility pass that checks the
  fix isn't overcomplicating things (and looks for a simpler, less-fragile root
  fix or a consolidation across several flags), and a re-validation that runs the
  proposed solution back past the original UAT tester personas to confirm it
  actually resolves their problem. Use this after ada-simulate-uat (it reads the
  run's flags.json), or when the user says "propose fixes", "propose fixes for
  the UAT issues", "red-team these fixes", "reduce the fragility / code debt", or
  "work out how to fix what UAT found".
---

# ADA Propose Fixes

You are stage two of the ADA UAT pipeline. Stage one (`ada-simulate-uat`) found
the friction and wrote a report + a `flags.json` ledger. Your job is to turn
those flags into **fix proposals that have already survived attack** — proposed,
red-teamed, de-risked for code debt, and confirmed by the personas who hit the
problem — and hand them to `ada-deploy-fixes`.

```
ada-simulate-uat   → flags.json (the problems)
ada-propose-fixes  → proposals.json + proposal report (YOU ARE HERE)
ada-deploy-fixes   → build, deploy to staging, verify
```

**You build nothing and deploy nothing.** No commits to `main`, no code left in
the product, no data touched. You investigate in a throwaway worktree and produce
proposals. The only thing that changes in the repo is optional Asana tickets, and
only behind a confirm gate.

Read the shared ADA facts first:
`.claude/skills/ada-simulate-uat/references/ada-project-facts.md` (the product's
honesty rules, the R&F source of truth, the reach recipes, the guardrails, the
autonomy/park boundary, and the environment reality). Re-confirm the load-bearing
ones against the current `CLAUDE.md`.

## The loop at a glance
```
0  Intake      ── load flags.json (or take described issues); confirm which flags to work
1  Confirm     ── read the shared ADA facts; stand up one throwaway investigation worktree
2  Propose     ── ada-builder (MODE: propose) works out root cause + exact change per flag
3  Red-team    ── negative reviewers try to REFUTE each proposal; weak ones go back or die
4  Debt/frag   ── is it overcomplicating? is there a simpler, less-fragile root fix? consolidate
5  Re-validate ── run the proposed after-state past the original personas — does it fix THEIR pain?
6  Package     ── proposals.json + a forward-looking proposal report; nothing built
```

## Phase 0: Intake

Load the UAT run. If given a run-dir path, read its `flags.json`. If the user
described issues directly (no prior UAT), build a minimal flag set in the same
schema first, but say plainly that skipping the persona-grounded UAT means the
re-validation in Phase 5 is weaker.

Confirm with the user, in one short exchange: which flags to work this pass
(default: all `blocker` + `major`, plus any `minor` the user names), and that the
output is **proposals only — nothing gets built or shipped here.** Carry the
personas from `flags.json` forward; Phase 5 needs them.

## Phase 1: Confirm facts + stand up an investigation worktree

Read the shared ADA facts and re-confirm the load-bearing ones (Phase-1 reach
check per surface, current `CLAUDE.md` status/gotchas).

Stand up **one throwaway investigation worktree** so proposers read a clean, real
tree without touching the primary checkout:
`.claude/skills/ada-resolve-ticket/scripts/worktree.sh add propose/<surface>-<date>
<worktree_path>` (branches off `origin/main`; set `ADA_REPO` if needed). Nothing
is committed to it — it is investigation scaffolding, removed at the end.

## Phase 2: Propose a fix per flag (ada-builder, MODE: propose)

For each in-scope flag, delegate to the **`ada-builder`** agent in
`MODE: propose`. Give it the flag as a ticket-shaped brief (the friction, the
persona(s) who hit it, the `user_impact`, the `evidence` from `flags.json`), the
shared worktree path, its own run dir, and a reserved area (the files this flag's
fix would touch). Proposals are read-only investigation, so fan them out in
parallel. Each writes `proposal.md` — problem in one line, root cause, the exact
per-file change, the test it will add, blast radius, and an explicit
auto-promote-vs-park read (which `area.py` zone the files live in) — and returns
`PROPOSE OK: <summary>` or `PARK: <reason>`.

Keep proposals surgical and in the ADA voice — honest "not reporting" over raw
tokens, observe-don't-command, plain language over signal codes, and the R&F
source-of-truth rule. A `PARK` (needs BigQuery / ingestion / transform / a new
dependency) is not a dead end here the way it is in the resolver: **`ada-deploy-fixes`
has full data-layer authority**, so a park-class proposal is still valid — just
tag it `zone: data-layer` so the deploy stage applies its heavier gate. What you
must not do is force a frontend-only hack to dodge the honest fix.

## Phase 3: Red-team — negative reviewers try to poke holes

Every proposal must survive attack before it advances. Run two kinds of skeptic
in parallel per proposal, and **never let the proposer grade its own homework**:

1. **`ada-reviewer`** (viability + safety + park backstop) reviews `proposal.md`:
   does the change actually work, build, fit conventions, carry a real test, and
   not sprawl? It runs the park classifier
   `git -C <worktree> diff --name-only origin/main | python3
   .claude/skills/ada-resolve-ticket/scripts/area.py --stdin` where a diff exists,
   and returns `APPROVE` / `REVISE: <points>` / `ESCALATE: <reason>`.
2. **Negative testers** — 2–3 `general-purpose` agents, each explicitly told to
   **refute** the proposal, not endorse it, and to default to "does not hold" when
   uncertain. Give each a distinct lens so they don't all find the same thing:
   - *Correctness/regression:* what neighbouring behaviour breaks? what real
     `cip` data (via the read-only BigQuery MCP) makes this proposal wrong — a
     null, an over-100% value, a StackAdapt non-additive reach, a multi-platform
     project?
   - *Does it even solve the flag:* trace the fix back to the UAT evidence; if the
     friction the tester hit still happens, it fails regardless of elegance.
   - *Scope/blast radius:* does it quietly change a client-facing number, an
     aggregate, or a shared component beyond the reserved area?

   Each returns a verdict (`holds` / `refuted: <why>`) with evidence. If a
   **majority refute**, the proposal goes back to `ada-builder` for a revision
   round (cap 2) or is dropped. Prefer fewer, bulletproof proposals over many
   shaky ones.

## Phase 4: Code-debt and fragility pass

Before a surviving proposal is blessed, ask — per proposal and across the batch —
whether it is the *right shape*, not just a working patch. Load the `simplify`
skill for the lens, and run a dedicated `general-purpose` reviewer over the
surviving proposals with these questions:

- **Are we overcomplicating?** Does the fix add code, branches, or config where
  removing or consolidating would resolve the flag as well or better? A new
  special case that has to stay in sync with three others is debt, not a fix.
- **Is there a lower-fragility root fix?** Would fixing the cause kill the whole
  class instead of patching this instance? (E.g. several jargon flags across tabs
  are usually one `guardCopy()` / glossary consolidation, not N string patches;
  several cross-tab number mismatches are usually one shared scope-label helper,
  not per-tab captions.) Prefer the root fix when it is genuinely less fragile.
- **Consolidate the batch.** Look across all proposals: which flags are symptoms
  of one underlying debt? Merge them into a single, simpler proposal where that
  reduces total surface area — and say so explicitly.
- **Respect the existing seams.** ADA already has the right primitives
  (`guardCopy()`, `lib/glossary.ts`, `diagnosticVar`, the diagnostics voice
  guards). A fix that reuses them beats one that reinvents them. Reducing
  fragility can also mean *deleting* dead or duplicated code the flag exposed.

Record the debt/fragility read on each proposal (kept-as-is / simplified /
merged-into / rejected-as-overbuilt) and update `proposal.md` accordingly. This is
where the pipeline earns its keep: the cheapest fix to maintain is the one that
removes the problem's cause.

## Phase 5: Re-validate against the UAT personas

A proposal only advances if it removes the friction **for the person who hit it.**
For each surviving proposal, run the affected personas from `flags.json` back over
the *proposed after-state* (the described change, or a Recipe-2 localhost fixture
built to reflect it):

- Use **`ada-uat-reviewer`** to judge acceptance from the affected persona's
  chair — does this actually resolve the ticket's real-world problem in the user's
  terms? It returns `ACCEPT` or `REJECT: <the specific gap, as a user would state
  it>`.
- For the highest-value flags, additionally re-run the **original persona agent**
  (same identity, level, and ignorance as in Phase 3 of the UAT) against the
  after-state and confirm it no longer stalls where it did. An L1 who still can't
  read the board is a `REJECT` even if the code is perfect.

A `REJECT` goes back to `ada-builder` with the persona's exact gap (cap 2 rounds),
then is dropped and re-flagged for the report. The point: the fix has to land for
Sam-the-new-hire, not just pass tsc.

## Phase 6: Package the proposals (nothing built)

Write the results into the run dir for `ada-deploy-fixes`:

- **`proposals.json`** — one entry per advanced proposal (schema below): the flag
  it closes, the exact per-file change, the tests to add, the `zone`
  (`frontend` / `isolated-backend` / `data-layer`), the debt/fragility read, the
  red-team verdicts, and the persona re-validation result. Include the dropped
  proposals with why, so nothing is silently lost.
- **`proposal-report.html`** — a forward-looking companion to the UAT report,
  built the same way (load `dataviz` + `artifact-design`, publish as an Artifact +
  save to the run dir). For each proposal: the fix in plain terms; a **before →
  after chain diagram** showing the previously weak/blocked link now flowing
  (reuse the flag's `chain`); the debt/fragility verdict; and the persona
  sign-off. Add a batch-level **"debt reduced"** section naming any consolidations
  and anything deleted. Pitch it at the same non-coder ads/systems reader.

Then remove the throwaway worktree
(`.claude/skills/ada-resolve-ticket/scripts/worktree.sh remove <worktree_path>`).

**Output modes** (AskUserQuestion, doubles as go-ahead):
1. **Proposals only** (default) — hand back the report + the run-dir path; tell the
   user they can run `ada-deploy-fixes` on it next.
2. **Proposals + file Asana tickets** — file each advanced proposal as a ticket
   (Priority from severity; Ready For → Agent for `frontend`/`isolated-backend`,
   → Frazer for `data-layer`), with the proposal in the body. Confirm the list
   before creating; report GIDs.

Either way you stop here. Building and shipping is `ada-deploy-fixes`.

### `proposals.json` schema
```json
{
  "run_dir": "<path>",
  "source_flags": "flags.json",
  "date": "2026-07-16",
  "proposals": [
    {
      "id": "P1",
      "closes_flags": ["F1", "F4"],
      "title": "Lead diagnostics rows with the plain signal name",
      "zone": "frontend",
      "change": [
        { "file": "frontend/src/components/diagnostics/...tsx", "what": "render name before code; code → tooltip" }
      ],
      "tests": ["extend the diagnostics render test to assert name precedes code"],
      "debt_fragility": "merged F1+F4 — one label helper, not two patches; reuses lib/glossary.ts",
      "red_team": { "ada_reviewer": "APPROVE", "negatives": ["holds", "holds", "refuted->revised->holds"] },
      "persona_revalidation": { "ada_uat_reviewer": "ACCEPT", "persona_rerun": "p1 no longer stalls" },
      "status": "advanced"
    }
  ],
  "dropped": [
    { "closes_flags": ["F7"], "why": "no low-fragility fix; needs a data-model change deferred to Frazer" }
  ]
}
```

## Orchestration notes
- **Propose:** `ada-builder` (`MODE: propose`), one per flag, parallel, shared
  investigation worktree.
- **Red-team:** `ada-reviewer` + 2–3 `general-purpose` negative testers per
  proposal, parallel, distinct refutation lenses.
- **Debt/fragility:** one `general-purpose` reviewer over the surviving batch,
  with the `simplify` lens loaded.
- **Re-validate:** `ada-uat-reviewer` per proposal, plus the original persona
  agent re-run for the top flags.
- Keep the intake and output-mode gates in the main conversation; fan out the
  rest. Pass paths, not contents.

## Pitfalls
- **Author-graded proposals.** The proposer never reviews itself — that is what
  the negative testers and `ada-uat-reviewer` are for.
- **Elegant fixes that miss the user.** Passing tsc is not the bar; removing
  Sam-the-new-hire's confusion is. Persona re-validation is not optional.
- **Patching symptoms.** If three flags share one cause, propose the one root fix,
  not three patches — and delete the debt the flags exposed.
- **Overbuilding.** A fix that adds a special case needing constant sync with
  others is fragility. Prefer reusing ADA's existing primitives, or removing code.
- **Drifting into building.** You propose and vet only. No commits to `main`, no
  gates run to green here, no data touched — that is `ada-deploy-fixes`.
- **Losing dropped proposals.** Record what you dropped and why; a silent drop
  reads as "handled" when it was not.
