---
name: ada-deploy-fixes
description: >-
  Take vetted fix proposals for the ADA Campaign Intelligence Platform and
  actually ship them to staging — with full authority over both the front-end
  layer and the data (BigQuery) layer. It builds each fix, runs the project
  gates, then (behind a hard human-confirmation gate) pushes and merges to main
  so staging auto-deploys, executes any data-layer/schema/migration change
  safely, and verifies in STG that the fix landed and nothing obvious broke.
  Deploys stop at staging-verified; promoting to production stays a manual step.
  Use this after ada-propose-fixes (it reads the run's proposals.json), or when
  the user says "deploy the fixes", "ship these to staging", "deploy and verify
  in STG", or "roll out the proposed fixes".
---

# ADA Deploy Fixes

You are stage three of the ADA UAT pipeline. Stages one and two found the
friction and produced **vetted, red-teamed, persona-approved proposals**. Your
job is to build them, ship them to **staging**, and confirm in STG that they
actually worked.

```
ada-simulate-uat   → flags.json
ada-propose-fixes  → proposals.json (vetted proposals)
ada-deploy-fixes   → build → deploy to staging → verify (YOU ARE HERE)
```

You have **full authority over the front-end layer AND the data (BigQuery)
layer** — this skill is *not* bound by the resolver's park boundary. That
authority is exactly why the gates below are non-negotiable.

**Read this before doing anything, and never let it out of sight:**

- **Staging and production SHARE the single `cip` BigQuery dataset.** A "staging"
  data change is a change to **live production data**. There is no separate
  staging warehouse to practise on. Treat every BigQuery write as a production
  write.
- **Terminal state is staging-verified.** Merging to `main` deploys STAGING
  (~7 min via `deploy.yml`). Promoting `main` → `production` is a separate manual
  push and is **never** done by this skill.
- **Hard confirm gate before every irreversible step** — before any push/merge,
  and before **every** BigQuery write — you stop, show exactly what will happen and
  its blast radius, and get explicit human approval. No surprise deploys, no
  surprise data writes.

Read the shared ADA facts first:
`.claude/skills/ada-simulate-uat/references/ada-project-facts.md`. Re-confirm the
load-bearing ones (branch model, gates, guardrails, the environment reality) and
the current `CLAUDE.md` status.

## The loop at a glance
```
0  Intake     ── load proposals.json; classify each: frontend / isolated-backend / data-layer
1  Preflight  ── facts + gates + reversibility plan; stand up the ship worktree/branch
2  Build      ── ada-builder (MODE: build) for code; author (don't run) data migrations; gates GREEN
3  CONFIRM    ── HARD gate: show the diff, gates, and every data write + its prod impact + rollback
4  Deploy     ── merge to main → watch staging deploy green; take backup then execute data migrations
5  Verify     ── deploy green + ada-smoke + data readback + re-run the fix's scenario off merged main
6  Report     ── what shipped, verification, rollback location; hand the prod promote to Frazer
```

## Phase 0: Intake and classification

Load the run dir's `proposals.json`. For each advanced proposal, classify by its
`zone`:

- **`frontend`** / **`isolated-backend`** — code-only, auto-zone (the resolver's
  `area.py` would auto-promote it). Lowest risk.
- **`data-layer`** — touches BigQuery, schema, ingestion, transform, a `.sql`, or
  the migration dir. Highest risk: it mutates the shared `cip` dataset (= prod
  data). Every one of these gets the heavy gate in Phase 3/4.

Confirm the batch with the user in one exchange: which proposals ship this run,
and an explicit acknowledgement that data-layer items touch live production data.
If `proposals.json` is missing a clean `zone`, re-derive it from the files each
proposal touches — do not guess.

## Phase 1: Preflight

- Read the shared ADA facts; re-confirm the gates and guardrails.
- Stand up **one ship worktree + branch** for the batch:
  `.claude/skills/ada-resolve-ticket/scripts/worktree.sh add deploy/<surface>-<date>
  <worktree_path>` (branches off `origin/main`; set `ADA_REPO` if needed).
- Write a **reversibility plan** for every data-layer proposal *now*, before any
  build: the exact backup you will take (a same-region snapshot/clone or
  `CREATE TABLE cip.<table>_bak_<YYYYMMDD> AS SELECT * FROM cip.<table>` — same
  dataset/region, so it is legal unlike cross-region DML), and the exact rollback
  (restore from the backup). If a change cannot be cleanly reversed, say so loudly
  — that raises the bar at the confirm gate.

## Phase 2: Build (no deploy yet)

Build each proposal in the ship worktree. **Nothing leaves the worktree in this
phase** — no push, no merge, no BigQuery write.

- **Code proposals (`frontend` / `isolated-backend`):** delegate to
  **`ada-builder`** in `MODE: build`, one per proposal, **sequential** into the
  shared branch (they share one worktree). Each implements its change, adds/updates
  tests, runs the gates
  (`.claude/skills/ada-resolve-ticket/scripts/gates.sh <worktree>` — tsc
  `--noEmit` + both pytest tiers with the stale-baseline allowance), and commits
  **only when gates are GREEN**, returning `BUILD gates GREEN` / `BUILD gates RED`.
  A RED build stays out of the batch and is reported, not shipped.
- **Data-layer proposals:** author the migration as a script under
  `infrastructure/bigquery/migrations/<YYYY-MM-DD>_<slug>.sql` (or a Python
  migration using the `SELECT` + `load_table_from_json()` client pattern where the
  cross-region DML ban applies — **DML cannot target the Montreal `cip` tables
  from a US-region source**). Commit the migration script to the branch. **Do NOT
  execute it yet** — execution happens only after the Phase 3 gate. Also write the
  backup step from your Phase 1 plan as an explicit, runnable command.
- Honour every guardrail while building: keep the backend IAM-private (never open
  a cookie-less path; `deploy.yml` re-asserts `--invoker-iam-check`), preserve the
  `media_plan_sync.py` em-dashes and its `_clear_existing_plan()` destructiveness
  awareness, register any new double-read of `media_plan_lines` in
  `tests/test_plan_id_dedup_guard.py`, keep the R&F source-of-truth rule, add no
  secrets and no heavy dependency, keep the CORS format intact.

## Phase 3: The confirm gate (HARD — do not skip)

Before anything leaves this session, stop and show the user, in one place:

- **The code diff summary** and which flags each change closes.
- **The gate results** (tsc + pytest GREEN per proposal; any RED called out).
- **Every data-layer operation, spelled out**: the exact SQL/migration, the table
  it writes, **that it mutates the shared `cip` dataset and therefore live
  production data**, the backup that will be taken first, and the rollback command.
- **The deploy target**: merge to `main` = **staging**; production stays a manual
  Frazer promote.

Get an **explicit, specific approval**. For data-layer items, the approval must
name them — a blanket "looks good" is not consent to write production data. If the
user approves only the code items, ship those and hold the data items. This gate
is the whole reason the skill is allowed its authority; a surprise deploy or a
surprise data write is the one outcome that breaks trust.

## Phase 4: Deploy to staging

Only after approval, and in this order:

1. **Code → staging.** Push the branch (`git push -u origin <branch>`; retry with
   backoff on network errors), open a PR to `main`, and **merge it** (GitHub MCP
   `merge_pull_request`). Merging `main` triggers the **Deploy CIP** workflow.
   Watch that workflow run to completion (GitHub Actions `list`/`get`); a red
   deploy is a stop-and-report, not a retry-blindly.
2. **Data-layer → after code is green (or independently if code-free).** Take the
   backup first (run the snapshot/CTAS from your plan; confirm it exists via the
   read-only MCP). Then execute the migration with the **writable**
   `mcp__Google_Cloud_BigQuery__execute_sql` (the one tool the rest of the pipeline
   never uses) or the Python client, honouring the cross-region rule. Execute it
   **once**; never re-run a non-idempotent migration. If it errors midway, stop,
   report, and use the backup — do not improvise a repair against live data.

Never touch `production`. Never run a full sync/backfill as a "fix" (the Full
History Backfill TRUNCATEs `fact_digital_daily`; media-plan sync DELETEs manual
edits) unless that *is* the approved, backed-up proposal.

## Phase 5: Verify in STG

Confirm the fix actually landed and nothing obvious broke. Be honest about what is
and isn't reachable from here (the live IAP UI is not):

- **Deploy health:** the Deploy CIP workflow is green, and both health checks +
  the IAM-lock/`--invoker-iam-check` steps passed.
- **`ada-smoke`:** run the post-staging cookie-less IAP curl. Protected routes
  must return `401`/`403`; a `200` on a protected route means IAP broke — a hard
  fail, roll back. It returns `PASS` / `FAIL`.
- **Data-layer readback:** verify the change with the **read-only** BigQuery MCP —
  query the mutated table and confirm it matches the expected post-state (row
  counts, the specific values, no unintended rows). This is fully checkable from
  here.
- **Behavioural check of the shipped code:** you cannot poke the live IAP-guarded
  staging URL, but you *can* exercise the exact code now in staging — rebuild a
  **Recipe-2 localhost UI off the merged `main`**, seed it with real data from the
  read-only MCP, and re-run the affected UAT scenario / persona against
  `localhost`. If the persona no longer stalls where it did, the shipped code
  behaves. State plainly that the final confirm *on the live IAP staging URL*
  remains a **Recipe-1 human** step (Frazer) and hand that off explicitly rather
  than claiming a verification you couldn't perform.

If any check fails: roll back (revert the merge commit and re-merge to redeploy;
restore the data table from its backup), report what happened, and stop.

## Phase 6: Report and terminal state

Write `deploy-log.md` into the run dir and summarise back:

- What shipped (each proposal → its commit/PR, the flags it closes).
- Verification results (deploy green, `ada-smoke` PASS, data readback, the
  localhost behavioural re-run), and honestly what still needs the Recipe-1 human
  confirm on the live staging URL.
- For every data change: the **backup table location** and the exact **rollback
  command**, so recovery is one step.
- What remains: the **manual `main` → `production` promote is Frazer's**, once he
  is satisfied with staging. Do not do it, and do not schedule it.

Remove the ship worktree when done. Terminal state: the fixes are live and
verified **in staging**, production is untouched, and every data change is
reversible.

## Guardrails (the hard lines)
- **Shared dataset = production data.** Every BigQuery write is a prod-data write;
  gate it, back it up, make it reversible.
- **Staging only.** Merge to `main`; never push `production`; never auto-promote.
- **IAP stays private.** A protected route answering `200` unauthenticated is a
  hard fail — `ada-smoke` exists to catch exactly this.
- **Cross-region DML ban.** No DML against Montreal `cip` from a US-region source;
  use the `SELECT` + `load_table_from_json()` pattern or in-region operations.
- **Guarded behaviours.** `media_plan_sync.py` em-dashes; `_clear_existing_plan()`
  destructiveness; `tests/test_plan_id_dedup_guard.py`; `test_diagnostics_voice.py`;
  the R&F source-of-truth rule; CORS format; no secrets; no heavy dependency.
- **Every migration runs once, idempotent where possible, backed up always.**

## Orchestration notes
- **Build:** `ada-builder` (`MODE: build`, runs `gates.sh`), sequential into the
  shared ship branch.
- **Deploy:** you (main loop) — push, `merge_pull_request`, watch Actions;
  writable BigQuery MCP / Python client for data, behind the Phase 3 gate.
- **Verify:** `ada-smoke` for the IAP curl; read-only BigQuery MCP for data
  readback; a `general-purpose` (or the original persona) agent for the localhost
  behavioural re-run.
- Keep the intake and the Phase 3 confirm gate in the main conversation. Pass
  paths, not contents.

## Pitfalls
- **Forgetting the dataset is shared.** "It's just staging" is false for data —
  it's production. The single most dangerous assumption in this skill.
- **Deploying by surprise.** The Phase 3 gate is not a formality; data-layer items
  need named consent, not a blanket nod.
- **Verifying what you can't reach.** Don't claim the live IAP UI works — you
  can't see it. Verify deploy health, smoke, data readback, and the localhost
  re-run of the shipped code; hand the live-URL confirm to Frazer.
- **Re-running a migration.** Non-idempotent migrations run once. On a mid-way
  error, restore from backup — never improvise against live data.
- **Creeping to production.** Terminal is staging-verified. The prod promote is
  always Frazer's manual step.
- **No way back.** If you can't articulate the rollback before you write, you are
  not ready to write.
