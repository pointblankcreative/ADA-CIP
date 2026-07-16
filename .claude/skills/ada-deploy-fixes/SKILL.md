---
name: ada-deploy-fixes
description: >-
  Take vetted fix proposals for the ADA Campaign Intelligence Platform and
  actually ship them to staging — with full authority over both the front-end
  layer and the data (BigQuery) layer. It builds each fix, runs the project
  gates, then (behind a hard human-confirmation gate) pushes and merges to main
  so staging auto-deploys, executes any data-layer/schema/migration change
  behind a cost gate (it estimates BigQuery spend and stops before an expensive
  job — the data itself is recoverable from Funnel, but processing is real money),
  and verifies in STG that the fix landed and nothing obvious broke.
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

- **The real risk here is COST, not data loss.** The `cip` tables are *derived* —
  the raw data lives in Funnel (`core_funnel_export.funnel_data`) and the `cip`
  dataset is rebuilt from it by the transformation/backfill. So a bad data write
  is **recoverable** (re-run the transformation) and data destruction is low risk.
  What is NOT cheap to undo is money: a large or looped BigQuery job can run up
  **hundreds of dollars** of Google Cloud processing. That is the thing to guard.
  **Estimate every BigQuery job's cost before running it, and hard-stop on
  anything expensive** (Phase 3). Cheap data work proceeds without ceremony.
- **Staging and production SHARE the single `cip` dataset**, so a "staging" data
  change is visible in production immediately. Because it is recoverable this is
  not a reason to freeze — but it IS a reason not to leave a user-facing table
  broken: before a destructive overwrite, take the cheap same-region snapshot so
  recovery is instant rather than a costly re-transform.
- **Terminal state is staging-verified.** Merging to `main` deploys STAGING
  (~7 min via `deploy.yml`). Promoting `main` → `production` is a separate manual
  push and is **never** done by this skill.
- **Confirm gate before the expensive or the deploy-shaped steps** — before any
  push/merge, and before any BigQuery job whose estimated cost crosses the
  threshold (Phase 3). No surprise deploys, and no surprise cloud bills.

Read the shared ADA facts first:
`.claude/skills/ada-simulate-uat/references/ada-project-facts.md`. Re-confirm the
load-bearing ones (branch model, gates, guardrails, the environment reality) and
the current `CLAUDE.md` status.

## The loop at a glance
```
0  Intake     ── load proposals.json; classify each: frontend / isolated-backend / data-layer
1  Preflight  ── facts + gates + a COST estimate per BigQuery job; stand up the ship worktree/branch
2  Build      ── ada-builder (MODE: build) for code; author + dry-run (don't run) data migrations; gates GREEN
3  CONFIRM    ── confirm the deploy; gate BigQuery jobs on COST (estimate first, hard-stop if expensive)
4  Deploy     ── merge to main → watch staging deploy green; snapshot then execute the (gated) migrations
5  Verify     ── deploy green + ada-smoke + data readback + re-run the fix's scenario off merged main
6  Report     ── what shipped, verification, rollback location; hand the prod promote to Frazer
```

## Phase 0: Intake and classification

Load the run dir's `proposals.json`. For each advanced proposal, classify by its
`zone`:

- **`frontend`** / **`isolated-backend`** — code-only, auto-zone (the resolver's
  `area.py` would auto-promote it). Lowest risk.
- **`data-layer`** — touches BigQuery, schema, ingestion, transform, a `.sql`, or
  the migration dir. The data is recoverable (it rebuilds from Funnel), so the
  risk here is **cost**, not loss: these are the jobs that can run up a Google
  Cloud bill, so each is cost-estimated and gated in Phase 3.

Confirm the batch with the user in one exchange: which proposals ship this run,
and an explicit acknowledgement that data-layer items run real BigQuery jobs
against the shared `cip` dataset (recoverable from Funnel, but they cost
processing). If `proposals.json` is missing a clean `zone`, re-derive it from the
files each proposal touches — do not guess.

## Phase 1: Preflight

- Read the shared ADA facts; re-confirm the gates and guardrails.
- Stand up **one ship worktree + branch** for the batch:
  `.claude/skills/ada-resolve-ticket/scripts/worktree.sh add deploy/<surface>-<date>
  <worktree_path>` (branches off `origin/main`; set `ADA_REPO` if needed).
- **Estimate cost for every BigQuery job you will run** (reads included — a big
  scan costs whether or not it writes). Use a dry run to get bytes scanned
  (BigQuery `dry_run` via the Python client, or read table sizes from
  `get_table_info` / `INFORMATION_SCHEMA`) and convert to dollars — on-demand
  analysis bills per TiB scanned (confirm the project's current rate/edition; it
  is on the order of a few dollars per TiB). Record a per-job estimate and a
  running total for the whole deploy. This number is what the Phase 3 gate turns
  on.
- **Reversibility is cheap here — take the cheap insurance, skip the expensive
  paranoia.** For a destructive overwrite of a *user-facing* `cip` table, plan a
  same-region snapshot first (a table clone, or
  `CREATE TABLE cip.<table>_bak_<YYYYMMDD> AS SELECT * FROM cip.<table>` — same
  dataset/region, legal unlike cross-region DML) so recovery is instant. The
  ultimate backstop is Funnel: the `cip` tables rebuild from
  `core_funnel_export.funnel_data` by re-running the transformation — but that
  re-transform itself costs processing, which is one more reason to price and get
  the change right the first time.

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
  from a US-region source**). **Dry-run it** to get its cost estimate, and prune
  it so it scans as little as possible (explicit columns, partition/date filters —
  never `SELECT *` over `funnel_data`). Commit the migration script to the branch.
  **Do NOT execute it yet** — execution happens only after the Phase 3 cost gate.
  Also write the snapshot step from your Phase 1 plan as an explicit, runnable
  command.
- Honour every guardrail while building: keep the backend IAM-private (never open
  a cookie-less path; `deploy.yml` re-asserts `--invoker-iam-check`), preserve the
  `media_plan_sync.py` em-dashes and its `_clear_existing_plan()` destructiveness
  awareness, register any new double-read of `media_plan_lines` in
  `tests/test_plan_id_dedup_guard.py`, keep the R&F source-of-truth rule, add no
  secrets and no heavy dependency, keep the CORS format intact.

## Phase 3: The confirm gate (cost-first)

Two kinds of thing can leave this session: a **staging deploy** (push/merge to
`main`) and **BigQuery jobs**. Gate them differently.

**Always confirm the deploy itself.** Show the code diff summary, which flags each
change closes, the gate results (tsc + pytest GREEN; any RED called out), and the
target (**merge to `main` = staging**; production stays a manual Frazer promote).
Get an explicit go-ahead before you push/merge.

**Gate BigQuery jobs on COST, not on the fact that they write** — the data is
recoverable from Funnel, so a small write is not worth a ceremony, but an
expensive job is exactly what puts the user "in trouble." Each job already has a
dollar estimate from Phase 1. Then:

- **Cheap — estimate ≤ ~$1 (configurable):** run it without ceremony.
- **Notable — > ~$1 and ≤ ~$20:** show the estimate (bytes scanned → dollars),
  what the job does, and the running total; get a quick confirm.
- **Expensive — > ~$20, OR the running total for the deploy would exceed ~$25:**
  **hard stop.** Spell out the estimate and *why* it is that big, and get an
  explicit, specific sign-off before running. Never let a job approach "hundreds
  of dollars" without this — that is the exact outcome this gate exists to prevent.
- Thresholds are defaults — **ask the user for their cap** if they have one, and
  respect it for the run.

**One extra light check for destructive-but-cheap ops:** a TRUNCATE / full
overwrite / DROP of a *user-facing* `cip` table is recoverable but leaves the live
dashboards wrong until recovery, so confirm it (and take the snapshot) even when
it is cheap. Additive or non-destructive low-cost writes need no gate.

If the user approves only some items, ship those and hold the rest. The whole
point of this gate is that nobody is surprised by a cloud bill or a broken
dashboard.

## Phase 4: Deploy to staging

Only after approval, and in this order:

1. **Code → staging.** Push the branch (`git push -u origin <branch>`; retry with
   backoff on network errors), open a PR to `main`, and **merge it** (GitHub MCP
   `merge_pull_request`). Merging `main` triggers the **Deploy CIP** workflow.
   Watch that workflow run to completion (GitHub Actions `list`/`get`); a red
   deploy is a stop-and-report, not a retry-blindly.
2. **Data-layer → after code is green (or independently if code-free), and only
   after the job cleared the Phase 3 cost gate.** For a destructive overwrite of a
   user-facing table, take the cheap snapshot first (run the clone/CTAS; confirm it
   exists via the read-only MCP). Then execute the migration with the **writable**
   `mcp__Google_Cloud_BigQuery__execute_sql` (the one tool the rest of the pipeline
   never uses) or the Python client, honouring the cross-region rule. Execute it
   **once**; never re-run a non-idempotent migration, and **never loop a big job to
   "retry"** — that multiplies the bill. After it runs, record the **actual** bytes
   billed and add it to the running total (dry-run estimates can be low). If it
   errors midway, stop, report, and recover from the snapshot or by re-running the
   transformation from Funnel — do not fire off repeated repair attempts against
   live data (each one costs).

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
- **Cost is the primary gate.** Estimate every BigQuery job before running it;
  cheap jobs proceed, costly ones (Phase 3 thresholds) stop for sign-off, and a
  single deploy never quietly runs up toward "hundreds of dollars." Track the
  running total and count the *actual* bytes billed after each run.
- **Data is recoverable, so don't freeze — but don't be reckless.** The `cip`
  tables rebuild from Funnel (`core_funnel_export.funnel_data`); a bad write is
  recoverable. Snapshot before a destructive overwrite of a user-facing table so
  recovery is instant, not a costly re-transform.
- **Staging only.** Merge to `main`; never push `production`; never auto-promote.
- **IAP stays private.** A protected route answering `200` unauthenticated is a
  hard fail — `ada-smoke` exists to catch exactly this.
- **Cross-region DML ban.** No DML against Montreal `cip` from a US-region source;
  use the `SELECT` + `load_table_from_json()` pattern or in-region operations.
- **Guarded behaviours.** `media_plan_sync.py` em-dashes; `_clear_existing_plan()`
  destructiveness; `tests/test_plan_id_dedup_guard.py`; `test_diagnostics_voice.py`;
  the R&F source-of-truth rule; CORS format; no secrets; no heavy dependency.
- **Every migration runs once**, pruned to scan as little as possible, dry-run
  first, and never looped to retry.

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
- **Running up a cloud bill.** The single most dangerous mistake here is an
  unestimated or looped BigQuery job that scans terabytes — a full-table
  `SELECT *` on `funnel_data`, a backfill re-run in a loop, an expensive migration
  nobody priced. Estimate first, gate on cost, watch the running total. "Hundreds
  of dollars" is the outcome to prevent.
- **Confusing recoverable with free.** Data loss is recoverable from Funnel, so
  don't freeze on it — but recovery is a re-transform, which itself costs. Get the
  change right; snapshot before a destructive overwrite so recovery is a restore,
  not a re-scan.
- **Verifying what you can't reach.** Don't claim the live IAP UI works — you
  can't see it. Verify deploy health, smoke, data readback, and the localhost
  re-run of the shipped code; hand the live-URL confirm to Frazer.
- **Re-running a migration.** Non-idempotent migrations run once. On a mid-way
  error, recover from the snapshot or Funnel — never loop repair attempts against
  live data (each one costs).
- **Creeping to production.** Terminal is staging-verified. The prod promote is
  always Frazer's manual step.
- **Deploying by surprise.** The Phase 3 deploy confirm is not a formality; get the
  go-ahead before you push/merge.
