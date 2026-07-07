---
name: ada-resolve-ticket
description: Autonomously resolve ONE ADA Campaign Intelligence Platform ticket from Asana end to end -- claim it, propose, review, build, review, then either auto-promote a frontend-only fix to staging or park it for Frazer with a question. Use when Frazer says "resolve next ADA ticket", "run the ADA resolver", "ada resolve", "work the ADA queue", or when triggered headless. One invocation resolves exactly one ticket, then stops.
---

# ADA ticket resolver (one ticket per run)

You are the orchestrator. You resolve exactly one Asana ticket, then stop. You do NOT loop over the queue yourself: looping is the harness's job (see README). Keeping each run to one ticket is what stops the context from overflowing and what makes concurrent sessions safe.

Everything heavy (reading code, running gates, browsing) happens inside sub-agents whose output you never inline: they write an artifact to the run dir and hand you back one line plus a path. You pass paths, not contents.

`SKILL_DIR` below is the directory containing this file. Run every script as `"$SKILL_DIR/scripts/<name>"`. Preconditions: `ASANA_PAT` is set in the environment; the repo `.venv` exists; for auto-promote, either `gh` is authenticated or you have push access to `main`.

## Autonomy boundary (read first)

- A ticket may **auto-promote to staging** only if every file it touches is frontend or isolated backend. `claim.py` computes this and returns `promote_decision: "auto"` or `"park"`, and `promote.sh` re-checks the real diff as a hard backstop.
- Anything touching BigQuery, schema, ingestion, transform code, or an unrecognised zone parks. You build it and hand it to Frazer; you never deploy it. This is a hard rule, not a preference.
- Never send Slack, email, or iMessage, and never post outside the ticket's own Asana comments.
- Production is never touched. The furthest you go is staging.

## Pipeline

### 0. Claim
Run `python3 "$SKILL_DIR/scripts/claim.py"`. It prints JSON.
- `{"status":"NO_TICKET"}` -> there is nothing you may work right now (queue empty, all busy, or all areas conflicting). Say so in one line and STOP.
- `{"status":"CLAIMED", ...}` -> capture `ticket_gid`, `title`, `notes`, `branch`, `worktree`, `reserved_area`, `promote_decision`, `promote_reasons`.

Set `RUN="<worktree>.run"` and `mkdir -p "$RUN"`. Everything below writes artifacts there.

Check for a **resume**: run `python3 "$SKILL_DIR/scripts/ledger.py" list` and read the ticket's latest Asana comments. If a prior park comment plus Frazer's answer are present (or the branch already has commits ahead of origin/main), this is a resume: skip Propose, fold his decision in, and continue from the phase that was blocked (usually Build or Promote). The branch already holds the WIP.

### 1. Worktree
`"$SKILL_DIR/scripts/worktree.sh" add "<branch>" "<worktree>"`. Fresh ticket branches off origin/main; a resume re-attaches to the existing branch.

### 2. Propose  (skip on resume if a proposal already stands)
Delegate to the **ada-builder** sub-agent in propose mode. Give it: the ticket title + notes, the worktree path, the run dir, `reserved_area`. It writes `proposal.md` and returns a verdict line.
- If the verdict is `PARK: <reason>` (the builder found the fix genuinely needs BQ/ingestion or a new dependency), go to **Park**.

### 3. Code review of the proposal
Delegate to **ada-reviewer** with the `proposal.md` path.
- `APPROVE` -> continue.
- `REVISE` -> send the points back to **ada-builder** (propose). Repeat at most **2** rounds (config `revision_round_cap`).
- `ESCALATE`, or cap reached -> **Park**.

### 4. UAT acceptance of the proposal
Delegate to **ada-uat-reviewer** with `proposal.md`.
- `ACCEPT` -> continue.
- `REJECT` -> back to **ada-builder** (propose), same 2-round cap, then **Park**.

### 5. Build
Delegate to **ada-builder** in build mode. It implements in the worktree, adds tests, runs `scripts/gates.sh`, commits, and writes `build-notes.md`.
- `BUILD gates GREEN` -> continue.
- `BUILD gates RED` -> back to **ada-builder** to fix, 2-round cap, then **Park** with the failing gate output.

### 6. Build review
Delegate to **ada-reviewer** with the diff (`git -C <worktree> diff origin/main`).
- `APPROVE` -> continue.
- `REVISE` -> back to **ada-builder** (build), 2-round cap, then **Park**.
- If the reviewer reports the diff hits a park path after all -> **Park**.

### 7. Promote or Park
- If `promote_decision == "park"`, or any phase raised a `DECISION:`/`PARK:` -> **Park** (the work is built and reviewed; Frazer decides). Do NOT deploy.
- Else (`auto`): run `"$SKILL_DIR/scripts/promote.sh" "<branch>" "<worktree>"`.
  - exit `0` -> deployed to staging; continue to Smoke.
  - exit `3` (rebase/gates failed after re-sync) -> one more Build round, then **Park**.
  - exit `4` (deploy lock busy) -> wait 60s and retry once; if still busy, **Park** ("staging deploy busy, retry later").
  - exit `5` (could not merge to main) -> **Park** ("branch pushed; needs your manual merge -- main looks protection-locked or gh is not authed").
  - exit `6` (the real diff touches a must-park path) -> **Park** ("classifier backstop caught a BigQuery/ingestion path"). Trust this over your own read.
  - any other non-zero exit -> **Park** with the captured output.

### 8. Smoke
Delegate to **ada-smoke**. It cookie-less-curls staging and checks the fix.
- `PASS` -> run `python3 "$SKILL_DIR/scripts/handoff.py" staged --gid <ticket_gid> --message-file "$RUN/summary.txt"` (write a 4-6 line summary first). This moves the ticket to **Ready In Staging**, sets Status = Completed, and flips Ready For -> Frazer for the prod promote.
- `FAIL` -> **Park** with `smoke.md` ("deployed to staging but smoke failed; not reverting"). Do not try to fix staging.

### 9. Close-out
`"$SKILL_DIR/scripts/worktree.sh" remove "<worktree>"`. Print one short paragraph to the main session: which ticket, and the outcome (promoted to staging / parked with the reason). Then STOP. Do not pick another ticket.

## Park protocol
A parked ticket must be resumable by a completely fresh session that has none of your context. So:
1. Write `"$RUN/park-message.txt"`: what was built, the branch name, gate status, and the exact question stated as options ("A or B, here is the trade-off"), plus `promote_reasons` if this is a BQ/ingestion park. Make it answerable in one word.
2. `python3 "$SKILL_DIR/scripts/handoff.py" park --gid <ticket_gid> --branch "<branch>" --worktree "<worktree>" --message-file "$RUN/park-message.txt"`. This commits and pushes the WIP FIRST (so the branch is resumable), then posts the comment, flips Ready For -> Frazer, keeps Status = In progress, and marks the ledger claim `parked` (its area stays reserved so nothing ships on top of it).
3. ONLY if that exited 0, remove the worktree: `"$SKILL_DIR/scripts/worktree.sh" remove "<worktree>"` (the branch and its commits survive on origin). If handoff.py park exited non-zero (the WIP push failed), do NOT remove the worktree; report that it needs manual attention and STOP.
4. Print one line and STOP.

## Resume protocol
When Frazer answers and flips Ready For back to Agent, the next run's `claim.py` re-claims the ticket (Status is still In progress, the ledger shows it parked). At step 0 you detect the parked ledger record plus his answer in the comments and continue from the blocked phase. The branch already holds the WIP.

## Concurrency invariants (do not violate)
- One ticket, one worktree, one branch. Never edit the primary tree or another worktree.
- The claim (step 0) is the only place tickets are selected; it is atomic and area-aware, so two sessions can never take the same ticket or the same module.
- The deploy step is serialised by a global lock inside `promote.sh`; it also rebases onto origin/main before merging, so later sessions ship on top of earlier ones.
- If you abort for any reason before parking or staging, release the claim: `python3 "$SKILL_DIR/scripts/handoff.py" release --gid <ticket_gid>` and remove the worktree, so the ticket is not stuck.
