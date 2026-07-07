---
name: ada-resolve-ticket
description: Autonomously resolve ONE ADA Campaign Intelligence Platform ticket from Asana end to end -- claim it, propose, review, build, review, then either auto-promote an eligible fix to staging or park it for Frazer with a question. Use when Frazer says "resolve next ADA ticket", "run the ADA resolver", "ada resolve", "work the ADA queue", or when triggered headless. One invocation resolves exactly one ticket, then stops.
---

# ADA ticket resolver (one ticket per run)

You are the orchestrator. You resolve exactly one Asana ticket, then stop. You do NOT loop over the queue yourself: looping is the harness's job (see README). Keeping each run to one ticket is what stops the context from overflowing and what makes concurrent sessions safe.

Everything heavy (reading code, running gates, browsing) happens inside sub-agents whose output you never inline: they write an artifact to the run dir and hand you back one line plus a path. You pass paths, not contents.

`SKILL_DIR` below is the directory containing this file. Run every script as `"$SKILL_DIR/scripts/<name>"`. Preconditions: `ASANA_PAT` is set in the environment; the repo `.venv` exists; for auto-promote, either `gh` is authenticated or you have push access to `main`. **If those preconditions don't hold, you're running under Claude Code on the web — read "Operating notes" at the end first; the pipeline still applies but you drive Asana over MCP and deliver via a draft PR instead of `promote.sh`.**

## Autonomy boundary (read first)

- A ticket may **auto-promote to staging** only if every file it touches is frontend or isolated backend. `claim.py` computes this and returns `promote_decision: "auto"` or `"park"`, and `promote.sh` re-checks the real diff as a hard backstop.
- Anything touching BigQuery, schema, ingestion, or transform code (`ingestion/`, `infrastructure/bigquery/`, any `.sql`, `transformation.py`/`media_plan_sync.py`/`daily_job.py`/`creative_assets.py`), or landing in an unrecognised zone, parks. You build it and hand it to Frazer; you never deploy it. This is a hard rule, not a preference.
- **The diagnostics engine (`backend/services/diagnostics/`) auto-promotes as of 2026-07-07** (Frazer's request — he runs this skill only when comfortable auto-promoting engine changes). Note that A5/F3/etc. diagnostic strings render in Slack alert bodies, so a ticket that wants that copy vetted should say "needs review before staging" in its body — `claim.py`'s `notes_force_park` then parks it regardless of the file zone.
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
- `PASS` -> run `python3 "$SKILL_DIR/scripts/handoff.py" staged --gid <ticket_gid> --message-file "$RUN/summary.txt"` (write a 4-6 line summary first). This moves the ticket to **Ready In Staging**, sets Status = Completed and Stage = Launch, and flips Ready For -> Frazer for the prod promote.
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

## Operating notes (learnings)

**Two environments.** The scripts assume Frazer's Mac: `ASANA_PAT`, a repo `.venv`, `gh`/push-to-`main`. Under **Claude Code on the web** none of that holds, and `claim.py`/`handoff.py`/`promote.sh` will exit on the missing `ASANA_PAT`. There, run the same pipeline but:
- **Asana over MCP.** Select the queue yourself with the Asana MCP tools, replicating `claim.py`'s `filter_candidates` (Ready For = 🤖 Agent, not completed, Status ≠ Completed; order High→Low priority, then In-progress before Not-started). Set Status → In progress on claim; on finish, post the comment + flip Ready For with `update_tasks`. The single custom-field write is `{"1216308984626884": "1216308984626885"}` (Ready For → Frazer).
- **Bootstrap deps.** No `.venv` → `python3 -m pip install -r requirements.txt --ignore-installed` once, then run pytest per CLAUDE.md (`cd /tmp && PYTHONPATH=$REPO python3 -m pytest …`). `gates.sh` already falls back to system `python3` when there's no venv.
- **Deliver via draft PR, not `promote.sh`.** The web session is pinned to one designated branch and must NOT merge to `main`. So treat every fix — even an `auto` one — like a park you hand back: commit to the designated branch, push, open a **draft PR**, and post the Asana handoff (Ready For → Frazer) explaining that merging to `main` is his promote step. Fix the commit identity if the stop-hook flags it (`git config user.email noreply@anthropic.com && git commit --amend --reset-author`).

**Gate reality.** `backend/tests` is the authoritative suite and must be fully green. The top-level `tests/` tree carries a documented **stale baseline** (`scripts/known_stale_tests.txt`); `gates.sh` runs `tests/` but fails only on failures *outside* that baseline. Do NOT read a raw "N failed" from the full suite as RED — that would park every clean ticket. When you touch pacing/retro code, expect the maintained coverage to be the `backend/tests` copy.

**What made the fix land first-try** (worth repeating in `proposal.md` and to the builder):
- Reuse existing conventions instead of inventing constants — e.g. AI-044 reused D2's `FREQ_BANDS[fmt]["max"]` fatigue ceiling rather than a new threshold; both reviewers called this out as the reason to trust it.
- Fix contradictions at the source: read the *same field the UI shows* (frequency from `PlatformMetrics`, the Performance-tab number) so the two surfaces can't disagree.
- Keep new copy out of voice-guarded templates: `tests/test_diagnostics_voice.py` formats the base `A5_MESSAGES` with a fixed `{slope,days,worst_suffix}` contract — append new strings separately and extend the voice test rather than changing those templates.
- Gate on regression safety: an overlay that's inert unless new data is present (frequency > 0) keeps every existing fixture green.

**Pipeline value.** Running proposal-review + UAT in parallel, then build-review on the diff, is cheap and catches scope/voice gaps early; the UAT reviewer specifically verifies the *user's* acceptance bar (e.g. Frazer's "name the driver" follow-up), which a pure code review misses. Keep delegating the heavy read/verify to sub-agents and passing paths, not contents.
