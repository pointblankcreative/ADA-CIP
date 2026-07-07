# ADA ticket resolver

An autonomous, concurrency-safe loop that drains the ADA "Ready For: Agent" queue in
Asana. One Claude Code session resolves one ticket end to end (propose, review, build,
review, then auto-promote an eligible fix to staging or park it for you with a
question), then stops. A thin outer loop fires one fresh session per ticket, so context
never overflows and several sessions can run at once without colliding.

## Layout

```
<repo>/.claude/skills/ada-resolve-ticket/
  SKILL.md              orchestration: the full pipeline the session follows
  config.json           GIDs, paths, gate commands, the park/auto area lists
  README.md             this file
  scripts/
    ledger.py           atomic claim ledger + area-overlap + status board (CLI: list/gc)
    area.py             classify touched files -> auto-promote vs must-park
    claim.py            pick + atomically claim the next ticket (--peek for the loop)
    asana.py            minimal Asana REST helpers (stdlib, uses ASANA_PAT)
    handoff.py          end-of-run Asana + ledger transitions (park / staged / release)
    worktree.sh         create/remove a per-session git worktree
    gates.sh            the real merge gates: synchronous tsc + backend pytest
    promote.sh          deploy mutex + rebase onto main + gates + merge -> staging
    test_*.py           dependency-free unit tests (ledger, area, claim)
<repo>/.claude/agents/
  ada-builder.md        proposes + implements (Opus)
  ada-reviewer.md       independent code review (Sonnet)
  ada-uat-reviewer.md   acceptance check as the affected persona (Sonnet)
  ada-smoke.md          post-deploy smoke on staging (Sonnet)
<repo>/resolve-loop.command   double-click drain loop (one fresh session per ticket)
```

## Prerequisites

- `ASANA_PAT` exported in the environment (the same token deploy.sh / list_ingest use).
- For auto-promote: either `gh` authenticated, or push access to `main`. If neither, the
  run parks with "needs manual merge" rather than failing (safe by design).
- The repo `.venv` present, and `frontend/node_modules` installed once (worktrees symlink it).
- Claude Code on your Max plan. Headless `claude -p` runs draw the monthly Agent SDK credit,
  not your interactive allowance.

## Triggering

- One ticket, interactively: say "resolve the next ADA ticket" (or run the skill) in a
  Claude Code session opened at the repo.
- One ticket, headless: `claude -p "Use the ada-resolve-ticket skill to resolve the next ADA ticket."`
- Drain the queue: double-click `resolve-loop.command` (or `./resolve-loop.command 5` to cap
  it at 5 tickets). It runs one fresh session per claimable ticket and stops when none remain.

## Asana state model

Two fields carry the whole state. "Ready For" is whose court the ball is in; "Status" is
where in the pipeline it sits.

| Ready For | Status | Meaning |
|-----------|--------|---------|
| Agent | Not started | new: the loop will pick it up |
| Agent | In progress | being worked now, or a resume you just handed back |
| Frazer | In progress | parked mid-flight: a comment on the ticket asks you something |
| Frazer | Completed (+ section "Phase 3: Ready In Staging", Stage "Launch") | shipped to staging; only your prod promote remains |

The claim step sets Status -> In progress. Park flips Ready For -> Frazer and leaves a
self-contained question. A successful ship moves the ticket to "Ready In Staging", sets
Status Completed and Stage Launch, and flips Ready For -> Frazer for your prod sign-off.

## Autonomy boundary

Frontend, isolated-backend, and (since 2026-07-07) diagnostics-engine fixes auto-promote to
staging. Anything touching BigQuery, schema, ingestion, transform code, or an unrecognised
zone parks for you. The rule lives in `config.json` (`park` / `auto` lists) and is
deliberately conservative: unknown zone parks. Plain version: frontend + the diagnostics
engine sail, anything that can move data or change schema stops. (The "Buy Type" ticket
touches `media_plan_sync.py`, so it correctly parks, exactly as its own body already flags.)

`backend/services/diagnostics/` was moved from park → auto at Frazer's request — he runs
the skill only when he's comfortable auto-promoting engine changes. Two safety valves remain
for that zone: a diagnostics change that also touches a `.sql`/transform file still parks,
and any ticket whose body says "needs review before staging" (or "do not auto-promote")
parks regardless of file zone via `claim.py`'s `notes_force_park` — diagnostic strings render
in Slack alert bodies, so that's the hook for copy that wants a human read first.

### Gate baseline

`backend/tests` is the authoritative suite and must be fully green. The top-level `tests/`
tree carries documented stale failures (`scripts/known_stale_tests.txt`); `gates.sh` runs it
but fails only on failures *outside* that baseline, so pre-existing rot doesn't park clean
tickets. Trim entries from that file (or empty it) as the duplicate pacing/retro tests are
reconciled — when it's empty, `tests/` becomes a hard gate again.

### Running under Claude Code on the web

The scripts assume the Mac (`ASANA_PAT`, `.venv`, `gh`/push-to-`main`). In a web session
those are absent and `claim.py`/`handoff.py`/`promote.sh` exit on the missing token — drive
Asana over the Asana MCP tools instead, `pip install -r requirements.txt` for the gate, and
push each fix to the designated branch. An `auto` ticket is then **merged to `main` via the
GitHub MCP (`merge_pull_request`)** — the web equivalent of `promote.sh`, since a push to
`main` is what deploys staging — followed by the `staged` Asana handoff; a `park` ticket is
handed back as a **draft PR** (Ready For → Frazer), never merged. If the merge is refused
(branch protection / token permission), it falls back to the draft-PR hand-off. Production is
still never touched. See SKILL.md "Operating notes".

## Concurrency

- One ticket, one git worktree, one branch. Separate directories sharing one `.git`, so
  sessions physically cannot corrupt each other's tree.
- The claim (in `claim.py`, under a file lock) is the only place tickets are selected: it is
  atomic and area-aware, so two sessions never take the same ticket or the same module.
  Disjoint modules run in parallel; overlapping ones serialise on their own.
- `promote.sh` holds a global deploy mutex and rebases onto origin/main before merging, so
  concurrent ships serialise and later ones build on earlier ones.
- See what is running at any moment: `python3 scripts/ledger.py list`. Reap a crashed
  session's claim: `python3 scripts/ledger.py gc`.

## Park and resume (your side)

When a run parks, you get an Asana comment stating what was built, the branch, and the exact
question as options. Answer in the ticket, then flip "Ready For" back to Agent. The next
drain re-claims it (its branch still holds the work), reads your answer, and continues from
where it stopped. You are only ever pulled in for the decision, never to babysit the loop.

## Confirm before your first live auto-promote

1. How `main` merges. `promote.sh` tries `gh pr merge --admin`, then a direct push to `main`.
   If `main` is protection-locked and `gh` is not set up, it parks with "needs manual merge"
   (branch already pushed). Decide whether to enable `gh` admin auto-merge or keep auto-promote
   as "push the branch, you click merge".
2. `ASANA_PAT` covers the ADA board (project 1215988273595218).
3. Supervise the first few. Run `./resolve-loop.command 1` and watch one ticket through before
   trusting a full drain.

## Known v1 assumptions and limits

- `deploy.yml` runs no test job, so the gates (`tsc --noEmit` + pytest from /tmp with
  PYTHONPATH) run inside the pipeline before promote. They are the merge gate.
- Smoke is cookie-less `curl` against staging (auth must return 401/403). Add Claude-in-Chrome
  for authenticated visual QA later if you want it.
- Best parallelism when each ticket declares the files it touches (a `Touches:` line, or paths
  in the body). Tickets with no declared files reserve both source roots and serialise.
- Revision loops cap at 2 rounds, then park. Ledger and run dirs live under `/tmp` (ephemeral);
  durable state is the branch plus Asana, so a fresh session can always resume.
- Cost: reviewers run on Sonnet, the builder on Opus (set in the agent files). Cap tickets per
  drain with the loop argument.

## Tests

```
cd scripts && python3 test_ledger.py && python3 test_area.py && python3 test_claim.py
```
(Also pytest-compatible. 15 + 15 + 7 checks covering overlap, claim lifecycle, parked
reservation, resume, crash self-heal, lock exclusivity, the park classifier, and candidate
selection.)
