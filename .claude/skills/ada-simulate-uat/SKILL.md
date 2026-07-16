---
name: ada-simulate-uat
description: >-
  Run a loop of simulated User Acceptance Testing (UAT) tuned for the ADA
  Campaign Intelligence Platform — the internal political-ad reporting stack
  (Flightdeck triage home, the per-project tabs — Summary, Pacing, Creative,
  Audiences, Diagnostics — the Diagnostics triage board, Alerts, and the Admin
  backfill flow). Recruit AI tester personas across an ad-ops expertise
  spread — from a new agency hire up through Frazer the builder — have them
  exercise a defined ADA surface the way real users would, categorise the
  friction, then either write a report or (at the user's choice) file Asana
  tickets or hand back a staging draft PR through the ada-builder / ada-reviewer
  / ada-uat-reviewer / ada-smoke pipeline. Use this whenever the user wants to
  run UAT, simulate user testing, find the friction in this flow, test this
  like real users would, run persona-driven test loops, stress an ADA surface
  for usability or correctness, or says things like "run UAT", "test this like
  real users would", "find the friction in this flow", or "simulate user
  testing".
---

# ADA Simulate UAT

You are running a User Acceptance Testing loop against the ADA Campaign
Intelligence Platform: a set of AI tester personas exercise an ADA surface like
real users, you gather their narratives, categorise the friction, and then
either report it, file it as Asana tickets, or fix it — looping until the value
dries up or context gets tight.

The whole thing is gated by two decisions up front: what is under test (and who
uses it), and what you are allowed to do with the findings (report, file
tickets, or draft PR). Do not skip the gates. A UAT with the wrong scope or the
wrong personas produces confident, useless findings. Agree the scope and the
plan, then run.

Work through the phases in order. Phases 0 to 2 are interactive and happen in the
main conversation. Phases 3 to 7 fan out to subagents. Phase 8 decides whether to
loop.

**Sandbox reality (read once, applies throughout).** This runs under Claude Code
on the web. It cannot `git push`, cannot reach the IAP-guarded staging/prod URLs
cookie-less (they 302/403), and touches Asana over MCP, not the resolver's Python
scripts. Testers are **read-only against a live BigQuery warehouse of real client
data** — read-only MCP only, never a real sync/backfill, never production.
Delivery stops at staging (a handed-off draft PR to `main`) or a filed ticket.
Production is Frazer's separate manual promote and is never in scope.

## The loop at a glance
```
0  Scope + users   ── ASK: which ADA surface, and who really uses it?
1  Confirm facts   ── read references/ada-project-facts.md; re-confirm the load-bearing ones
2  Plan + mode     ── PRESENT personas + plan, then ASK output mode (= go-ahead)
   ───────────────────────────────────────────────────────────────────────
3  Run             ── fan out tester agents; each narrates its attempt + logs friction
4  Assess          ── one fresh agent dedupes & categorises every narrative into flags
5  Propose         ── [fixes mode] ada-builder proposes a fix per flag
6  Vet             ── ada-reviewer (viable + park backstop) + ada-uat-reviewer (solves it)
7  Ship / Report   ── [fixes] build + gates + smoke + confirm + draft PR  |  [report] compile
   ───────────────────────────────────────────────────────────────────────
8  Loop            ── re-run against the new state until dry or context-tight, then stop
```

## Phase 0: Scope and users (ASK)

Establish two things in one short exchange. Keep it tight, and if the user
already said all this in their request, just confirm your understanding in a line
and move on.

Scope: the bounds of the review. Do not assume "the whole app." Pin it to one ADA
surface, for example:

- a project's **Diagnostics tab** (triage board ACT NOW / KEEP AN EYE ON /
  HEALTHY / NOT REPORTING; signals D/A/R/C/F; the health-score gauge that is
  deliberately *different from pacing*);
- the **Pacing tab** (oscilloscope, envelope history, the `y=100` = ON PLAN
  reference line, self-serve vs direct-buy split, the `is_direct` toggle);
- the **Creative "Call Sheet" tab** (rotation cards, creative×platform matrix,
  SCALE/HOLD/REFRESH/EARLY verdicts, report totals strip, GA4 funnel);
- the **Audiences "Electorate" tab** (dossiers, resonance matrix, saturation);
- the **Summary verdict** hero end to end;
- **Flightdeck** (portfolio pulse, attention list, flight rows);
- the **Alerts feed** + acknowledge;
- the **Full History Backfill** admin flow (409 guard, honest running → success);
- a **client report export** view;

or the whole product if that is genuinely what they want. While you are here, get
what "success" means for the user attempting it (the end state they are trying to
reach — e.g. "tell a client the campaign is on track without pinging a
colleague") and anything out of scope or off-limits.

Users: who actually touches this. ADA has exactly ONE user today — **Frazer**,
the founder and sole builder — with the explicit goal of team-wide adoption on
rollout, plus clients who only ever view outputs. So anchor the range to that
reality: Frazer as the L4 power user who hunts subtle cross-surface wrongness; an
**onboarding teammate** as the L1 who hits the jargon cliff; and, for
client-facing surfaces, a **read-only client** who never logs in but reads
the numbers. The expertise axis here is **familiarity with political ad-ops and
the platform's own vocabulary** (pacing, R&F / dedup reach, diagnostics signals,
self-serve vs direct-buy, flight) — NOT coding skill.

## Phase 1: Confirm the project facts

The project facts are already established for ADA in
`references/ada-project-facts.md` — **read it rather than rediscovering them.**
It carries, worked out: how a tester reaches the product (the IAP wall, and the
Recipe 1/2/3 ladder — human-on-live-staging, local `next dev` + BigQuery-MCP
fixtures, or static evaluation over real BigQuery), the test tooling and the
authoritative gates, what the product is (and therefore what a good fix looks
like — honest "not reporting" copy, plain language over codes, observe-don't-
command voice, the R&F source-of-truth rule), the ship path and its guardrails,
and the Asana board GIDs.

Because facts drift, **re-confirm the load-bearing ones** before you fan out:
skim the current `CLAUDE.md` (status, owed verifications, gotchas), and do a
quick reach check for the surface under test (which router + `lib/*` files back
it, and whether a Recipe 2 stub or Recipe 3 read is the right fidelity). If
anything in the reference has gone stale, trust the repo and note the drift.

The one fact worth restating up front: for most ADA testers the **primary method
is reasoning over rendered React components + backend router JSON** (Recipe 3
plus reading `frontend/src/components`, `lib/flight.ts`, `lib/creative.ts`,
`lib/glossary.ts`, `components/glossary.tsx`, and the diagnostics engine copy) —
because you cannot drive the real IAP-guarded UI from here. Stand up the Recipe 2
localhost fixture UI when the finding is about interaction/visual flow.

## Phase 2: Personas, plan, and output mode

Draft a spread of personas, present the plan, and get an explicit go-ahead in the
form of the output-mode choice.

**Personas.** Default to 3, spaced along the ad-ops / ADA-vocabulary axis (up to
~5 for a broad surface, down to 2 for a narrow one). Anchor the low end at the
least ad-ops-literate person who will realistically touch it — **always include
the onboarding teammate as L1** (team-wide adoption is the goal; that novice end
is where the jargon and glossary-discoverability findings live and it is the end
most easily skipped by accident). **Always include Frazer as L4** — he is the
only current user and the correctness backstop. For client-facing surfaces
(exported reports/dashboards), add the optional read-only client persona. Give
each persona exactly one level rather than a blended range. A persona hedged as
"L1 to L2" drifts upward in practice and stops reproducing a real novice's
confusion, which is the whole reason it is there.

| Level | Who they are (ADA axis = ad-ops + platform vocabulary) | What they surface |
|------|--------------------------------------------------------|-------------------|
| **L1 Novice** | New agency hire being onboarded; pacing / R&F / diagnostics signals / self-serve-vs-direct-buy all stop them cold | Onboarding cliffs, unexplained terms, glossary discoverability, "NOT REPORTING"-vs-zero confusion, jargon leakage |
| **L2 Casual** | Account manager / campaign coordinator; knows the ad platforms and the campaigns, treats ADA as read-and-report | Unclear next steps, whether the verdict tells them what to tell the client, silent data staleness, over-100% glosses |
| **L3 Competent** | Campaign strategist / senior operator; fluent in pacing, benchmarks, R&F; runs several campaigns and reallocates budget mid-flight | Metric-differs-between-tabs, scope-label ambiguity, evidence granularity, Flightdeck→project workflow gaps |
| **L4 Expert** | **Frazer** — platform owner; knows every campaign and the data model cold; impatient with wasted clicks and subtle wrongness | Cross-tab correctness bugs, honesty-of-numbers, StackAdapt reach plausibility, admin-flow friction (backfill 409, media-plan sync) |
| **Client** *(optional, read-only)* | Candidate / comms staff who never log in but see exported report views | Jargon leaking into client copy, un-glossed over-100% figures, any misleading or embarrassing number |

Give each persona a concrete identity so the tester agent can stay in character:
name and one-line bio (role, agency context); exactly one expertise level from
the scale (commit; do not hedge with a range) and the ad-ops terms they do and
don't know; their goal (tied to Phase 0); temperament (patient vs impatient,
reads captions or ignores them, trusts the verdict word or cross-checks against
the native ad managers); and how they personally would judge "it worked."

**Plan.** Present the personas plus the scenarios each will attempt (map personas
to workflows; it is often better for several personas to attempt the *same* core
workflow — e.g. "open this campaign and tell me if it is healthy and on budget"
— so you can compare where each expertise level trips). State how they will test
(Recipe 3 reasoning over components + router JSON, the Recipe 2 localhost fixture
UI, or — for a data-correctness question — Recipe 3 against the read-only
BigQuery MCP), what gets captured (a first-person narrative plus a structured
issue log), and roughly how many passes and agents so the user knows the cost.
Invite them to amend personas or scope before you proceed.

**Output mode.** Use AskUserQuestion for this. It is a discrete choice that
changes everything downstream, and it doubles as the go-ahead:

1. **Report only** (the strong default; recommend this unless the user explicitly
   asked for fixes). No code changes; you produce a categorised UAT report of
   findings and recommendations.
2. **File Asana tickets** (the ADA-native fix path). Turn each verified flag into
   a ticket on the ADA board (project GID `1215988273595218`) via the Asana MCP
   (`create_tasks` / `update_tasks`): map severity → Priority (field
   `1215988107013686`), and set Ready For (field `1216308984626884`) → 🤖 Agent
   `1216308984626886` for auto-eligible flags or → 👨🏻‍💻 Frazer `1216308984626885`
   for park-class ones — so `ada-resolve-ticket` drains them one per run under
   full propose/review/park discipline. Nothing is built in this session.
3. **Fixes then draft PR.** Run the ADA fix pipeline (Phases 5–7) and hand back
   ONE **draft PR to `main` (staging only)**, Ready For → Frazer, **auto-eligible
   flags only**. The sandbox cannot push, so this is a handed-off branch, never a
   live deploy. Production is never in scope.

Record the answer. It decides whether Phases 5 to 7 run the fix pipeline, file
tickets, or stop at the report.

## Phase 3: Run the UAT passes (fan out tester agents)

Spawn one subagent per persona via the Agent tool — **default to `Explore`**
(read-only; it cannot edit the product's files) for Recipe-3 reasoning runs, and
reach for `general-purpose` ONLY when a Recipe 2 run genuinely needs Bash to stand
up `next dev` + Playwright. Run them in parallel (one message, one tool block per
persona). Each tester shares no memory with you or the others, so its prompt must
be self-contained: the full persona, the scope, how to reach the product (the
Recipe from Phase 1, with the exact files/paths or the fixture-stub setup), the
scenario(s) to attempt, and the rules of engagement below — including the
forbidden actions **named verbatim**, because for a `general-purpose` tester the
read-only guarantee is prompt-level, not structural.

### Staying in character is the whole ballgame

The value of this skill lives or dies on whether a persona reproduces a real
user's ignorance. An agent that quietly uses its own expertise to get unstuck
erases the exact finding you ran this to get. So instruct every tester:

- **Act only on what is visible in the UI.** If a label uses a term this persona
  would not know, the persona does not know it either, and that is a finding, not
  something to look past. Do not lean on outside knowledge of how ADA works
  internally. (An L1 who "just knows" that a health score of 75 is a success, or
  that D3 means reach depth, is not an L1.)
- **Do not read the source to figure out what to do.** An L1–L3 user cannot. Only
  L4 Frazer would inspect a component or a router, and only as far as a real
  impatient owner actually would to confirm a suspected bug.
- **React like a person, not a test harness.** Try the obvious thing, get
  confused, look for a cue (a caption, a glossary underline, a tooltip), and if
  none exists, guess or give up, and log exactly where and why. Calibrate the
  confusion to the level: an L1 stalls on "pacing 100% = ON PLAN," on a signal
  code shown without its plain name, on "NOT REPORTING" reading as a bug; L4
  breezes past basics but bristles at two tabs disagreeing on the same metric or
  a StackAdapt reach that looks physically implausible.
- **Be honest, not theatrical.** Report what the product actually does, backed by
  evidence (a UI quote, a `file:line`, a screenshot path, a real BigQuery row via
  the read-only MCP), not a dramatised failure. A tester who invents a bug is
  worse than useless.
- **Stay read-only, against a live warehouse — name the forbidden tools in the
  prompt.** Testers observe and report; they never edit the product. Use ONLY
  `mcp__Google_Cloud_BigQuery__execute_sql_readonly` — never the writable
  `execute_sql`, never any INSERT/UPDATE/DELETE/TRUNCATE. Never POST to (or
  otherwise trigger) a `/run`, `/sync`, `/admin`, backfill, or media-plan-sync
  endpoint on any environment; never call a write MCP tool (Asana / GitHub /
  Drive / Slack write); never aim Playwright at a live IAP URL. Read-only MCP and
  localhost fixtures only — and this holds by rule regardless of whether
  credentials happen to be present.

**What "friction" looks like here** (so a tester knows what to flag — it is NOT
paste/embed friction): does a novice grok "pacing 100% = ON PLAN"? Does a signal
code (D3/A4) read as jargon because the plain name isn't leading? Is "self-serve
vs direct-buy budget split" self-explanatory on the verdict hero? Does the
StackAdapt individual-vs-household reach split confuse, or does a summed reach
look too high? Does "NOT REPORTING" read as honest-and-intentional or as a bug?
Are over-100% conversion / sessions-per-click figures glossed as normal? Is the
glossary hover discovered at all? Do two tabs showing the same metric at
different scopes read as intentional (the #121 diagnostics-vs-summary scope
concern), or as inconsistent data? Treat **">100%"** and **"numbers differ
between tabs"** as *explain-don't-assume-broken* checks: the tester's job is to
confirm the UI explains the surprise, because these are correct-by-design.

Each tester logs every issue with: the step, observed behaviour, expected
behaviour, severity (blocker / major / minor / polish), and evidence. It returns
both the narrative and the structured issue log.

### How testers actually exercise the thing
Pick the highest-fidelity method the environment supports (established in
Phase 1):

1. **Recipe 2 — localhost fixture UI.** Seed real rows from the read-only
   BigQuery MCP into a small stub, run `next dev`, and drive
   `http://localhost:3000` with the preinstalled Chromium via the Playwright
   library. Best for interaction/usability/visual-flow findings; catches what
   static reading never will. (Never aim it at the live IAP URLs.)
2. **Recipe 3 — reason over rendered components + router JSON**, and reproduce
   the SQL against real `cip` / `cip_stackadapt` data via the read-only MCP when
   the question is about numbers. This is the primary method here. Cite concrete
   `file:line` and real rows; never fabricate behaviour; if unsure, mark the
   issue "needs verification" (or "needs Recipe 1" for the owed live-staging
   checks like the backfill 409 and StackAdapt 26022 CATIE reach).

## Phase 4: Assess and categorise (one fresh assessor)

Hand all the narratives and issue logs to a single fresh assessor agent
(`general-purpose`, fresh so it is not biased by having "lived" any one run). It
must:

- **Deduplicate**: the same friction from three personas is one flag. Note that
  it hit multiple expertise levels, which raises its priority (a term that stops
  both L1 and L2 is a bigger jargon problem than one that only stops L1).
- **Categorise** the flags into ADA improvement areas: jargon / raw-token
  leakage & inconsistent labels; honest empty / not-reporting / thin-data states;
  diagnostics readability (score-vs-pacing, 70-not-100 bands); cross-tab metric
  consistency (scope-labelled); metrics that legitimately exceed 100%;
  reach/frequency correctness (StackAdapt especially, non-additive); loading /
  verdict / async-action honesty; advisory voice & suggested-move ownership;
  plus correctness bugs, workflow gaps, and accessibility.
- **Rank** by severity × frequency × how many expertise levels it blocked. A
  wrong client-facing number outranks everything.
- **Flag which flags are correct-by-design** ("explain-don't-assume-broken"): a
  tester's ">100%" or "tabs disagree" reaction is a finding only if the UI
  *fails to explain it*; if the caption/gloss is present and clear, that's a
  "what went well," not a bug.
- **Return a structured flag list**: each flag has id, title, category, severity,
  which personas hit it, evidence, and a one-line "why it matters." For fixes/
  tickets modes, also tag each flag **auto-eligible vs park-class** by which
  file zones a fix would touch (see the autonomy boundary in the reference).

If output mode is **report only**, skip to Phase 7 (compile the report). If
**file Asana tickets**, skip to Phase 7's ticket path.

## Phase 5: Propose fixes (fixes mode only)

Do NOT reinvent the fix pipeline — ADA already ships it. First stand up **one
shared round branch + worktree** for the whole batch, so the auto-eligible flags
collapse into a single draft PR instead of N of them:
`.claude/skills/ada-resolve-ticket/scripts/worktree.sh add uat/<surface>-<date>
<worktree_path>` (it branches off `origin/main` and wires up frontend deps; set
`ADA_REPO` if the repo root isn't the default). Every `ada-builder` invocation
this round is handed that SAME worktree path and a per-flag run dir.

For each high-value, auto-eligible flag, delegate to the **`ada-builder`** agent
in `MODE: propose`. Give it the flag as a ticket-shaped brief (the friction, the
affected persona, the evidence), the shared worktree path, its own run dir, and
the reserved area — the files that flag's fix may touch, kept **disjoint** from
the other flags' so the sequential builds in Phase 7 don't collide. Propose is
read-only investigation, so these can fan out in parallel; each writes its own
`proposal.md` (problem, root cause, exact per-file change, the test it will add,
blast radius, and an explicit auto-promote-vs-park read) and returns exactly one
line: `PROPOSE OK: <summary>` or `PARK: <reason>`. A `PARK` means the honest fix
needs BigQuery / ingestion / transform / a new dependency — route it to the
ticket/park path, do not force a frontend-only hack. Keep proposals surgical and
in the ADA voice (honest "not reporting" over raw tokens, observe-don't-command,
plain language over codes).

## Phase 6: Vet the proposals

Vet with a **separate skeptical reviewer so the author does not grade its own
homework** — and reuse ADA's two-reviewer split rather than a generic reviewer.
Run these in parallel per proposal:

1. **`ada-reviewer`** (viability + safety + park backstop). It checks the change
   works, builds, fits conventions, has meaningful tests, and does not sprawl —
   and runs the park-path backstop
   `git -C <worktree> diff --name-only origin/main | python3
   .claude/skills/ada-resolve-ticket/scripts/area.py --stdin` (exit 3 =
   must-park). It returns
   `APPROVE` / `REVISE: <points>` / `ESCALATE: <reason, incl. must-park paths>`.
2. **`ada-uat-reviewer`** (does it solve the flag). It judges from the affected
   persona's chair whether the change actually removes the friction the tester
   hit and honours the ADA voice, returning `ACCEPT` / `REJECT: <the gap>`.

The **safety axis for ADA** (what `ada-reviewer` and you are checking — NOT
XSS-in-embed, which does not exist here):

- **IAP stays enforced.** A change must not open a cookie-less path; a protected
  route returning 200 to an unauthenticated caller is a hard fail. `deploy.yml`
  re-asserts `--invoker-iam-check` — keep the backend IAM-private.
- **Data residency / cross-region DML.** No DML against the Montreal `cip`
  dataset from a US-region source; require the Python `SELECT` +
  `load_table_from_json()` pattern. Flag any proposal adding cross-region DML.
- **Data honesty / source of truth.** Never reintroduce Funnel StackAdapt
  reach/frequency into an aggregate; never invent or smooth over a value on a
  client-facing surface; honour honest "not reporting" copy and the `is_direct` /
  media-plan-sync destructiveness guardrails.
- **No secrets, no new heavy dependency** (lean `requirements.txt` / frontend
  deps).

Send back anything that fails (`REVISE`/`REJECT`) to `ada-builder` for a revision
round (cap 2), then park. `ESCALATE` or a must-park path → park it for Frazer.
Prefer fewer, verified fixes over many shaky ones.

## Phase 7: Ship and verify, file tickets, or compile the report

Route by the Phase 2 output mode.

**Fixes then draft PR:** run the vetted, accepted, auto-eligible flags through
**`ada-builder`** in `MODE: build` **sequentially into the shared round branch**
(they share one worktree, so they cannot build in parallel). Each build implements
its flag, adds/updates tests, runs the gates
(`.claude/skills/ada-resolve-ticket/scripts/gates.sh <worktree>` — tsc
`--noEmit` + both pytest tiers with the stale-baseline allowance), and commits in
the worktree **only when its gates are GREEN** (it does NOT push), returning
`BUILD gates GREEN` / `BUILD gates RED`. A `BUILD gates RED` or a must-park flag
never gets committed — it drops out of the batch and is handed to Frazer
separately, while the green flags stay on the branch. The result is **one branch
carrying every landed fix → one draft PR to `main`**. Then:

- **Confirmation gate (hard).** Before anything leaves this session — before you
  open the branch/draft PR or hand off commands — stop and show the user exactly
  what will ship: the diff summary, the flags each change closes, the gate
  results, and that the target is a **draft PR to `main` = staging only** (never
  production, which is Frazer's separate manual promote). Get an explicit
  confirmation. This is the one outcome that would break trust if it happened by
  surprise, and the whole loop was run on the understanding that the user gets
  the last word before anything lands.
- **Smoke as a reality check.** The sandbox can't push, so real post-deploy smoke
  is a Frazer-side step after he merges. Where you *can* — e.g. after a
  Recipe-2-style local run, or to reality-check the change against the affected
  scenario — do a quick check; and note that once the branch is merged, the
  post-staging cookie-less IAP curl is exactly the **`ada-smoke`** agent's job
  (expect 401/403 on protected routes; a 200 is the red flag). Hand off the
  branch + the smoke expectation to Frazer.

**File Asana tickets:** for each verified flag, create a ticket on the ADA board
via the Asana MCP with the friction, the persona, the evidence, and a suggested
fix; set Priority from severity and Ready For → Agent (auto-eligible) or → Frazer
(park-class). Confirm the ticket list with the user before creating (the same
hard gate — creating tickets is a visible, shared action). **Flag the downstream
consequence in that confirmation:** a ticket tagged Ready For → 🤖 Agent becomes
eligible for `ada-resolve-ticket` to auto-promote to **staging** on a later run
with no fresh prompt, so tag a borderline flag → 👨🏻‍💻 Frazer (park) when a human
should eyeball it before any staging deploy. Report the created GIDs.

**Report mode:** compile the report (template below) and hand it back. Offer, but
do not perform, the ticket-filing or fix pipeline as a next step.

## Phase 8: Loop and context management

This is a loop. After a round, re-run the affected scenarios against the new state
to confirm improvement and surface the next layer of friction (fixing the top
blocker usually reveals the one behind it). Keep going while it is productive.

Stop and flag when any of these hit; do not silently truncate:

- Two consecutive rounds surface nothing new (dry).
- The remaining flags are all low-severity polish.
- **Your context window is getting tight.** This is a hard stop: summarise what
  is done, what is outstanding, and where you would resume, then hand back.

Because subagents do not share your memory, checkpoint between rounds: keep a
running list of flags (open / fixed / filed / deferred, with the auto-vs-park
tag) so a fresh round, or a fresh session, can pick up cleanly.

## Report template (report mode, or the summary in fixes/tickets mode)
```markdown
# ADA UAT Report: <surface>  ·  <date>

## Scope & method
<which ADA surface, which Recipe testers reached it by, how many rounds>

## Personas
<name · level (L1 onboarding hire … L4 Frazer / client) · goal, one line each>

## Findings (ranked)
### [BLOCKER] <title>  (hit by <personas>, <category>)
<what happens, expected, evidence: UI quote / file:line / real BQ row>.
Fix: <recommendation or what was done>.  [auto-eligible | park-class]
### [MAJOR] ...
### [MINOR] ...
### [POLISH] ...

## Correct-by-design (explained, not bugs)
<the >100% / cross-tab-scope / NOT-REPORTING cases where the UI already glosses
the surprise — worth protecting in future changes>

## What went well
<the flows that were smooth and honest, worth protecting>

## Recommendations / changes shipped
<prioritised; link each filed Asana ticket GID and/or the staging draft PR;
mark park-class flags handed to Frazer>

## Not tested / open questions
<gaps; anything marked "needs verification"; the Recipe-1-only checks
(#123 backfill end-to-end, #114 StackAdapt 26022 CATIE) a live-staging human must
close>
```

## Orchestration notes

The Agent tool is the engine, and for the fix pipeline the agents are ADA's own,
by `agentType`:

- **Phase 3 testers:** `Explore` by default (read-only) — one per persona,
  launched in a single message so they run concurrently; escalate to
  `general-purpose` only for a Recipe 2 run that needs Bash + Playwright, and name
  the forbidden actions verbatim in its prompt.
- **Phase 4 assessor:** one fresh `general-purpose` agent.
- **Phase 5 propose:** `ada-builder` (`MODE: propose`), one per high-value flag.
- **Phase 6 vet:** `ada-reviewer` (viability + `area.py` park backstop) and
  `ada-uat-reviewer` (user acceptance), in parallel — this is the skeptical
  second reader; do not have the builder grade itself.
- **Phase 7 build + smoke:** `ada-builder` (`MODE: build`, runs `gates.sh`), then
  `ada-smoke` for the post-staging cookie-less IAP check (Frazer-side after
  merge; a 200 on a protected route is the red flag).

Keep the interactive gates (Phases 0 to 2 and the Phase 7 confirmation) in the
main conversation; only the heavy fan-out belongs in subagents. Pass paths, not
contents.

## Pitfalls (the things that bite)

- **Personas leaking expertise.** An L1 that "just knows" a 75 health score is
  good, or that D3 means reach depth, is not an L1. Holding character is where
  the real ADA findings come from — the jargon cliff and the glossary-
  discoverability gaps only show up if the novice stays novice.
- **Fabricated bugs.** Every flag needs evidence — a UI quote, a `file:line`, or
  a real BigQuery row via the read-only MCP. Unverified observations are marked
  as such (or "needs Recipe 1" for the live-staging-only checks), not shipped as
  fact.
- **Treating correct-by-design as broken.** ">100%", tabs disagreeing at
  different scopes, and "NOT REPORTING" are correct-by-design; the finding is
  only whether the UI *explains* the surprise. Don't file "the number is wrong"
  when the real issue is a missing gloss.
- **Author-graded fixes.** Vet with `ada-reviewer` + `ada-uat-reviewer`, not the
  builder that wrote the proposal.
- **Reinventing the pipeline.** Don't hand-roll a generic fix swarm that doesn't
  know ADA's park rules, gates, or voice guards — a generic swarm could sail a
  BigQuery/schema change straight past the park boundary. Call `ada-builder` /
  `ada-reviewer` / `ada-uat-reviewer` / `ada-smoke` and the `area.py` check.
- **Deploying blind or by surprise.** The sandbox can't push and never touches
  production. Fixes stop at a handed-off draft PR to `main` (staging) or a filed
  ticket, always behind the Phase 7 confirmation gate. Never aim Playwright at a
  live IAP URL, never run a writable BigQuery query or a real sync/backfill.
- **Running out of road quietly.** When context gets tight, say so and hand off
  cleanly with the flag ledger. A half-finished loop that pretends to be complete
  is the worst outcome.
