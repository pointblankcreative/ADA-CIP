---
name: ada-simulate-uat
description: >-
  Run a loop of simulated User Acceptance Testing (UAT) tuned for the ADA
  Campaign Intelligence Platform — the internal political-ad reporting stack
  (Flightdeck triage home, the per-project tabs — Summary, Pacing, Creative,
  Audiences, Diagnostics — the Diagnostics triage board, Alerts, and the Admin
  backfill flow). Recruit AI tester personas across an ad-ops expertise spread —
  from a new agency hire up through Frazer the builder — have them exercise a
  defined ADA surface the way real users would, categorise the friction, and
  produce a designed, chart-and-diagram UAT report pitched at an ads/systems
  literate but non-coder stakeholder, plus a machine-readable flag ledger it
  hands to the ada-propose-fixes skill. Use this whenever the user wants to run
  UAT, simulate user testing, find the friction in this flow, test this like
  real users would, run persona-driven test loops, stress an ADA surface for
  usability or correctness, or says things like "run UAT", "test this like real
  users would", "find the friction in this flow", or "simulate user testing".
---

# ADA Simulate UAT

You run a User Acceptance Testing loop against the ADA Campaign Intelligence
Platform: AI tester personas exercise an ADA surface like real users, you gather
their narratives, categorise the friction, and then **produce a report a
non-coder can act on** — a designed page with diagrams and charts, not a wall of
`file:line`. This is stage one of a three-skill pipeline:

```
ada-simulate-uat   → discover friction, produce the report + a flag ledger
ada-propose-fixes  → propose fixes, red-team them, cut debt, re-validate w/ personas
ada-deploy-fixes   → build, deploy to staging, verify it worked in STG
```

This skill **never changes code or data.** Its job is to find and explain the
problems clearly, and to write a clean handoff for the next stage.

The whole thing is gated by two decisions up front: what is under test (and who
uses it), and what you do with the findings (report, file tickets, or hand off
to `ada-propose-fixes`). Do not skip the gates. A UAT with the wrong scope or the
wrong personas produces confident, useless findings. Agree the scope and the
plan, then run.

**Environment reality (read once, applies throughout).** These skills run under
Claude Code on the web / remote execution. Here, `git push` and the GitHub MCP
DO work (this environment pushes branches and opens PRs) — the older `CLAUDE.md`
"sandbox cannot push" note describes the *local* sandbox; trust what your
environment actually does. What you still cannot do from here: reach the
IAP-guarded staging/prod URLs cookie-less — they `302`/`403` (verified), so the
live UI is a human-only "Recipe 1" path. Testers are **strictly read-only against
a live BigQuery warehouse of real client data** — read-only MCP only, never a
sync/backfill, never production.

## The loop at a glance
```
0  Scope + users   ── ASK: which ADA surface, and who really uses it?
1  Confirm facts   ── read references/ada-project-facts.md; re-confirm the load-bearing ones
2  Plan + mode     ── PRESENT personas + plan, then ASK output mode (= go-ahead)
   ───────────────────────────────────────────────────────────────────────
3  Run             ── fan out tester agents; each narrates its attempt + logs friction
4  Assess          ── one fresh agent dedupes & categorises every narrative → flags.json
5  Report          ── build the designed, chart+diagram report for a non-coder reader
6  Deliver         ── report only  |  file Asana tickets  |  hand the run dir to ada-propose-fixes
   ───────────────────────────────────────────────────────────────────────
7  Loop            ── re-run against the new state until dry or context-tight, then stop
```

## The run directory (the pipeline's shared contract)

All three skills read and write one **run directory** — pass its path from skill
to skill. Default it to your scratchpad (e.g.
`<scratchpad>/ada-uat/<surface>-<date>/`) or a path the user gives; it is working
state, **not committed to the repo.** This skill writes into it:

- `flags.json` — the machine-readable flag ledger (schema at the end). This is
  the contract `ada-propose-fixes` consumes.
- `report.html` — the designed human report (also published as an Artifact).
- `narratives/` — optional: each tester's raw first-person run.

## Phase 0: Scope and users (ASK)

Establish two things in one short exchange. Keep it tight; if the user already
said all this, confirm your understanding in a line and move on.

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
client-facing surfaces, a **read-only client** who never logs in but reads the
numbers. The expertise axis here is **familiarity with political ad-ops and the
platform's own vocabulary** (pacing, R&F / dedup reach, diagnostics signals,
self-serve vs direct-buy, flight) — NOT coding skill.

## Phase 1: Confirm the project facts

The project facts are already established for ADA in
`references/ada-project-facts.md` — **read it rather than rediscovering them.**
It carries, worked out: how a tester reaches the product (the IAP wall, and the
Recipe 1/2/3 ladder — human-on-live-staging, local `next dev` + BigQuery-MCP
fixtures, or static evaluation over real BigQuery), the test tooling, what the
product is (and therefore what a good fix looks like — honest "not reporting"
copy, plain language over codes, observe-don't-command voice, the R&F
source-of-truth rule), and the Asana board GIDs.

Because facts drift, **re-confirm the load-bearing ones** before you fan out:
skim the current `CLAUDE.md` (status, owed verifications, gotchas), and do a
quick reach check for the surface under test (which router + `lib/*` files back
it, and whether a Recipe 2 stub or Recipe 3 read is the right fidelity). If
anything in the reference has gone stale, trust the repo and note the drift.

The one fact worth restating: for most ADA testers the **primary method is
reasoning over rendered React components + backend router JSON** (Recipe 3 plus
reading `frontend/src/components`, `lib/flight.ts`, `lib/creative.ts`,
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
only current user and the correctness backstop. For client-facing surfaces, add
the optional read-only client persona. Give each persona exactly one level, not a
range — a persona hedged as "L1 to L2" drifts upward and stops reproducing a real
novice's confusion, which is the whole reason it is there.

| Level | Who they are (ADA axis = ad-ops + platform vocabulary) | What they surface |
|------|--------------------------------------------------------|-------------------|
| **L1 Novice** | New agency hire being onboarded; pacing / R&F / diagnostics signals / self-serve-vs-direct-buy all stop them cold | Onboarding cliffs, unexplained terms, glossary discoverability, "NOT REPORTING"-vs-zero confusion, jargon leakage |
| **L2 Casual** | Account manager / campaign coordinator; knows the ad platforms and the campaigns, treats ADA as read-and-report | Unclear next steps, whether the verdict tells them what to tell the client, silent data staleness, over-100% glosses |
| **L3 Competent** | Campaign strategist / senior operator; fluent in pacing, benchmarks, R&F; runs several campaigns and reallocates budget mid-flight | Metric-differs-between-tabs, scope-label ambiguity, evidence granularity, Flightdeck→project workflow gaps |
| **L4 Expert** | **Frazer** — platform owner; knows every campaign and the data model cold; impatient with wasted clicks and subtle wrongness | Cross-tab correctness bugs, honesty-of-numbers, StackAdapt reach plausibility, admin-flow friction (backfill 409, media-plan sync) |
| **Client** *(optional, read-only)* | Candidate / comms staff who never log in but see exported report views | Jargon leaking into client copy, un-glossed over-100% figures, any misleading or embarrassing number |

Give each persona a concrete identity so the tester agent can stay in character:
name and one-line bio (role, agency context); exactly one expertise level and the
ad-ops terms they do and don't know; their goal (tied to Phase 0); temperament
(patient vs impatient, reads captions or ignores them, trusts the verdict word or
cross-checks against the native ad managers); and how they personally judge "it
worked." **Record the personas in `flags.json`** — the later skills re-validate
proposed fixes against exactly these people.

**Plan.** Present the personas plus the scenarios each will attempt (map personas
to workflows; it is often better for several personas to attempt the *same* core
workflow — e.g. "open this campaign and tell me if it is healthy and on budget" —
so you can compare where each level trips). State how they will test (Recipe 3
reasoning, the Recipe 2 localhost fixture UI, or Recipe 3 against the read-only
BigQuery MCP for a data-correctness question), what gets captured (a first-person
narrative plus a structured issue log), and roughly how many passes and agents so
the user knows the cost. Invite them to amend personas or scope before you run.

**Output mode.** Use AskUserQuestion. It is a discrete choice and doubles as the
go-ahead:

1. **Report only** (the strong default). Produce the designed report + the flag
   ledger. No tickets, no code, no handoff.
2. **Report + file Asana tickets.** Also turn each verified flag into a ticket on
   the ADA board (project GID `1215988273595218`) via the Asana MCP
   (`create_tasks` / `update_tasks`): map severity → Priority (field
   `1215988107013686`), set Ready For (field `1216308984626884`) → 🤖 Agent
   `1216308984626886` (auto-eligible) or → 👨🏻‍💻 Frazer `1216308984626885`
   (park-class). Note in the confirmation that an Agent-tagged ticket can later be
   auto-promoted to staging by `ada-resolve-ticket` with no fresh prompt.
3. **Report + hand off to `ada-propose-fixes`.** Produce the report and the
   ledger, then tell the user the run-dir path and that they can run
   `ada-propose-fixes` on it next (or offer to invoke it). This skill still stops
   at the report — it does not propose or build.

## Phase 3: Run the UAT passes (fan out tester agents)

Spawn one subagent per persona via the Agent tool — **default to `Explore`**
(read-only; it cannot edit the product's files) for Recipe-3 reasoning runs, and
reach for `general-purpose` ONLY when a Recipe 2 run genuinely needs Bash to stand
up `next dev` + Playwright. Run them in parallel (one message, one tool block per
persona). Each tester shares no memory with you or the others, so its prompt must
be self-contained: the full persona, the scope, how to reach the product (the
Recipe from Phase 1, with exact files/paths or the fixture-stub setup), the
scenario(s), and the rules of engagement below — including the forbidden actions
**named verbatim**, because for a `general-purpose` tester the read-only guarantee
is prompt-level, not structural.

### Staying in character is the whole ballgame

The value of this skill lives or dies on whether a persona reproduces a real
user's ignorance. An agent that quietly uses its own expertise to get unstuck
erases the exact finding you ran this to get. So instruct every tester:

- **Act only on what is visible in the UI.** If a label uses a term this persona
  would not know, the persona does not know it either, and that is a finding, not
  something to look past. (An L1 who "just knows" a health score of 75 is a
  success, or that D3 means reach depth, is not an L1.)
- **Do not read the source to figure out what to do.** An L1–L3 user cannot. Only
  L4 Frazer would inspect a component or a router, and only as far as a real
  impatient owner actually would to confirm a suspected bug.
- **React like a person, not a test harness.** Try the obvious thing, get
  confused, look for a cue (a caption, a glossary underline, a tooltip), and if
  none exists, guess or give up, and log exactly where and why. Calibrate the
  confusion to the level: an L1 stalls on "pacing 100% = ON PLAN," on a signal
  code shown without its plain name, on "NOT REPORTING" reading as a bug; L4
  breezes past basics but bristles at two tabs disagreeing on the same metric or a
  StackAdapt reach that looks physically implausible.
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
concern) or as inconsistent data? Treat **">100%"** and **"numbers differ between
tabs"** as *explain-don't-assume-broken* checks: the finding is whether the UI
explains the surprise, because these are correct-by-design.

Each tester logs every issue with: the step, observed behaviour, expected
behaviour, severity (blocker / major / minor / polish), and evidence. It returns
both the narrative and the structured issue log.

### How testers actually exercise the thing
Pick the highest-fidelity method the environment supports (Phase 1):

1. **Recipe 2 — localhost fixture UI.** Seed real rows from the read-only
   BigQuery MCP into a small stub, run `next dev`, and drive
   `http://localhost:3000` with the preinstalled Chromium via the Playwright
   library. Best for interaction/usability/visual-flow findings. (Never aim it at
   the live IAP URLs.)
2. **Recipe 3 — reason over rendered components + router JSON**, and reproduce the
   SQL against real `cip` / `cip_stackadapt` data via the read-only MCP when the
   question is about numbers. This is the primary method here. Cite concrete
   `file:line` and real rows; never fabricate behaviour; if unsure, mark the issue
   "needs verification" (or "needs Recipe 1" for the owed live-staging checks like
   the backfill 409 and StackAdapt 26022 CATIE reach).

## Phase 4: Assess and categorise (one fresh assessor)

Hand all the narratives and issue logs to a single fresh assessor agent
(`general-purpose`, fresh so it is not biased by having "lived" any one run). It
must:

- **Deduplicate**: the same friction from three personas is one flag. Note it hit
  multiple levels — that raises its priority (a term that stops both L1 and L2 is
  a bigger jargon problem than one that only stops L1).
- **Categorise** into ADA improvement areas: jargon / raw-token leakage &
  inconsistent labels; honest empty / not-reporting / thin-data states;
  diagnostics readability (score-vs-pacing, 70-not-100 bands); cross-tab metric
  consistency (scope-labelled); metrics that legitimately exceed 100%;
  reach/frequency correctness (StackAdapt especially, non-additive); loading /
  verdict / async-action honesty; advisory voice & suggested-move ownership; plus
  correctness bugs, workflow gaps, and accessibility.
- **Rank** by severity × frequency × how many levels it blocked. A wrong
  client-facing number outranks everything.
- **Mark correct-by-design** ("explain-don't-assume-broken"): a ">100%" or "tabs
  disagree" reaction is a finding only if the UI *fails to explain it*; if the
  gloss is present and clear, that is a "what went well," not a bug.
- **Tag each flag's likely fix zone** — `frontend` / `isolated-backend` /
  `data-layer` / `unknown` — by which files a fix would touch (see the autonomy
  boundary in the reference). The later skills use this.
- **Capture the friction chain** where a flag is a broken/weak logic or user
  journey (e.g. the backfill payoff that never arrives, or reach that can't
  dedup): an ordered list of steps with each link tagged fine / weak / blocked.
  This is what powers the report's chain diagram.

Write the result to **`flags.json`** (schema at the end).

## Phase 5: Build the report (the deliverable)

Produce a **designed, self-contained report for a reader who knows ads and
ad-tech and thinks in interconnected systems, but does not read code.** Follow
the exemplar the user gave: plain-language explanation, a flow/chain diagram that
shows *why* a problem bites (real links, but some weak, some blocked), and
comparative bars that size each issue against its siblings. No `file:line` in the
main narrative — the technical evidence lives in `flags.json` and, at most, a
collapsed "for the fix team" appendix.

**Before you build it, load two skills:** `dataviz` (for the charts — palette,
bars, legibility) and `artifact-design` (for the page). Then render the report as
an **Artifact** (default-private HTML page the user can view and share) AND save a
copy to `report.html` in the run dir. Artifacts render **mermaid** natively — use
it for the chain diagrams; use dataviz-styled bars for the comparisons.

The report should contain, in this order:

- **Header + verdict.** The surface, the date, and a one-line honest verdict
  ("The Diagnostics board reads as broken to a new hire — 2 blockers before they
  reach a single real signal").
- **How each blocker/major bites — plain terms + a diagram.** For each top flag,
  a short plain-language explanation and, where it is a chain, a diagram of the
  user's journey or the data/logic path with each link tagged **fine / weak /
  blocked** (mirroring the exemplar's "hire sitters → parents freer → reputation
  ticks up → *blocked: no rooms* → payoff never arrives"). State what the user
  expected, what actually happens, and why it matters to *their* job (telling a
  client, reallocating budget), not to the code.
- **Sizing the issues against each other.** A comparative bar chart ranking the
  flags by impact (severity × how many expertise levels it blocks), so the reader
  sees at a glance which one to care about first — the exemplar's "how strong is
  this lever next to its siblings."
- **Persona coverage.** Who hit what — a small matrix or bars showing which levels
  each flag blocked (an issue that stops L1 *and* L2 is worse than one that only
  trips L1).
- **Correct-by-design (explained, not bugs).** The ">100%" / cross-tab-scope /
  "NOT REPORTING" cases where the UI already glosses the surprise — green, worth
  protecting.
- **What went well.** The flows that were smooth and honest.
- **Not tested / open questions.** Gaps; anything "needs verification"; the
  Recipe-1-only checks a live-staging human must close.

Keep it honest and non-theatrical: every claim traces to real evidence in
`flags.json`. Scale the design effort to the finding count — a two-blocker run
does not need ten charts.

## Phase 6: Deliver

Route by the Phase 2 output mode:

- **Report only:** hand back the Artifact link + `report.html`, and give the
  run-dir path so a fix pass can pick it up later.
- **Report + file Asana tickets:** create the tickets (see Phase 2 mode 2), then
  confirm the list with the user **before** creating (creating tickets is a
  visible, shared action) and report the GIDs.
- **Report + hand off to `ada-propose-fixes`:** state the run-dir path and that
  `flags.json` is ready for the next skill; offer to invoke `ada-propose-fixes`
  on it. Do not propose or build here.

Whatever the mode, this skill's terminal state is: the report exists, `flags.json`
is written, and nothing in the product changed.

## Phase 7: Loop and context management

This is a loop. After a round, re-run the affected scenarios against the new state
(useful once fixes have shipped via the other skills) to confirm improvement and
surface the next layer of friction — fixing the top blocker usually reveals the
one behind it. Keep going while it is productive.

Stop and flag when any of these hit; do not silently truncate:

- Two consecutive rounds surface nothing new (dry).
- The remaining flags are all low-severity polish.
- **Your context window is getting tight.** Hard stop: summarise what is done,
  what is outstanding, and where you would resume, then hand back.

Because subagents do not share your memory, checkpoint between rounds in
`flags.json` (open / reported / filed / fixed) so a fresh round or session can
pick up cleanly.

## `flags.json` schema (the pipeline contract)

```json
{
  "surface": "Diagnostics tab — project 26009",
  "date": "2026-07-16",
  "method": "Recipe 3 + Recipe 2 localhost fixtures",
  "rounds": 1,
  "personas": [
    { "id": "p1", "name": "Sam (new hire)", "level": "L1", "goal": "tell if the campaign is healthy" }
  ],
  "flags": [
    {
      "id": "F1",
      "title": "Signal codes lead; plain names buried",
      "category": "jargon-labels",
      "severity": "major",
      "personas_hit": ["p1", "p2"],
      "levels_blocked": ["L1", "L2"],
      "user_impact": "A new hire can't tell what D3 means, so the board reads as noise, not guidance.",
      "correct_by_design": false,
      "zone": "frontend",
      "chain": [
        { "step": "open Diagnostics board", "link": "fine" },
        { "step": "see 'D3 · 42'", "link": "weak", "note": "code leads, plain name is a tooltip" },
        { "step": "decide what to do", "link": "blocked", "note": "no plain meaning visible" }
      ],
      "evidence": [
        { "type": "file-line", "ref": "frontend/src/components/diagnostics/...tsx:120", "detail": "code rendered before name" },
        { "type": "ui-quote", "detail": "\"D3 · 42\"" }
      ]
    }
  ],
  "correct_by_design": [
    { "title": "sessions-per-click > 100%", "why_ok": "one click can spawn several sessions; UI glosses it" }
  ],
  "what_went_well": ["the health-score legend clearly says it differs from pacing"],
  "not_tested": ["#123 backfill end-to-end — needs a Recipe-1 human on live staging"]
}
```

`zone`, `chain`, and `evidence` are what the downstream skills lean on: `zone`
routes auto-vs-park and frontend-vs-data-layer; `chain` powers both this report's
diagram and the proposal report; `evidence` is the technical handoff kept out of
the human narrative.

## Orchestration notes

- **Phase 3 testers:** `Explore` by default (read-only) — one per persona,
  launched in a single message so they run concurrently; escalate to
  `general-purpose` only for a Recipe 2 run that needs Bash + Playwright, and name
  the forbidden actions verbatim in its prompt.
- **Phase 4 assessor:** one fresh `general-purpose` agent.
- **Phase 5 report:** you build it in the main loop after loading `dataviz` +
  `artifact-design`; a `general-purpose` agent can draft chart data, but keep
  authorship and the Artifact publish in your hands.

Keep the interactive gates (Phases 0–2 and the delivery confirmation) in the main
conversation; only the heavy fan-out belongs in subagents. Pass paths, not
contents.

## Pitfalls (the things that bite)

- **Personas leaking expertise.** An L1 that "just knows" a 75 health score is
  good, or that D3 means reach depth, is not an L1. Holding character is where the
  real ADA findings come from — the jargon cliff only shows up if the novice stays
  novice.
- **Fabricated bugs.** Every flag needs evidence — a UI quote, a `file:line`, or a
  real BigQuery row via the read-only MCP. Unverified observations are marked as
  such (or "needs Recipe 1"), not shipped as fact.
- **A report full of code.** The reader knows ads and systems, not code. If the
  main narrative leans on `file:line` or component names, you built the wrong
  report — move that to `flags.json` and explain the *user* impact instead.
- **Treating correct-by-design as broken.** ">100%", tabs disagreeing at different
  scopes, and "NOT REPORTING" are correct-by-design; the finding is only whether
  the UI *explains* the surprise.
- **Changing anything.** This skill reports; it never edits code or data, never
  pushes a fix, never files a ticket without the confirm gate. Fixes are
  `ada-propose-fixes` and `ada-deploy-fixes`.
- **Running out of road quietly.** When context gets tight, say so and hand off
  cleanly with `flags.json`. A half-finished loop that pretends to be complete is
  the worst outcome.
