---
name: simulate-uat
description: >-
  Run a loop of simulated User Acceptance Testing (UAT): recruit AI tester
  personas across an expertise spread, have them exercise a defined product,
  feature, or workflow the way real users would, categorise the friction, then
  either write a report or (at the user's choice) propose, vet, and apply fixes.
  Use this whenever the user wants to UAT something, simulate real-user testing,
  stress a workflow for usability or UX issues, run persona-driven test loops,
  find where users get stuck, or says things like "run UAT", "test this like
  real users would", "find the friction in this flow", or "simulate user
  testing". Project-agnostic: it discovers how the target runs and deploys
  before touching anything.
---

# Simulated UAT

You are running a User Acceptance Testing loop: a set of AI tester personas
exercise a product like real users, you gather their narratives, categorise the
friction, and then either report it or fix it, looping until the value dries up
or context gets tight.

The whole thing is gated by two decisions up front: what is under test (and who
uses it), and what you are allowed to do with the findings (report, draft PR, or
ship). Do not skip the gates. A UAT with the wrong scope or the wrong personas
produces confident, useless findings. Agree the scope and the plan, then run.

Work through the phases in order. Phases 0 to 2 are interactive and happen in the
main conversation. Phases 3 to 7 fan out to subagents. Phase 8 decides whether to
loop.

## The loop at a glance
```
0  Scope + users   ── ASK: what's under test, and who really uses it?
1  Learn project   ── discover how to run it, reach it, and (if fixing) ship it
2  Plan + mode     ── PRESENT personas + plan, then ASK output mode (= go-ahead)
   ───────────────────────────────────────────────────────────────────────
3  Run             ── fan out tester agents; each narrates its attempt + logs friction
4  Assess          ── one fresh agent dedupes & categorises every narrative into flags
5  Propose         ── [fixes mode] swarm proposes a fix per flag
6  Vet             ── skeptical reviewer checks each: viable? secure? solves the flag?
7  Ship / Report   ── [fixes] apply + verify + confirm + ship  |  [report] compile
   ───────────────────────────────────────────────────────────────────────
8  Loop            ── re-run against the new state until dry or context-tight, then stop
```

## Phase 0: Scope and users (ASK)

Establish two things in one short exchange. Keep it tight, and if the user
already said all this in their request, just confirm your understanding in a line
and move on.

Scope: the bounds of the review. Do not assume "the whole app." Pin it to one of:
a specific function ("the date picker in the booking form"), a specific workflow
("export a report and embed it in a Notion page"), a surface ("the checkout flow
end to end"), or the whole product if that is genuinely what they want. While you are here, also get what "success" means for the user
attempting it (the end state they are trying to reach), and anything out of scope
or off-limits.

Users: who actually touches this, their roles, their technical comfort, the
context they work in. Nudge for the range: the least technical person who will
use it, and the most. You need that range to build personas that catch both "I
have no idea what this word means" bugs and "this is subtly wrong and it is
wasting my time" bugs.

## Phase 1: Learn the project

Before anything runs, discover how this specific project works. This is what
makes the skill portable: you fill in the facts rather than assuming them. For a
fully worked example of these facts filled in for one repo, see
`references/pb-webtools.md` (treat it as a template, not as defaults that apply
elsewhere).

Read the project's `CLAUDE.md`, `README`, any contributing or deploy docs, and
the scripts in `package.json` or its equivalent. From those, establish:

- **Reach**: how a tester actually gets to the product. A live or staging URL, a
  dev server command (such as `npm run dev`), a build then open, or an entry
  file. Prefer driving the real running thing over reading code. If a page is
  auth-guarded, find the documented way to reach an unguarded local copy.
- **Test tooling**: what is available to drive the UI. If Playwright or a
  headless browser is configured, testers should script real clicks and read
  real behaviour and screenshots. Note any environment quirks (for example a
  preinstalled browser path you should not reinstall).
- **Ship path** (only needed for fixes mode): the branch and PR convention, or
  the deploy command, and every guardrail around it: protected files, forbidden
  commands, rules that must be deployed separately, "verify before pushing." If
  none of this is documented, ask the user how to run and deploy before you
  touch anything.

## Phase 2: Personas, plan, and output mode

Draft a spread of personas, present the plan, and get an explicit go-ahead in the
form of the output-mode choice.

**Personas.** Default to 3, spaced along an expertise axis (up to ~5 for a broad
surface, down to 2 for a narrow function). Calibrate the axis to the band the
user described in Phase 0, and anchor the low end at the least technical person
who will realistically touch it: include that person as a persona unless the user
said the audience is uniformly technical. The novice end is where the onboarding
and jargon findings live, and it is the end most easily skipped by accident. Give
each persona exactly one level rather than a blended range. A persona hedged as
"L1 to L2" drifts upward in practice and stops reproducing a real novice's
confusion, which is the whole reason it is there. Space them out so you catch both
"I don't know what this word means" bugs and "this is subtly wrong and it is
wasting my time" bugs.

| Level | Who they are | What they surface |
|------|--------------|-------------------|
| **L1 Novice** | Minimal technical background; jargon stops them cold; needs hand-holding | Onboarding cliffs, unexplained terms, dead ends, missing guidance |
| **L2 Casual** | Comfortable with basic tools; copies and pastes; does not grok internals | Real-world paste/embed friction, unclear next steps, silent failures |
| **L3 Competent** | Edits HTML/CSS confidently, troubleshoots, but the tool's domain is not theirs | Edge cases, inconsistent state, workflow gaps |
| **L4 Expert** | Could build this themselves given time; impatient with friction | Subtle correctness bugs, wasted clicks, "why doesn't it just..." gaps |

Give each persona a concrete identity so the tester agent can stay in character:
name and one-line bio (role, org context); exactly one expertise level from the
scale (commit to a level, do not hedge with a range) and the tools they know;
their goal (tied to Phase 0); temperament (patient vs impatient, reads
instructions or ignores them, gives up easily or brute-forces); and how they
personally would judge "it worked."

**Plan.** Present the personas plus the scenarios each will attempt (map personas
to workflows; it is often better for several personas to attempt the *same* core
workflow so you can compare where each expertise level trips). State how they
will test (driving the real UI vs reasoning over rendered output), what gets
captured (a first-person narrative plus a structured issue log), and roughly how
many passes and agents, so the user knows the cost. Invite them to amend personas
or scope before you proceed.

**Output mode.** Use AskUserQuestion for this. It is a discrete choice that
changes everything downstream, and it doubles as the go-ahead:

1. **Report only** (the strong default; recommend this unless the user explicitly
   asked for fixes). No code changes; you produce a categorised UAT report of
   findings and recommendations.
2. **Fixes then mega draft PR.** Apply all fixes on a working branch, push, and
   open one draft PR collecting every fix for human review before it ships.
3. **Fixes then ship.** Apply, verify, and deploy via the project's normal path.
   Only offer or pick this when the project's conventions allow direct shipping
   and the change is low-stakes. It requires a final human confirmation right
   before the irreversible step (Phase 7).

Record the answer. It decides whether Phases 5 to 7 run the fix pipeline or you
stop at the report.

## Phase 3: Run the UAT passes (fan out tester agents)

Spawn one subagent per persona (via the Agent tool: `general-purpose`, or
`Explore` for read-only runs), in parallel (one message, one tool block per
persona). Each tester shares no memory with you or the others, so its prompt must
be self-contained: the full persona, the scope, how to reach the product (from
Phase 1), the scenario(s) to attempt, and the rules of engagement below.

### Staying in character is the whole ballgame

The value of this skill lives or dies on whether a persona reproduces a real
user's ignorance. An agent that quietly uses its own expertise to get unstuck
erases the exact finding you ran this to get. So instruct every tester:

- **Act only on what is visible in the UI.** If a label uses a term this persona
  would not know, the persona does not know it either, and that is a finding, not
  something to look past. Do not lean on outside knowledge of how the tool works
  internally.
- **Do not read the source to figure out what to do.** An L1 to L3 user cannot.
  Only an L4 might inspect rendered HTML, and only as far as a real impatient
  expert actually would.
- **React like a person, not a test harness.** Try the obvious thing, get
  confused, look for a cue, and if none exists, guess or give up, and log exactly
  where and why. Calibrate the confusion to the level: an L1 stalls on jargon and
  missing guidance; an L4 breezes past basics but bristles at wasted clicks and
  subtle wrongness.
- **Be honest, not theatrical.** Report what the product actually does, backed by
  evidence, not a dramatised failure. A tester who invents a bug is worse than
  useless.
- **Stay read-only.** Testers observe and report; they never edit the product.

Each tester logs every issue with: the step, observed behaviour, expected
behaviour, severity (blocker / major / minor / polish), and evidence (a quote
from the UI, a `file:line`, a screenshot path, the exact rendered output). It
returns both the narrative and the structured issue log.

### How testers actually exercise the thing
Pick the highest-fidelity method the environment supports (established in Phase 1):

1. **Drive the real UI.** If a browser driver is configured, script the actual
   clicks and typing and observe real behaviour and screenshots. This catches the
   bugs static reading never will.
2. **Reason over the rendered output or code** when driving the UI is not
   practical: read the actual HTML/JS/CSS the user would hit and trace what
   *would* happen, citing concrete lines. Never fabricate behaviour; if unsure,
   mark the issue "needs verification."

## Phase 4: Assess and categorise (one fresh assessor)

Hand all the narratives and issue logs to a single fresh assessor agent (fresh so
it is not biased by having "lived" any one run). It must:

- **Deduplicate**: the same friction from three personas is one flag. Note that
  it hit multiple expertise levels, which raises its priority.
- **Categorise** the flags into areas of improvement (onboarding/guidance, labels
  and copy, error handling, workflow gaps, correctness bugs, output/embed
  quality, performance, accessibility, trust/security signals).
- **Rank** by severity times frequency times how many expertise levels it
  blocked.
- **Return a structured flag list**: each flag has id, title, category, severity,
  which personas hit it, evidence, and a one-line "why it matters."

If output mode is report only, skip to Phase 7 (compile the report).

## Phase 5: Propose fixes (fixes mode only, swarm)

Dispatch a swarm of subagents, one per high-value flag, in parallel. Each
proposes a concrete fix: the exact change (diff-level where possible), the files
touched, why it resolves the flag, and any risk or side-effects. Keep proposals
surgical and in keeping with the codebase's constraints. A fix that adds a heavy
dependency or a phone-home to a set of lightweight tools is almost always wrong.

## Phase 6: Vet the proposals (skeptical reviewer)

For each proposal, verify three things before it is allowed to ship, ideally with
a separate skeptical reviewer agent so the author does not grade its own homework:

1. **Viability**: does it actually work, build, and not break neighbouring
   behaviour, and does it fit the codebase's conventions?
2. **Security and safety**: no injected script or XSS in generated output, no new
   external calls or data leaks, no auth or rules regressions, no secrets. This
   matters doubly when the output is code other people run or embed.
3. **Does it solve the flag?** Trace it back to the UAT evidence. A fix that is
   elegant but does not remove the friction the tester hit is rejected.

Drop or send back anything that fails a check. Prefer fewer, verified fixes over
many shaky ones.

## Phase 7: Ship and verify, or compile the report

**Fixes mode:** apply the vetted fixes, testing as you go so nothing regresses
(re-run the relevant tester scenario or the real UI against each fix to confirm
the friction is gone). Then route by the Phase 2 output mode:

- **Mega draft PR**: commit on the working branch, push, and open one draft PR
  summarising every fix and linking each to its UAT flag. Then watch the PR.
- **Ship**: follow the project's deploy path exactly, respecting every guardrail
  from Phase 1. Before the irreversible step (the push to the production branch,
  or the deploy command), stop and show the user exactly what will ship: the diff
  summary, the flags it closes, and the deploy target. Get an explicit
  confirmation. This is a hard gate: a deploy is consequential, and the user
  trusted the loop this far on the understanding that they would get the last
  word before anything went live. A surprise deploy is the one outcome that would
  break that trust.

**Report mode:** compile the report (template below) and hand it back. Offer, but
do not perform, the fix pipeline as a next step.

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
running list of flags (open / fixed / deferred) so a fresh round, or a fresh
session, can pick up cleanly.

## Report template (report mode, or the summary in fixes mode)
```markdown
# UAT Report: <scope>  ·  <date>

## Scope & method
<what was under test, how testers reached it, how many rounds>

## Personas
<name · expertise · goal, one line each>

## Findings (ranked)
### [BLOCKER] <title>  (hit by <personas>, <category>)
<what happens, expected, evidence>. Fix: <recommendation or what was done>.
### [MAJOR] ...
### [MINOR] ...
### [POLISH] ...

## What went well
<the flows that were smooth, worth protecting in future changes>

## Recommendations / changes shipped
<prioritised; in fixes mode, link each to its PR or commit>

## Not tested / open questions
<gaps, anything marked "needs verification">
```

## Orchestration notes

The Agent tool is the engine: parallel tester agents in Phase 3, one fresh
assessor in Phase 4, a fix swarm in Phase 5, skeptical reviewers in Phase 6.
Launch independent agents in a single message so they run concurrently. Keep the
interactive gates (Phases 0 to 2) in the main conversation; only the heavy
fan-out belongs in subagents.

## Pitfalls (the things that bite)

- **Personas leaking expertise.** An L1 that "just knows" the fix is not an L1.
  Holding character is where the real findings come from.
- **Fabricated bugs.** Every flag needs evidence; unverified observations are
  marked as such, not shipped as fact.
- **Author-graded fixes.** Vet proposals with a different agent than wrote them.
- **Deploying blind or by surprise.** In fixes mode, test each change before it
  ships, respect the project's deploy rules, and confirm with the user before the
  irreversible step.
- **Running out of road quietly.** When context gets tight, say so and hand off
  cleanly. A half-finished loop that pretends to be complete is the worst outcome.
