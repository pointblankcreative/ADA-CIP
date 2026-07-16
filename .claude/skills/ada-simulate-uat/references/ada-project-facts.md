# ADA project facts (Phase 1, pre-worked)

The **Phase 1: Confirm the project facts** reference for the ADA Campaign
Intelligence Platform. Unlike the generic skill, these facts are already
established — read this instead of rediscovering from scratch. But facts drift
(URLs, gates, the agent fleet, the owed staging verifications all move), so
**re-confirm the load-bearing ones** against the current `CLAUDE.md`, the repo,
and a quick reach check before you fan out testers. If any fact below has gone
stale, trust the repo over this file and note the drift.

---

## What the product is (this shapes what a good fix looks like)

ADA-CIP is a **custom, internal, single-tenant campaign-intelligence platform**
for Point Blank Creative (a political ad agency) that replaced Funnel.io +
Looker. It monitors 5–15 concurrent multi-platform buys (Meta, Google Ads,
LinkedIn, StackAdapt, TikTok, Snapchat, Perion/Hivestack DOOH) for candidate,
union, and advocacy clients, with **Canadian data residency** (BigQuery in
`northamerica-northeast1`, Montreal).

Stack: Next.js 14 + React + TypeScript + Tailwind + Recharts (frontend);
FastAPI + Pydantic v2 + Uvicorn (backend); Google BigQuery (`cip` dataset in
`point-blank-ada`); Cloud Run behind Identity-Aware Proxy (IAP).

This design **obsesses over honesty and legibility**, and that is what makes a
fix good or bad here:

- **Honesty over raw tokens.** Missing/thin/early data reads as an honest,
  non-alarming sentence — `guardCopy()` maps ~90 machine tokens (e.g.
  `min_impressions_1000`, `single_platform`) to plain English across three
  categories (pending / na / waiting), plus a distinct "too early to call"
  band. Never surface a raw token, a bare zero, a blank, or a scary red where
  the honest answer is "not enough data yet."
- **Plain language over codes.** Diagnostic signal NAMES lead; codes (D3, A4,
  C1, F3) are demoted to tooltips. Every underlined metric term should have a
  working glossary popover (`components/glossary.tsx` + `lib/glossary.ts`).
- **Observe, don't command.** Advisory engine copy (A5/F3 bodies) observes; it
  never orders. Guarded by `tests/test_diagnostics_voice.py`. Suggested moves
  carry an owning team pill (Media/Trading/Creative/Web/Client) that names *who
  performs* the move, not necessarily the viewer.
- **Reach/frequency source of truth.** Spend/impressions/clicks always come from
  Funnel. Reach & frequency come from Funnel's 7-day fields for every platform
  EXCEPT StackAdapt; for StackAdapt they come from the direct
  `reachFrequency` feed (`cip_stackadapt.stackadapt_reach_frequency`) as a
  **current calendar-month dedup bucket, never a summed flight-to-date figure**
  (Funnel's 1-day SA field overcounts true dedup reach 7–10×). Reach is shown
  individual vs household/residential separately. **Never reintroduce the Funnel
  StackAdapt reach columns into any user-facing aggregate.**
- **The worst possible bug is a wrong client-facing number.** Account directors
  paste these figures into decks a client will interrogate. Correctness and
  honest glosses (e.g. "over 100% is normal" on sessions-per-click) beat polish.

---

## Reach (how a tester actually gets to the product)

**You cannot point a browser at staging or production from this sandbox.** Both
frontends are IAP-guarded and return `302` to interactive Google SSO; both
backends are IAM-private Cloud Run and return a flat `403` on `/`, `/health`,
and `/docs`. There is no ADC, no gcloud, no IAP cookie here, and the SSO screen
cannot be scripted headlessly. Do **not** aim Playwright at a live URL — it will
only ever reach the Google login page.

The deployed URLs (for reference and for the smoke check, not for tester drive):

- Production frontend: `https://cip-frontend-807520113440.northamerica-northeast1.run.app`
- Production backend:  `https://cip-backend-807520113440.northamerica-northeast1.run.app`
- Staging frontend:    `https://cip-frontend-staging-807520113440.northamerica-northeast1.run.app`
- Staging backend:     `https://cip-backend-staging-807520113440.northamerica-northeast1.run.app`

Ranked reach recipes, highest fidelity first:

**Recipe 1 — human on live staging IAP UI (NOT sandbox-doable; the true stack).**
A human whose Google account is granted "IAP-secured Web App User" on the
`cip-frontend-staging` Cloud Run service opens the staging frontend in a real
browser, completes SSO, and lands on the Flightdeck. Every tab then works
against real BigQuery `cip` data because the browser calls the frontend's own
same-origin `/api/*` path (covered by the frontend's IAP), and the server-side
proxy (`frontend/src/app/api/[...path]/route.ts`) forwards each call to the
IAM-private backend with a Google-minted identity token. This is the ONLY path
that exercises the real pacing/diagnostics engines, real proxy auth, and real
IAP — and the only way to close the two staging verifications `CLAUDE.md` still
owes (#123 Full History Backfill end-to-end; #114 StackAdapt-only reach on
26022 CATIE). When a UAT question needs Recipe 1, say so and hand it to Frazer;
do not fake it.

**Recipe 2 — local `next dev` + fixture backend seeded from the BigQuery MCP
(best sandbox-feasible interactive fidelity).**
1. Pull representative REAL rows via the read-only BigQuery MCP
   (`mcp__Google_Cloud_BigQuery__execute_sql_readonly` on
   `point-blank-ada.cip.*`) for the flows under test — at minimum a project
   list plus one project's pacing/performance/diagnostics — and save as JSON
   fixtures.
2. Serve those fixtures from a tiny local stub (30-line FastAPI/Flask on `:8000`)
   at the EXACT paths `frontend/src/lib/api.ts` calls: `/api/projects/`,
   `/api/projects/{code}`, `/api/pacing/{code}` (+ `/history`, `/run`),
   `/api/performance/{code}` (+ `/adsets`, `/ads`, `/creatives`),
   `/api/projects/{code}/creative/rotation|matrix`,
   `/api/projects/{code}/audiences/matrix`, `/api/diagnostics/{code}`
   (+ `/history`, `/run`), `/api/alerts/`, `/api/benchmarks/{code}`,
   `/api/ffs/{code}`.
3. `cp -r /home/user/ADA-CIP/frontend/. /tmp/cip-fe/ && cd /tmp/cip-fe &&
   rm -rf node_modules && npm install --legacy-peer-deps`, then
   `NEXT_PUBLIC_API_URL=http://localhost:8000 npx next dev -p 3000`.
4. Drive `http://localhost:3000` with the preinstalled Chromium via the
   Playwright library (see Test tooling). localhost has NO IAP, so headless
   drive + screenshots work. `route.ts` falls back to an unauthenticated call to
   `NEXT_PUBLIC_API_URL` when there is no metadata server, so a plain localhost
   stub answers.
   Fidelity: full real UI/components/interactions with representative real
   numbers — but the real pacing/diagnostics MATH and the real proxy/IAP path
   are bypassed by fixtures, so data-correctness fidelity is only as good as the
   rows you seed.

**Recipe 2b — real backend locally (boots, but returns NO data in-sandbox).**
`cd /tmp && python3 -m pip install -r /home/user/ADA-CIP/requirements.txt
--ignore-installed`, then `PYTHONPATH=/home/user/ADA-CIP python3 -m uvicorn
backend.main:app --port 8000`. It boots (lifespan swallows the BQ-client
failure) and the auth middleware stubs a dev user, so routing/validation/engine
code runs — but `bigquery.Client()` needs ADC the sandbox lacks, and the app
process can't call the BigQuery MCP, so every data endpoint 500s. Use only to
exercise request/response shape and engine code paths, not data. Prefer Recipe
2's fixture stub for populated screens. **Safety here is by rule, not by luck:**
even if this real backend were run somewhere credentials exist (the resolver's own
commands sometimes run on Frazer's Mac, not only this credential-less sandbox), a
tester still must never send it a writable query or a `/run` / sync / backfill /
admin request — the "every data endpoint 500s in-sandbox" behaviour is a fidelity
caveat, not the safety guarantee.

**Recipe 3 — static evaluation over real BigQuery via the MCP (highest DATA
fidelity, zero UI).**
For a tab/metric under test, read the router (`backend/routers/pacing.py`,
`performance.py`, `diagnostics/engine.py`, `creative.py`) and its SQL, reproduce
the query against real `cip` / `cip_stackadapt` data through
`mcp__Google_Cloud_BigQuery__execute_sql_readonly`, then reason about what the
component (`lib/flight.ts`, `lib/creative.ts`, `lib/glossary.ts`, the tab
components) would render from that JSON. Best when the UAT question is about
specific numbers, thresholds (Section 10 alert bands), diagnostic scores,
StackAdapt dedup reach, or pacing math — where correctness, not look-and-feel,
is the point.

**Frontend does not render meaningfully without a backend.** `page.tsx` is a
client component whose `load()` calls `api.projects.list()` on mount; on fetch
failure it flips to an error/empty state. There is no static fallback content, so
a tester needs at least Recipe 2's stub (or Recipe 1's live backend) to see
populated screens.

**Primary method for most testers, stated plainly:** reasoning over rendered
React components + backend router JSON (Recipe 3, plus reading
`frontend/src/components`, `lib/flight.ts`, `lib/creative.ts`, `lib/glossary.ts`,
`components/glossary.tsx`, and the diagnostics engine copy). Stand up Recipe 2
when the finding is about interaction/usability/visual flow and you want real
clicks and screenshots.

---

## Testers stay READ-ONLY against a LIVE warehouse

ADA is backed by a live BigQuery warehouse holding real client campaign data.
Testers **must never mutate real data and never hit production**:

- Use ONLY `mcp__Google_Cloud_BigQuery__execute_sql_readonly` for data — never
  `execute_sql` (the writable variant), never any INSERT/UPDATE/DELETE/TRUNCATE.
- Never trigger a real sync, backfill, media-plan re-sync, or
  `/run` transformation against staging or prod — the Full History Backfill
  TRUNCATEs `fact_digital_daily`, and media-plan sync's `_clear_existing_plan()`
  DELETEs manual edits. Testing those flows for real is a Recipe 1 human step
  Frazer owns; in the sandbox you test their honest state transitions against the
  fixture stub, not the live job.
- Do not aim Playwright at the live IAP URLs. localhost fixture UI only.

---

## Test tooling and verification gates

**Playwright is available; do not reinstall browsers.** The `playwright`
automation LIBRARY (not the `@playwright/test` runner) is global at
`/opt/node22/lib/node_modules/playwright` (CLI 1.56.1). Chromium is preinstalled
at `PLAYWRIGHT_BROWSERS_PATH=/opt/pw-browsers` — never run `playwright install`.
Drive the browser with a plain Node script using `chromium.launch()`, run as:
`PLAYWRIGHT_BROWSERS_PATH=/opt/pw-browsers NODE_PATH=/opt/node22/lib/node_modules
node your_uat.js`. Both env vars are required — a bare `require('playwright')`
fails without `NODE_PATH`. The project itself ships NO test framework and no
Playwright config; you write raw library scripts, not `test()`/`expect()`.

**Toolchain:** Node v22.22.2, npm 10.9.7, Python 3.11. Use `npm install
--legacy-peer-deps` (`npm ci` is broken: eslint 9 vs eslint-config-next).

**The authoritative gates** (run these on any fix, and to confirm a fix landed):

- **Frontend type gate — THE gate:** copy frontend to `/tmp`
  (`cp -r $REPO/frontend/. /tmp/cip-fe/`), remove `node_modules`,
  `npm install --legacy-peer-deps`, then `npx tsc --noEmit` (strict, noEmit).
  `next build` may be OOM-killed in the sandbox and is NOT a pass/fail signal;
  there is no ESLint config, so `next lint` isn't a reliable gate either.
- **Backend tests — authoritative suite:** run from OUTSIDE the repo (the repo
  `.env` CORS list breaks pydantic-settings parsing):
  `cd /tmp && PYTHONPATH=$REPO python3 -m pytest $REPO/backend/tests -q`
  (234 passing as of 2026-07-05). Install deps with
  `python3 -m pip install -r $REPO/requirements.txt --ignore-installed`
  (PyJWT RECORD conflict otherwise).
- **Top-level `tests/` stale baseline:** the `$REPO/tests` tree has drifted —
  ~530 pass with a documented **~14 stale failures** (recorded in the resolver's
  `scripts/known_stale_tests.txt`) that predate prod source changes and are NOT
  regressions. Do not read a raw "N failed" from the full suite as RED.
  `gates.sh` runs `tests/` but fails only on failures *outside* that baseline.
- **Guard tests that must stay green:** `tests/test_plan_id_dedup_guard.py` (any
  file reading `media_plan_lines` more than once must register there);
  `tests/test_diagnostics_voice.py` (A5/F3 observe-don't-command voice).
- **Prefer the resolver's gate runner.** For a fix, point the pipeline at
  `.claude/skills/ada-resolve-ticket/scripts/gates.sh <worktree>` — it runs both
  pytest tiers with the stale-baseline allowance and the tsc gate exactly as the
  project expects, rather than an ad-hoc test invocation.
- **BigQuery MCP** (`execute_sql_readonly` on `cip` / `cip_stackadapt` /
  `core_funnel_export`) is how you verify a data claim.

---

## Ship path (fixes mode) and guardrails

**Branch model:** `main` → staging; `production` → production. Merging to `main`
deploys STAGING (~7 min via `.github/workflows/deploy.yml`), NOT production.
**Promoting `main` → `production` is a separate manual push done ONLY by Frazer —
never automated by this skill, never in scope.** The sandbox cannot `git push`;
it delivers by opening a **draft PR to `main`** off a per-fix branch and handing
the branch off. Production is never touched.

**Autonomy boundary (area.py park classifier).** A fix may target staging only if
every file it touches is frontend or isolated backend (auto zones:
`frontend/`, `backend/routers/`, `backend/models/`, `backend/middleware/`,
`backend/services/diagnostics/`, `tests/`, `backend/tests/`, `docs/`,
`backend/config.py`). Anything touching BigQuery, schema, ingestion, or transform
— `ingestion/`, `infrastructure/bigquery/`, `scripts/`, any `.sql`,
`transformation.py` / `media_plan_sync.py` / `daily_job.py` /
`creative_assets.py` — or an unrecognised zone, **parks**: build it and hand it
to Frazer, never ship it. The check is
`git diff --name-only origin/main | python3
.claude/skills/ada-resolve-ticket/scripts/area.py --stdin` (exit 3 = must-park).
A ticket body that says "review before staging" also forces park.

**Hard guardrails (from `CLAUDE.md` Section 4) any fix must honour:**
- **IAP stays private.** `deploy.yml` re-asserts `--invoker-iam-check` on every
  deploy; the backend must stay IAM-private. A change must not open a cookie-less
  path — a protected route returning 200 to an unauthenticated caller is a hard
  fail (this is exactly the `ada-smoke` check).
- **BigQuery cross-region DML ban.** DML (INSERT/UPDATE/DELETE) cannot target the
  Montreal `cip` tables from a US-region source. Use the Python
  `SELECT` + `load_table_from_json()` pattern, never `INSERT INTO … SELECT FROM`.
- **CORS format.** deploy.yml passes CORS as a comma-separated string of full
  URLs with protocol, no trailing slashes, no spaces.
- **`media_plan_sync.py` em-dashes are load-bearing** (log messages) —
  preserve them; and `_clear_existing_plan()` DELETEs all lines before
  re-insert, so manual edits are lost (destructive — treat as park-class).
- **plan_id dedup guard:** any file reading `media_plan_lines` twice must
  register in `tests/test_plan_id_dedup_guard.py`.
- **Data honesty / source of truth:** never reintroduce Funnel StackAdapt
  reach/frequency into an aggregate; never invent or smooth over a value on a
  client-facing surface; honour honest "not reporting" copy.
- **No secrets, no new heavy dependency** (lean `requirements.txt` and frontend
  deps matter).

**The ADA agent fleet does the ship pipeline — do not reinvent it.** Delegate
propose/build to `ada-builder`, code review + park backstop to `ada-reviewer`,
user-acceptance to `ada-uat-reviewer`, and the post-staging cookie-less IAP curl
to `ada-smoke`. The one-ticket-per-run resolver skill `ada-resolve-ticket` is the
canonical path for landing a filed fix.

---

## Asana board (the ADA-native fix destination)

- Project: **ADA Campaign Intelligence Platform**, GID `1215988273595218`.
- Custom fields: Priority `1215988107013686` (High `…687` / Medium `…688` /
  Low `…689`); Status `1215988107013691` (Not started / In progress / Completed);
  Stage `1215988107013696`; **Ready For** `1216308984626884`
  (🤖 Agent `1216308984626886` / 👨🏻‍💻 Frazer `1216308984626885`).
- The "Ready For: 🤖 Agent" queue is what `ada-resolve-ticket` drains one ticket
  per run. Filing a verified UAT flag as a ticket (via the Asana MCP
  `create_tasks` / `update_tasks`) with Ready For → Agent for auto-eligible flags
  or → Frazer for park-class ones feeds that pipeline under full
  propose/review/park discipline.

---

## Owed staging verifications (least battle-tested — highest-value to re-test)

`CLAUDE.md` still owes two staging checks that only Recipe 1 (a granted human on
live staging) can close — Recipes 2/3 cannot validate them:
1. **#123 Full History Backfill end-to-end** — instant button disable, honest
   running → success, no false "Failed to fetch", a mid-run second click returns
   a clean 409.
2. **#114 StackAdapt-only campaign (26022 CATIE)** — real dedup reach/frequency
   + a diagnostics score computed on dedup reach; individual vs household stay
   separate; reach not summed across months.

---

## Sandbox reality (assume nothing can deploy)

- The sandbox **cannot `git push`** — hand commands/branches to Frazer.
- It **cannot reach IAP-guarded URLs** cookie-less (302/403); the live UI is
  human-only from here.
- Asana ticket filing goes over the **Asana MCP**, not the resolver's Python
  scripts (no `ASANA_PAT` here).
- Delivery stops at **staging** (a handed-off draft PR to `main`) or a parked
  draft PR. **Production is never touched by this skill.**
