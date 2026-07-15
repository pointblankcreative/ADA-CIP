# CLAUDE.md — ADA-CIP Project Context

## Section 1: Project Overview

Custom-built platform for Point Blank Creative Inc. replacing Funnel.io and Looker. Centralises campaign monitoring, budget pacing, automated reporting, and client-facing dashboards for a political advertising agency running 5-15 concurrent campaigns across Meta, Google Ads, LinkedIn, StackAdapt, TikTok, Snapchat, and Perion/Hivestack (DOOH).

## Section 2: Current Status (Updated 2026-07-14)

**Phase:** UAT closeout shipped to production. Production was promoted **2026-07-14** to `92a4f46` (clean fast-forward; Deploy CIP green — both health checks + the IAM-lock/`--invoker-iam-check` steps passed), bringing prod level with staging (this doc-only commit is the sole `main`-ahead-of-`production` delta since). That promotion carried **9 PRs**: #114 (StackAdapt R&F Stage 2), #115 (diagnostic signal-code key + suggested-move owner tags), #116 (one-time "underlined terms have definitions" hint), #118 (video-metric quartile funnel), and the 2026-07-14 closeout drain **#119–#123** (below). Prior prod was `4934ce3` (#112, promoted 2026-07-13); the full-UI-overhaul + 24-ticket UAT round went to prod 2026-07-05 via `2d3bef9`.

> **Verification closeout (2026-07-15, branch `claude/full-history-backfill-verify-1lux34`):** (1) #114 ✅ — verified via the direct-feed read path on BigQuery: StackAdapt-only **26022 CATIE** surfaces real current-month dedup reach (top line Lookalike Banner = 41,502 individual / 14,557 household, freq 3.32 = impressions ÷ reach exactly), household stored separately; Funnel's hidden SA reach for the same campaign is 702 (1-day snapshot) or 80,305 (summed, double-counted) with freq 0.0 — confirming why it's excluded. The direct feed was re-synced 2026-07-15 (17 campaigns, all 3 grains), so the live key works and diagnostics D1–D4 score SA on real dedup reach. (2) #123 — the CODE is verified (instant button-disable before any await, honest running/success/failed/stalled, no false "Failed to fetch", 409 guard correct even against the current stale `running` rows). **But a real Full History Backfill has NOT completed since 2026-06-21** — the last 4 `transform_full` runs are stuck `running` (never truncated `fact_digital_daily`; the table stays healthy because they die during the all-history US read *before* the TRUNCATE, and the twice-daily daily pipeline keeps it fresh). Root cause is almost certainly the backend Cloud Run `--timeout=300`; the April 2026 success ran in 76s but the all-history read has since grown past the 5-min request cap. **Raise `--timeout` (and ideally `--memory`) before running a real backfill** — a run that survives past the read-then-TRUNCATE and dies during the reload would leave prod's shared `fact_digital_daily` empty. See the 2026-07-15 closeout report.

### Shipped 2026-07-14 (closeout drain → production, promoted 2026-07-14)
Autonomous drain of the "Ready For: 🤖 Agent" queue, each ticket through the full propose→review→UAT→build→build-review pipeline:
- **#119 — dead-code cleanup:** removed `performance-tab.tsx`, `orbit-intro.tsx`, `lib/viz/audio-engine.ts` (all unimported; only comment refs remained) + this status refresh. `tsc` clean.
- **#120 — UI copy/label polish (ADA 1215990207548378):** labeled the pacing oscilloscope's `y=100` "100% = ON PLAN" reference line; retrospective/history date-picker caption. Verified AI-028/035/081 already moot; split out AI-034 (CAPE casing = ingest/park) and AI-036 (date-range picker = feature) to their own tickets.
- **#121 — metric-differs-between-tabs (ADA 1215990182814162):** scope labels on the Diagnostics tab (per-objective / flight-to-date vs the Pacing/Summary project-total, as-of-today figures) so differing tab numbers read as intentional. Deferred the deeper same-scope engine-vs-router pacing reconciliation (needs `backend/services/pacing.py` = park) to its own ticket.
- **#122 — internal status wording (ADA 1215990182814034):** collapsed three byte-identical status→CSS-var helpers into one `diagnosticVar` (`lib/utils.ts`); aligned the audiences tab's local `"ACT"`→`"ACTION"`. Zero user-visible/wire/data change — the `STRONG`/`WATCH`/`ACTION` values stay (backend `StatusBand` enum, stored in BigQuery `fact_diagnostic_signals`).
- **#123 — non-blocking full re-import (ADA 1215990005858989):** the 'Full History Backfill' no longer shows a false "Failed to fetch". `api_run_transformation` now runs via `await asyncio.to_thread(run_transformation, mode)` (still synchronous — response waits — but frees the single uvicorn event loop; NOT BackgroundTasks, which Cloud Run's `--min-instances=0`/no-CPU-always would kill); new read-only `GET /transformation-status` reads the latest `transform_full` ingestion_log row; frontend fires-and-polls (honest running/success/failed/stalled). **409 concurrency guard** (FULL only, 60-min active window) + synchronous button-disable prevent two concurrent `TRUNCATE fact_digital_daily`.
- **Parked (not shipped):** Meta thumbnails + audience-estimate verify (ADA 1215990207612555) — 64px re-heal needs `creative_assets.py` (park) + a live Meta token; estimates need a live campaign. Flipped to Frazer.
- **StackAdapt R&F (ADA 1215990005858637):** ±2% reconciliation passed; the McGill June coverage gap is closed — a full sync ran 2026-07-13 21:34 and `cip_stackadapt` now holds 17 campaigns incl. McGill Video (1,380 ind. reach) + Banner (2,048), matching the report.

### Shipped 2026-07-03 → 07-05 (main → production, promoted 2026-07-05)
- **UAT round (PRs #97 + #98, 24 tickets):** diagnostics legibility (signal names lead, codes D3/A4 demoted to tooltips; a health-score legend that states it is *different from pacing*; a metric **glossary** primitive — `components/glossary.tsx` + `lib/glossary.ts` — with hover/focus popovers for pillars/cost-metrics/signals; honest "not reporting" copy via `guardCopy()` instead of raw tokens like `min_impressions_1000`); advisory engine voice (A5/F3 bodies observe, don't command — guarded by `tests/test_diagnostics_voice.py`); suggested moves carry an owning team (`SIGNAL_ACTIONS` → `SignalAction {action, owner, hedge?, platform?}`).
- **Alerts truthfulness (#2/#22):** honest feed empty/error states + live Act-now count; coverage-gated severity (`ALERT_LOW_COVERAGE_THRESHOLD = 0.10` downgrades thin-evidence ACTION alerts critical→warning); `ack_note` column added to `cip.alerts` (migration `infrastructure/bigquery/migrations/2026-07-03_alerts_ack_note.sql`, applied out-of-band — fixed a feed 500; staging+prod share the single `cip` dataset so it is already live for both).
- **Creative/Audiences (#5/#8/#10/#11/#17-23):** per-platform video 25/50/75/100 drop-off anchored at q25 (never `video_views`); one report-ready conversions figure + honest over-100% gloss; single "HOOK RATE" term + `n/a` static; unnamed-creative fallback keeps the raw token (backend `_alias_resolution` NULLIF); per-metric quartile direction cues.
- **Flightdeck/pacing (#15/#21/#24):** `is_direct` toggle gains confirm + one-shot Undo + honest re-pace tooltips; line codes behind a Show IDs toggle; Listen/audio mode removed (Signals + oscilloscope); "Unmapped Spend" de-escalated + collapsed by default; self-serve vs direct-buy budget split on the verdict hero.
- **Loading/intro (#12):** honest loading text; full-screen orbit-boot intro removed (`intro-provider.tsx` gutted to a no-op context shell; `orbit-intro.tsx` + `lib/viz/audio-engine.ts` orphaned, then removed in the 2026-07-14 closeout).
- **CI/GA4:** deploy.yml re-asserts `--invoker-iam-check` on every deploy (backend stays IAM-private); GA4 project attribution via `LEFT(session_campaign, LEN(code))`.

### Shipped 2026-06-11 → 06-12 (main/staging)
- **Design system:** PB tokens (light default, dark-ready), Folsom/Inter/Chivo Mono via next/font (font variables MUST stay on `<html>`, not `<body>`), semantic Tailwind mapping, favicon set.
- **Shell + Flightdeck:** top bar + ⌘K palette replace the sidebar; Flightdeck replaces Overview at `/` (portfolio pulse, attention list, flight rows, Signals orbit for active campaigns).
- **Project tabs:** Summary · Pacing · Creative · Audiences · Diagnostics (Settings behind the gear; legacy `?tab=performance` → creative). Summary is verdict-first (`lib/flight.ts`).
- **Pacing:** orbit instrument ("lines in flight", opt-in audio), envelope history chart.
- **Diagnostics:** Triage Board (ACT NOW / KEEP AN EYE ON / HEALTHY / NOT REPORTING); engine copy rewritten in plain language with platform display labels (`shared/normalization.platform_label` — keep in sync with frontend `platformLabel`); layered evidence (plain meaning → curated facts → raw tree); per-signal alert banners deduped against board cards; band-zone gauges (`band-scale.tsx`: 70+ is the goal, not 100).
- **Creative tab ("Call Sheet"):** rule-generated verdicts in `lib/creative.ts` (SCALE/HOLD/REFRESH/EARLY; awareness ranks on completion rate, conversion/mixed on CPA — deliberately NOT CPCV, clients see rates); rotation cards with attention funnels + 8-day sparklines; creative×platform matrix with KPI lenses (incl. CPM); reporting strip; GA4 after-the-click; long-tables drawer; alias rename; real thumbnails.
- **Audiences tab ("Electorate"):** audience×creative matrix with lenses; dossiers (response stack vs PB-history quartiles, frequency trend, Meta personas, saturation slots).
- **Backend (Phase 14):** `routers/creative.py` — rotation (`window=flight|7d`), creative/matrix, audiences/matrix; benchmarks extended with hook_rate/engagement_rate quartiles; mixed projects get MERGED awareness+conversion benchmark sets (per-metric preference). 1,000-impression honesty guards throughout.
- **Creative assets (Phase 19):** `services/creative_assets.py` — time-budgeted sync (240s, store-as-you-scan, per-source status, `POST /api/admin/creative-assets/sync?force=true`); Meta images fetched ONCE → GCS (`creative-assets/` in the alert-charts bucket) → served via the backend image proxy `/api/projects/creative-assets/image?variant=` (signed URLs abandoned — SA lacks signBlob); Meta targeting specs → deterministic personas + delivery-estimate pool sizes in `cip.adset_targeting`; daily pipeline Stage 1d. Secrets `cip-meta-token`/`cip-stackadapt-key` → env `META_ACCESS_TOKEN`/`STACKADAPT_API_KEY` (runtime SA needed per-secret secretAccessor grants).
- **Slack alerts:** Block Kit redesign, verdict-word headlines, brand severity colours; alert acknowledge with IAP user + optional note (`ack_note`); twice-daily sync countdown in UI (2:30 AM/PM America/Vancouver).

### Open items
- **StackAdapt creative matching:** ~7 `no_match` statics — SA creative-library names ≠ Funnel ad names; match via SA campaign→ad relationship instead of creative name.
- **StackAdapt R&F direct feed (ADA 1215990005858637):** Stage 1 (ETL + `cip_stackadapt.stackadapt_reach_frequency`, PR #112) and Stage 2 (read-path FILL in `performance.py` + diagnostics engine SA source swap + household/individual model split) shipped. Funnel's SA reach stays excluded from every SQL aggregate; the real numbers come from the direct feed's current calendar-month bucket. Remaining: ±2% reconciliation vs the StackAdapt R&F report on the first live campaign; frontend household surface is currently just the Reach-KPI household sub-line (deeper per-campaign household display deferred).
- **pool_size/saturation null:** Meta refuses delivery estimates on ENDED ad sets; verify on the first live campaign (estimate circuit breaker stops after 6 consecutive refusals per run).
- **Post-promotion verify-after (2026-07-14 promotion):** confirm `cip-sheets-reader` still holds `roles/run.invoker` on prod `cip-backend` after the `--invoker-iam-check` enforcement, so the twice-daily scheduler sync (2:30 AM/PM) doesn't 403 — deploy.yml does NOT manage that binding (only `infrastructure/deploy.sh` does). Also confirm `GET /api/alerts/` = 200 on prod (ack_note present — should already be true via the shared `cip` dataset). **[2026-07-15 verify]** Strong indirect evidence the binding is intact: the OIDC scheduler (`cip-sheets-reader` → `/api/admin/daily-run`) is still producing successful `transform_daily` runs twice daily through 2026-07-15 09:30 UTC, which would 403 if the binding were gone; `cip.alerts.ack_note` confirmed present (STRING; 209 rows, queryable) so the feed returns 200 for an authed caller (a raw unauth curl returns 403 = the healthy IAM wall). Definitive `gcloud run services get-iam-policy` check + idempotent re-bind still owed to Frazer (Claude sandbox has no gcloud).
- ✅ Dead-code cleanup (2026-07-15): `components/campaign-table.tsx`, `components/ad-drilldown.tsx`, `components/adset-drilldown.tsx` removed — orphaned remnants of the retired Performance tab (replaced by Creative + Audiences), zero importers in `frontend/src` (`CampaignTable`/`AdDrillDown`/`AdSetDrillDown` had only their own definitions). `tsc --noEmit` clean after removal. Prior 2026-07-14 pass removed `performance-tab.tsx`, `orbit-intro.tsx`, `lib/viz/audio-engine.ts`.
- **Verification findings (2026-07-15 closeout, adversarially confirmed — pre-existing, NOT this branch; each wants its own ticket):**
  - *Diagnostics StackAdapt reach leak (edge case):* `services/diagnostics/engine.py` excludes Funnel's SA reach only CONDITIONALLY — `adset_sql` (454–470) pulls SA reach with no exclusion, and the direct-feed override drops+rebuilds the SA `adset_bucket` entries only inside `if sa_rows:` (552). When the current-month direct feed is absent (month-start before the daily sync writes the new period=30 bucket, any feed read error, or a campaign_id mismatch) Funnel's 7–10× inflated SA reach survives into D1–D4, so the Diagnostics tab can show wrong/too-healthy reach while the Summary tab (`performance.py`, which excludes it UNCONDITIONALLY) correctly shows "not reporting". Violates the "never reintroduce Funnel SA reach" invariant. Currently DORMANT (July bucket synced for 26022). `test_diagnostics_keeps_funnel_reach_when_direct_feed_empty` locks the leak in as "graceful degradation". Fix: drop SA from `adset_bucket` unconditionally, independent of the `if sa_rows:` rebuild.
  - *Full History Backfill data-safety hardening (PR #123 / the transform):* the FULL-mode `TRUNCATE fact_digital_daily` (`transformation.py:376`) is not atomic — a reload failure after the truncate leaves prod's shared table EMPTY (no staging-swap, no restore). The 60-min 409 window doubles as BOTH the concurrency guard and the "dead run" cutoff, and the UI's "stalled" state re-enables the button, so a >60-min run can be re-clicked into a concurrent TRUNCATE+reload; a TOCTOU gap also exists (guard check yields before the 'running' row is written). Reduced real-world risk by single-user rollout. Before any real backfill: raise the backend Cloud Run `--timeout` (currently 300s — the actual reason the last 4 full runs die mid-read) AND ideally snapshot `fact_digital_daily` / add a staging-swap.
- Deferred: audience CTR-trend sparkline slot, per-row matrix narrative reads, dark-mode toggle, ESLint setup (repo has no config; `tsc --noEmit` is the type gate). Minor: `frontend/tsconfig.tsbuildinfo` is a committed TS incremental-build cache — harmless but ideally gitignored (it self-heals on the next build; it still lists the 2026-07-15-deleted files until then).
- Carried from 2026-04: diagnostic threshold calibration pass, FFS wizard, Quality pillar (blocked on CRM), Asana backlog (tab confirmation UI, blurred creative underlay, client logos, client-level benchmarks).

### Engine + data (stable since 2026-04-20)
Pipeline Funnel.io → BigQuery for all 8 platforms; GA4 ingestion (the GA4 config dropdown lists properties present in `fact_ga4_daily` — a client property must be connected as a Funnel source to appear; GA4 ownership irrelevant). Diagnostic Signal Engine live for persuasion (D/A/R) + conversion (C/F), mixed campaigns dual-output; Quality pillar deferred (CRM). Convention: any file reading `media_plan_lines` more than once must register in `tests/test_plan_id_dedup_guard.py`.

### Verification recipes (Claude sandbox)
- Backend: run pytest from a directory OUTSIDE the repo (the repo `.env` CORS list breaks pydantic-settings parsing). The **maintained** suite is `backend/tests`: `cd /tmp && PYTHONPATH=$REPO python3 -m pytest $REPO/backend/tests -q` (234 passing, 2026-07-05). NOTE: the top-level `$REPO/tests` tree has drifted — 530 pass / **14 stale failures** (duplicate `tests/test_pacing.py` + `tests/test_pacing_router_retro.py` + `tests/test_retrospective_mode.py` + `tests/test_media_plan_platform_map.py` mocks that predate the `_sync_in_progress` / is_direct / retro source changes already in prod — NOT regressions). Treat `backend/tests` as the gate; retire or reconcile the stale duplicates. Install deps with `python3 -m pip install -r $REPO/requirements.txt --ignore-installed` (PyJWT RECORD conflict otherwise).
- Frontend: copy to /tmp (`cp -r $REPO/frontend/. /tmp/cip-fe/` — rsync may be absent), remove node_modules, `npm install --legacy-peer-deps` (`npm ci` broken: eslint 9 vs eslint-config-next), then `npx tsc --noEmit` (clean as of 2026-07-05). Full `next build` may be OOM-killed in the sandbox; tsc is the gate.

### Current Users
Only Frazer so far. Goal is team-wide adoption on rollout.

### Branch Model
`main` → staging, `production` → production (auto-deploy via GitHub Actions, ~7 min). Create a branch per PR (`feat/ada-ui-overhaul` carried the whole overhaul), merge to main, promote main → production when ready. Claude's sandbox cannot run git push — Frazer runs handed-off commands.

## Section 3: Architecture

- **Frontend:** Next.js 14+ with React, TypeScript, Tailwind CSS, Recharts
- **Backend API:** FastAPI (Python 3.11+), Pydantic v2, Uvicorn
- **Data Warehouse:** Google BigQuery (dataset: `cip` in project `point-blank-ada`, region `northamerica-northeast1`)
- **Auth:** Google Cloud Identity-Aware Proxy (IAP) on Cloud Run
- **Hosting:** Google Cloud Run (containers), auto-deployed from main branch
- **Region:** northamerica-northeast1 (Montreal) — Canadian data residency

### URLs
- **Production Backend:** `https://cip-backend-807520113440.northamerica-northeast1.run.app`
- **Production Frontend:** `https://cip-frontend-807520113440.northamerica-northeast1.run.app`
- **Staging Backend:** `https://cip-backend-staging-807520113440.northamerica-northeast1.run.app`
- **Staging Frontend:** `https://cip-frontend-staging-807520113440.northamerica-northeast1.run.app`

## Section 4: Critical Deployment & Development Gotchas

These are hard-won lessons — do NOT skip this section:

### 1. BigQuery Cross-Region
The `cip` dataset is in `northamerica-northeast1` (Montreal). DML statements (INSERT, UPDATE, DELETE) **CANNOT target cross-region tables**. If you need to move data from US-region tables, use the Python BigQuery client with `SELECT` + `load_table_from_json()` pattern — NOT `INSERT INTO ... SELECT FROM`.

### 2. CORS Format
The deploy.yml passes CORS origins as a build arg. The format **must be a comma-separated string of full URLs with protocol**. Example: `https://cip-frontend-staging-807520113440.northamerica-northeast1.run.app,http://localhost:3000`. No trailing slashes, no spaces.

### 3. Port Mapping
Cloud Run expects the container to listen on `$PORT` (default 8080). The Dockerfile must expose the correct port. The backend uses uvicorn — make sure the CMD uses `--port $PORT`.

### 4. IAP Config
IAP is configured on Cloud Run. API requests from the frontend need to include the IAP audience token. Backend-to-backend calls (like from Cloud Scheduler) need OIDC tokens.

### 5. Service Account
`cip-sheets-reader@point-blank-ada.iam.gserviceaccount.com` — has BigQuery Data Editor + Job User roles. Media plan Google Sheets must be shared with this email as Viewer.

### 6. Media Plan Sync Gotchas
- `_clear_existing_plan()` DELETEs all lines for a project before re-inserting — manual edits are lost
- Line IDs include a random UUID, so they change on every sync
- The `_tab_belongs_to_project()` filter has 3 checks: title code, client/project metadata, budget ratio (0.3x–3.0x)
- Em dashes (—) are used throughout media_plan_sync.py log messages. Preserve them.

## Section 5: Key Conventions

- **Project codes:** YYNNN format (e.g., 25013, 26009). Primary key linking campaigns across all systems.
- **Campaign names:** Platform campaign names embed the project code, usually at the start
- **Project code regex:** `r'(?:^|\b)(2[0-9]\d{3})(?:\b|\s|-|_|$)'`
- **Platform identification:** Each row in funnel_data belongs to exactly one platform

## Section 6: Repository Structure

```
/
├── CLAUDE.md
├── infrastructure/
│   ├── bigquery/           # DDL scripts for BigQuery tables
│   └── setup/              # GCP setup scripts
├── ingestion/
│   └── transformation/     # Phase 1: Funnel.io → normalized tables
├── backend/
│   ├── main.py             # FastAPI entry point
│   ├── config.py           # Configuration
│   ├── routers/
│   │   ├── admin.py        # Admin endpoints (sync, projects)
│   │   ├── pacing.py       # Pacing engine endpoints
│   │   └── ...
│   ├── services/
│   │   ├── media_plan_sync.py  # ★ Media plan Google Sheets → BigQuery
│   │   ├── bigquery_client.py  # BQ helper (scalar_param, string_param, date_param)
│   │   ├── pacing.py           # Pacing calculations
│   │   └── ...
│   └── models/             # Pydantic models
├── frontend/
│   ├── src/
│   │   ├── app/            # Next.js app router
│   │   ├── components/     # Shared UI components
│   │   └── lib/
│   │       └── api.ts      # API client + TypeScript types
│   └── package.json
├── requirements.txt
├── Dockerfile
└── .github/workflows/deploy.yml  # CI/CD → Cloud Run
```

## Section 7: Asana Integration

All bugs and features are tracked in Asana. When working on a task, update the Asana ticket with your progress.

- **Project:** ADA Campaign Intelligence Platform
- **Project GID:** `1215988273595218`
- **URL:** https://app.asana.com/1/9281551468324/project/1215988273595218/list
- **Custom fields:** Priority `1215988107013686` (High/Medium/Low) · Status `1215988107013691` (Not started/In progress/Completed) · Stage `1215988107013696` (Backlog/Planning/Execution/Launch/Complete In Production) · Ready For `1216308984626884` (🤖 Agent `1216308984626886` / 👨🏻‍💻 Frazer `1216308984626885`)
- **Sections:** Phase 0: Backlog `1215988107013754` · Phase 1: Planning `1215988273595220` · Phase 2: Execution `1215988107013672` · Phase 3: Ready In Staging `1215988107013673` · Phase 4: Complete In Production `1215988331209931`
- **Ticket-resolver skill:** `.claude/skills/ada-resolve-ticket/` drains the "Ready For: 🤖 Agent" queue on this board (one ticket per session; `config.json` holds the GIDs above).
- **Retired board:** `1213881933598770` ("CIP — Campaign Intelligence Platform") is defunct as of 2026-06 — its ticket/field/section GIDs (including the dated lists below) no longer resolve; kept only as historical context.

### Open Tickets (as of 2026-04-20 — retired board `1213881933598770`, GIDs no longer valid)

**Feature backlog:**
- `1213988918905284` — [FEATURE] Interactive tab confirmation UI for media plan sync — two-step preview/confirm flow
- `1213891599887233` — [FEATURE] Blurred creative underlay for campaign UI
- `1213891489614027` — [FEATURE] Auto-display client logo in campaign UI

**Diagnostic engine follow-on (deferred / calibration):**
- `1214050846233692` — [Diag] Quality signals Q1-Q3 — **deferred** pending CRM disposition data
- `1214047462724891` — [Diag] Phase 0 validation across all active projects — partial (25042 verified); full sweep pending
- `1214044687660071` — [Diag] FFS wizard in project settings
- `1214038918477523` — [Diag] Historical backfill of completed campaigns for calibration corpus

**Benchmarks (blocked on creative metadata audit):**
- `1213929922989571` — Investigate creative duration/format metadata in platform data (prerequisite)
- `1213917492001776` — Client-level historical benchmarks
- `1213917562834224` — Cross-client internal benchmarks

### Recently Completed (selected)
- All media plan sync bugs (tab filter, matching, overwrite, dedup, flight dates) — FIXED
- All pacing bugs (grace period, flight-date spend attribution, NULL line_status) — FIXED
- GA4 URL selection, Google Drive sharing, recently ended section, objective KPIs — SHIPPED
- OSSTF re-sync, Quantcast cleanup, inline edit persistence — RESOLVED
- Frontend type drift, scalar_param, creative alias NULL — FIXED

## Section 8: Testing Endpoints

Useful curl commands for verification:

```bash
# Trigger media plan sync
curl -X POST "https://cip-backend-staging-807520113440.northamerica-northeast1.run.app/api/admin/sync-media-plan?sheet_id=1uLg2KdgNrDH6MBhXsavfXpFOleCzCrVYdft1AA8jtuU&project_code=25042"

# Get pacing data
curl "https://cip-backend-staging-807520113440.northamerica-northeast1.run.app/api/pacing/25042"

# Get pacing history (oscilloscope)
curl "https://cip-backend-staging-807520113440.northamerica-northeast1.run.app/api/pacing/25042/history?days=30"

# Create creative alias
curl -X POST "https://cip-backend-staging-807520113440.northamerica-northeast1.run.app/api/admin/creative-aliases" -H "Content-Type: application/json" -d '{"project_code":"25042","original_name":"test","alias":"Test Alias"}'
```

### BigQuery Verification Queries

```sql
-- Check media plan lines for a project
SELECT platform_id, audience_name, budget, line_code
FROM `point-blank-ada.cip.media_plan_lines`
WHERE plan_id LIKE 'plan-25042%'
ORDER BY platform_id, budget DESC;

-- Check total budget matches expected
SELECT SUM(budget) as total_budget, COUNT(*) as line_count
FROM `point-blank-ada.cip.media_plan_lines`
WHERE plan_id LIKE 'plan-25042%';
-- Expected: ~$63,750 total for 25042

-- Verify no Quantcast contamination
SELECT * FROM `point-blank-ada.cip.media_plan_lines`
WHERE plan_id LIKE 'plan-25042%' AND audience_name LIKE '%Quantcast%';
-- Expected: 0 rows
```

## Section 9: Data Sources

Raw Funnel.io passthrough data lives in `point-blank-ada.core_funnel_export.funnel_data` (US region). This table has 1,463 columns with platform-specific suffixes and ~800K rows from Oct 2023 to present. The transformation layer reads from this table cross-region and writes to normalized tables in the `cip` dataset.

### Reach/Frequency source of truth
> **Reach/Frequency source of truth.** Spend, impressions, and clicks always come from Funnel (`fact_digital_daily` / `fact_adset_daily`). Reach and frequency come from Funnel's 7-day platform fields for every platform EXCEPT StackAdapt. For StackAdapt, reach and frequency come from the direct StackAdapt `reachFrequency` API feed in `cip_stackadapt.stackadapt_reach_frequency` (joined on `Campaign_ID__StackAdapt`), because Funnel's `Unique_impressions_1_Day_Creative__StackAdapt` is a 1-day per-creative field that overcounts true dedup reach by 7–10×. StackAdapt reports dedup reach only in fixed calendar buckets (daily/weekly/monthly); the campaign headline is the **current calendar-month** bucket, never a summed flight-to-date figure (reach is non-additive). Reach is stored and shown as individual and household ("residential") separately (never collapsed); household data exists only from 2026-06-03 onward. Never reintroduce the Funnel StackAdapt reach/frequency columns into any user-facing aggregate.

Read-path implementation (Stage 2, ADA 1215990005858637): `backend/routers/performance.py` keeps the Funnel-side SQL R&F exclusion (`RF_EXCLUDED_PLATFORMS = {"stackadapt"}` stays) and adds a Python-side FILL layer (`_stackadapt_direct_rf`) that supplies the SA numbers from the direct feed; `backend/services/diagnostics/engine.py` overrides the StackAdapt `adset_bucket` entries with the current-month feed so D1/D2/D3/D4 score SA-only campaigns on real dedup reach.

## Section 10: Alert Thresholds (Defaults)

- **Pacing Over Warning:** >115%
- **Pacing Critical Over:** >130%
- **Pacing Under Warning:** <85%
- **Pacing Critical Under:** <70%
- **Budget Exceeded:** actual > planned (critical)
- **Flight Ending Soon:** <7 days remaining + >15% unspent (info)
- **Data Stale:** no data >36 hours (warning)
