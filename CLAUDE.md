# CLAUDE.md — ADA-CIP Project Context

## Section 1: Project Overview

Custom-built platform for Point Blank Creative Inc. replacing Funnel.io and Looker. Centralises campaign monitoring, budget pacing, automated reporting, and client-facing dashboards for a political advertising agency running 5-15 concurrent campaigns across Meta, Google Ads, LinkedIn, StackAdapt, TikTok, Snapchat, and Perion/Hivestack (DOOH).

## Section 2: Current Status (Updated 2026-07-05)

**Phase:** Full UI overhaul + a 24-ticket UAT round are **LIVE IN PRODUCTION** — `production` promoted to `2d3bef9` on 2026-07-05 (fast-forward, deploy run green: both health checks passed, IAM-lock + `--invoker-iam-check` steps succeeded). `main` (staging) and `production` are now in sync.

### Shipped 2026-07-03 → 07-05 (main → production, promoted 2026-07-05)
- **UAT round (PRs #97 + #98, 24 tickets):** diagnostics legibility (signal names lead, codes D3/A4 demoted to tooltips; a health-score legend that states it is *different from pacing*; a metric **glossary** primitive — `components/glossary.tsx` + `lib/glossary.ts` — with hover/focus popovers for pillars/cost-metrics/signals; honest "not reporting" copy via `guardCopy()` instead of raw tokens like `min_impressions_1000`); advisory engine voice (A5/F3 bodies observe, don't command — guarded by `tests/test_diagnostics_voice.py`); suggested moves carry an owning team (`SIGNAL_ACTIONS` → `SignalAction {action, owner, hedge?, platform?}`).
- **Alerts truthfulness (#2/#22):** honest feed empty/error states + live Act-now count; coverage-gated severity (`ALERT_LOW_COVERAGE_THRESHOLD = 0.10` downgrades thin-evidence ACTION alerts critical→warning); `ack_note` column added to `cip.alerts` (migration `infrastructure/bigquery/migrations/2026-07-03_alerts_ack_note.sql`, applied out-of-band — fixed a feed 500; staging+prod share the single `cip` dataset so it is already live for both).
- **Creative/Audiences (#5/#8/#10/#11/#17-23):** per-platform video 25/50/75/100 drop-off anchored at q25 (never `video_views`); one report-ready conversions figure + honest over-100% gloss; single "HOOK RATE" term + `n/a` static; unnamed-creative fallback keeps the raw token (backend `_alias_resolution` NULLIF); per-metric quartile direction cues.
- **Flightdeck/pacing (#15/#21/#24):** `is_direct` toggle gains confirm + one-shot Undo + honest re-pace tooltips; line codes behind a Show IDs toggle; Listen/audio mode removed (Signals + oscilloscope); "Unmapped Spend" de-escalated + collapsed by default; self-serve vs direct-buy budget split on the verdict hero.
- **Loading/intro (#12):** honest loading text; full-screen orbit-boot intro removed (`intro-provider.tsx` gutted to a no-op context shell; `orbit-intro.tsx` + `lib/viz/audio-engine.ts` now orphaned — safe to `git rm`).
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
- **pool_size/saturation null:** Meta refuses delivery estimates on ENDED ad sets; verify on the first live campaign (estimate circuit breaker stops after 6 consecutive refusals per run).
- **Post-promotion verify-after (2026-07-05):** confirm `cip-sheets-reader` still holds `roles/run.invoker` on prod `cip-backend` after the new `--invoker-iam-check` enforcement, so the twice-daily scheduler sync (2:30 AM/PM) doesn't 403 — deploy.yml does NOT manage that binding (only `infrastructure/deploy.sh` does). Also confirm `GET /api/alerts/` = 200 on prod (ack_note present — should already be true via the shared `cip` dataset).
- `performance-tab.tsx`, `orbit-intro.tsx`, `lib/viz/audio-engine.ts` retired but on disk (unimported) — `git rm` when comfortable.
- Deferred: audience CTR-trend sparkline slot, per-row matrix narrative reads, dark-mode toggle, ESLint setup (repo has no config; `tsc --noEmit` is the type gate).
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

- **Project:** CIP — Campaign Intelligence Platform
- **Project GID:** `1213881933598770`
- **Section for Brightwater work:** GID `1213878198552357` (but tickets currently in default section `1213881933598771`)
- **Build Phase custom field GID:** `1213906940579551`, Brightwater option GID: `1213906940579553`
- **URL:** https://app.asana.com/1/9281551468324/project/1213881933598770/list

### Open Tickets (as of 2026-04-20)

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

## Section 10: Alert Thresholds (Defaults)

- **Pacing Over Warning:** >115%
- **Pacing Critical Over:** >130%
- **Pacing Under Warning:** <85%
- **Pacing Critical Under:** <70%
- **Budget Exceeded:** actual > planned (critical)
- **Flight Ending Soon:** <7 days remaining + >15% unspent (info)
- **Data Stale:** no data >36 hours (warning)
