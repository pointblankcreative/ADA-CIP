# CLAUDE.md — ADA-CIP Project Context

## Section 1: Project Overview

Custom-built platform for Point Blank Creative Inc. replacing Funnel.io and Looker. Centralises campaign monitoring, budget pacing, automated reporting, and client-facing dashboards for a political advertising agency running 5-15 concurrent campaigns across Meta, Google Ads, LinkedIn, StackAdapt, TikTok, Snapchat, and Perion/Hivestack (DOOH).

## Section 2: Current Status (Updated 2026-04-16)

**Phase:** Phase 2 (Brightwater) — building out features to make CIP compelling for team adoption.

### Deployed to Staging (as of 2026-04-16)
- Core data pipeline: Funnel.io → BigQuery transformation for all 8 platforms
- GA4 sessions/conversions ingestion and URL management
- Admin panel: project management, pipeline controls, media plan sync
- Performance dashboard: daily metrics, budget pacing, platform breakdown, ad set & ad drill-downs
- Industry benchmarks (political advertising baselines)
- Reach/frequency ingestion from ad-set-level Funnel.io data
- Creative variant aliases with nullable platform_id
- Media plan sync: budget-based tab filtering, non-destructive matching, dedup fix, audience_name override persistence across re-syncs, per-line flight date parsing, retry on _delete_old_versions (3× with backoff)
- Pacing engine: per-line flight date spend attribution, 2-day grace period for new flights, completed flight UI treatment, stale line_id auto-purge on every pacing run
- All media_plan_lines queries protected with ROW_NUMBER dedup CTE (pacing router, pacing service, performance router, benchmarks router, diagnostic engine)
- Oscilloscope pacing health visualization with pending-line handling
- Overview page: recently ended section, budget bar uses utilization color
- Objective-based KPI classification (awareness/conversion/mixed) with reach/freq endpoint
- Cross-region BigQuery fix for adset transform
- Dual-URL CORS fix for Cloud Run new URL format
- Google Drive sharing instructions for media plan setup
- **Diagnostic Signal Engine — Persuasion:** Distribution (D1-D4) + Attention (A1-A5) + Resonance (R1-R3) all live. R2 guard-fails pending Phase 3 earned-impression connectors.
- **Diagnostic Signal Engine — Conversion:** Acquisition (C1-C3) + Funnel (F1-F5) live. Quality (Q1-Q3) **deferred** pending per-client CRM integration — weight redistributed to Acq 0.43 / Funnel 0.57. See `docs/diagnostics/quality-pillar-deferred.md`.
- **Diagnostic Signal Engine — Mixed campaigns:** Engine + queries + tests live on `feat/engine-mixed-campaigns` (Build Plan §12). Per-line classification, dual DiagnosticOutput, per-subset pacing. Frontend diagnostics tab still needs dual-health-card treatment before merge to main.

### What Needs Doing Next

**Diagnostic Engine (priority):**
1. **Wire into `daily_job.py`** + alert integration
2. **Frontend diagnostics tab** — must handle mixed campaigns (dual health cards) before `feat/engine-mixed-campaigns` can merge to main
3. **Phase 2.5 design note** — within-a-line ad-set arch mixing limitation (Arch A / Arch B classification is currently per-line, not per-ad-set)

**Future / Blocked on CRM:**
- **Quality pillar (Q1-Q3)** — deferred indefinitely pending per-client CRM disposition-data ingestion. See `docs/diagnostics/quality-pillar-deferred.md` for unblocking requirements and candidate signal definitions.

**Other Features (Asana backlog):**
4. **Interactive tab confirmation UI** for media plan sync (two-step preview/confirm flow)
5. **Blurred creative underlay** — campaign-specific visual backgrounds
6. **Auto-display client logo** in campaign UI

### Current Users
Only Frazer so far. Goal is to make it good enough that the whole team adopts it immediately on rollout.

### Branch Model
`main` → staging, `production` → production (both auto-deploy via GitHub Actions, ~7 min). Branches are in sync as of 2026-04-10. Create a new branch per PR, merge to main, then merge main → production when ready.

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

### Open Tickets (as of 2026-04-10)
- `1213988918905284` — [FEATURE] Interactive tab confirmation UI for media plan sync — two-step preview/confirm flow
- `1213891599887233` — [FEATURE] Blurred creative underlay for campaign UI
- `1213891489614027` — [FEATURE] Auto-display client logo in campaign UI

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
