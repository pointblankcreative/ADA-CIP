# Campaign Intelligence Platform (CIP)

## Project Overview
Custom-built platform for Point Blank Creative Inc. replacing Funnel.io and Looker. Centralises campaign monitoring, budget pacing, automated reporting, and client-facing dashboards for a political advertising agency running 5-15 concurrent campaigns across Meta, Google Ads, LinkedIn, StackAdapt, TikTok, Snapchat, and Perion/Hivestack (DOOH).

## Architecture
- **Frontend**: Next.js 14+ with React, TypeScript, Tailwind CSS, Recharts
- **Backend API**: FastAPI (Python 3.11+), Pydantic v2, Uvicorn
- **Data Warehouse**: Google BigQuery (dataset: `cip` in project `point-blank-ada`, region `northamerica-northeast1`)
- **App Database**: Cloud SQL PostgreSQL 15+ (for app state: users, sessions, settings)
- **Auth**: Firebase Auth (Google Workspace SSO for internal, magic-link for clients)
- **Task Orchestration**: Google Cloud Scheduler + Cloud Functions
- **Hosting**: Google Cloud Run (containers)
- **Storage**: Google Cloud Storage
- **Region**: northamerica-northeast1 (Montreal) — Canadian data residency

## Data Sources
Raw Funnel.io passthrough data lives in `point-blank-ada.core_funnel_export.funnel_data` (US region). This table has 1,463 columns with platform-specific suffixes and ~800K rows from Oct 2023 to present. The transformation layer reads from this table cross-region and writes to normalized tables in the `cip` dataset.

## Key Conventions
- **Project codes**: YYNNN format (e.g., 25013, 26009). This is the primary key linking campaigns across all systems.
- **Campaign names**: Platform campaign names embed the project code, usually at the start (e.g., "25013 - BCGEU Bargaining Escalation - Conversion")
- **Project code regex**: `r'(?:^|\b)(2[0-9]\d{3})(?:\b|\s|-|_)'` — extract first 5-digit code starting with 20-30
- **Platform identification**: Each row in funnel_data belongs to exactly one platform, identified by which platform-specific columns are non-null

## Repository Structure
```
/
├── CLAUDE.md
├── docs/
│   └── specs/                    # Implementation specs (from CIP_Unified_Spec)
├── infrastructure/
│   ├── bigquery/                 # DDL scripts for BigQuery tables
│   ├── setup/                    # GCP setup scripts
│   └── terraform/                # Optional IaC
├── ingestion/
│   ├── transformation/           # Phase 1: Funnel.io → normalized tables
│   └── connectors/               # Phase 2: Direct API connectors
├── backend/
│   ├── main.py                   # FastAPI entry point
│   ├── routers/                  # API route modules
│   ├── services/                 # Business logic
│   │   ├── pacing.py             # Pacing engine
│   │   ├── alerts.py             # Alert generation
│   │   └── slack.py              # Slack integration
│   ├── models/                   # Pydantic models
│   └── config.py                 # Configuration
├── frontend/
│   ├── src/
│   │   ├── app/                  # Next.js app router
│   │   ├── components/           # Shared UI components
│   │   └── lib/                  # Utilities, API client
│   └── package.json
├── requirements.txt
├── Dockerfile
└── docker-compose.yml
```

## Development Commands
```bash
# Backend
cd backend && uvicorn main:app --reload --port 8000

# Frontend
cd frontend && npm run dev

# Run transformation
python -m ingestion.transformation.run

# Run tests
pytest tests/
```

## Important Design Decisions
1. **Frontend-first phasing**: Build app layer on existing Funnel.io data first, replace connectors later
2. **Cross-region queries**: Transformation reads from US-region funnel_data, writes to Montreal-region cip dataset
3. **Idempotent pipelines**: All transformations use MERGE (upsert) by composite key, never blind append
4. **Raw-first storage**: Preserve full API response fidelity in raw tables before normalization
5. **Project-centric schema**: Every record links to a project_code (YYNNN). This is how the agency thinks about campaigns.
6. **Pacing as core value**: The pacing engine is the #1 business-critical feature. It compares actual spend vs. planned budget from media plans, accounting for blocking chart (which weeks are active).
7. **Even pacing baseline**: planned_spend_to_date = (budget / total_active_days) × elapsed_active_days. Alert if >115% (warning) or >130% (critical) overspend, or <85% (warning) or <70% (critical) underspend.

## Platform Column Mapping Reference
The funnel_data table uses platform-suffixed columns. Key mappings for the transformation layer:

### Meta (Facebook_Ads)
- spend: `Amount_Spent__Facebook_Ads`
- impressions: `Impressions__Facebook_Ads`
- clicks: `Link_Clicks__Facebook_Ads` (use link clicks, not all clicks)
- campaign_name: `Campaign_Name__Facebook_Ads`
- campaign_id: `Campaign_ID__Facebook_Ads`
- ad_set_name: `Ad_Set_Name__Facebook_Ads`
- ad_set_id: `Ad_Set_ID__Facebook_Ads`
- ad_name: `Ad_Name__Facebook_Ads`
- ad_id: `Ad_ID__Facebook_Ads`
- account_id: `Ad_Account_ID__Facebook_Ads`
- reach_7d: `Reach___7_Day_Ad_Set__Facebook_Ads`
- frequency_7d: `Frequency___7_Day_Ad_Set__Facebook_Ads`
- video_plays: `Video_Plays__Facebook_Ads`
- video_thruplay: `Video_thruplay__Facebook_Ads`
- video_25: `Video_Watches_at_25__Facebook_Ads`
- video_50: `Video_Watches_at_50__Facebook_Ads`
- video_75: `Video_Watches_at_75__Facebook_Ads`
- video_95: `Video_Watches_at_95__Facebook_Ads`
- video_100: `Video_Watches_at_100__Facebook_Ads`
- conversions: `Campaign_Result_value__Facebook_Ads` (or sum of specific conversion types like Leads__Facebook_Ads)
- clicks_all: `Clicks_all__Facebook_Ads`

### Google Ads
- spend: `Cost__Google_Ads`
- impressions: `Impressions__Google_Ads`
- clicks: `Clicks__Google_Ads`
- campaign_name: `Campaign__Google_Ads`
- campaign_id: `Campaign_ID__Google_Ads`
- ad_set_name: `Ad_Group_Name__Google_Ads` (ad groups = ad sets)
- ad_set_id: `Ad_Group_ID__Google_Ads`
- ad_name: `Ad_Name__Google_Ads`
- ad_id: `Ad_ID__Google_Ads`
- account_id: `Ad_Account_Customer_ID__Google_Ads`
- conversions: `Conversions__Google_Ads`
- video_views: `Video_views__Google_Ads`
- engagements: `Engagements__Google_Ads`

### StackAdapt
- spend: `Cost__StackAdapt`
- impressions: `Impressions__StackAdapt`
- clicks: `Clicks__StackAdapt`
- campaign_name: `Campaign__StackAdapt`
- campaign_id: `Campaign_ID__StackAdapt`
- conversions: `Conversions__StackAdapt`
- video_started: `Video_started__StackAdapt`
- video_25: `Video_completed_25__StackAdapt`
- video_50: `Video_completed_50__StackAdapt`
- video_75: `Video_completed_75__StackAdapt`
- video_95: `Video_completed_95__StackAdapt`

### TikTok
- spend: `Total_cost__TikTok`
- impressions: `Impressions__TikTok`
- clicks: `Clicks_Destination__TikTok` (destination clicks preferred over all clicks)
- campaign_name: `Campaign_name__TikTok`
- campaign_id: `Campaign_ID__TikTok`
- ad_set_name: `Adgroup_name__TikTok` (adgroups = ad sets)
- ad_set_id: `Adgroup_ID__TikTok`
- ad_name: `Ad_name__TikTok`
- ad_id: `Ad_ID__TikTok`
- account_id: `Advertiser_ID__TikTok`
- conversions: `Conversions__TikTok`
- reach_7d: `Reach___7_Day_Adgroup__TikTok`
- frequency_7d: `Frequency___7_Day_Adgroup__TikTok`

### Snapchat
- spend: `Spend__Snapchat`
- impressions: `Impressions__Snapchat` (or `Paid_impressions__Snapchat`)
- clicks: `Swipes__Snapchat` (swipes = clicks on Snapchat)
- campaign_name: `Campaign_name__Snapchat`
- campaign_id: `Campaign_ID__Snapchat`
- ad_set_name: `Ad_Squad_Name__Snapchat` (ad squads = ad sets)
- ad_set_id: `Ad_Squad_ID__Snapchat`
- ad_name: `Ad_Name__Snapchat`
- ad_id: `Ad_ID__Snapchat`
- conversions: Look for conversion columns with __Snapchat suffix
- reach_7d: `Reach___7_Day_Campaign__Snapchat`
- frequency_7d: `Frequency___7_Day_Campaign__Snapchat`
- video_views: `Video_Views_time_based__Snapchat`

### LinkedIn
- spend: `Spend__LinkedIn`
- impressions: `Impressions__LinkedIn`
- clicks: `Clicks__LinkedIn`
- campaign_name: `Campaign__LinkedIn`
- campaign_id: `Campaign_ID__LinkedIn`
- campaign_group: `Campaign_Group__LinkedIn`
- campaign_group_id: `Campaign_Group_ID__LinkedIn`
- conversions: `Conversions__LinkedIn`
- action_clicks: `Action_Clicks__LinkedIn`
- landing_page_clicks: `Landing_Page_Clicks__LinkedIn`

## Alert Thresholds (Defaults)
- Pacing Over Warning: >115%
- Pacing Critical Over: >130%
- Pacing Under Warning: <85%
- Pacing Critical Under: <70%
- Budget Exceeded: actual > planned (critical)
- Flight Ending Soon: <7 days remaining + >15% unspent (info)
- Data Stale: no data >36 hours (warning)

## Testing
- Always validate spend totals against platform UIs — 1% tolerance
- Test pacing calculations against manual spreadsheet calculations
- Project code extraction must handle edge cases (codes embedded mid-string, codes with hyphens/underscores)

## Media Plan Spreadsheet Structure
Each campaign has a Google Sheets media plan. The project code is embedded in the sheet title (e.g., "26009 CUPE OMERS Media Plan"). The Sheet ID and relevant GIDs are stored in the `media_plans` BigQuery table. Two tabs are critical:

### Blocking Chart Tab (GID varies per sheet)
Header rows 1-9 contain metadata:
- Row 2: `Client` (col B) → value (col D)
- Row 3: `Project` (col B) → value (col D) — this is the project/campaign name, NOT the project code
- Row 4: `Start & End Dates` (col B) → start date (col C), end date (col D)
- Row 6: `Run Length` → days (col C), weeks (col D)
- Row 8: `Net Budget` → currency (col C), amount (col E)

Data table starts at row 11 (headers) with line items starting at row 12:
- Col A: `Platform` — e.g., "Open Internet", "Meta (Facebook, Instagram, Threads)", "LinkedIn"
- Col B: `Objective + Format` — e.g., "Awareness Display Banner Ads", "Conversion Social Ads"
- Cols C-onwards: Week-beginning date columns (e.g., "Feb 23", "Mar 2", "Mar 9") — cells contain start/end dates if the line is active that week, colour-coded green (active) or red (ending)
- Second-to-last data col: `Budget` — line item budget (CAD)
- Last data col: `Objective %` — percentage of total budget
- Final col: `Notes`

**How to map to CIP tables:**
- Each row → one `media_plan_lines` record
- Week columns with dates → `blocking_chart_weeks` records (is_active = TRUE for that line+week)
- Platform names need normalising: "Open Internet" → "stackadapt" (or "perion"), "Meta (Facebook, Instagram, Threads)" → "meta", "LinkedIn" → "linkedin"

### Media Plan Tab (GID varies per sheet)
Header rows 1-10 contain the same metadata as Blocking Chart (Client, Project, Timeframe, Run Length, Net Budget).

Data table starts at row 12 (headers) with line items starting at row 14:
- Col A: `Site/Network` — platform name (may span multiple rows via merge cells)
- Col B: `Goal` — e.g., "Reach & Frequency", "Conversions", "Engagement"
- Col C: `Start` — line item start date
- Col D: `End` — line item end date
- Col E: `Days` — number of active days
- Col F: `ID` — internal line ID (e.g., "2a", "2b")
- Col G: `Audience Name` — targeting audience descriptor
- Col H: `Geo targeting` — geographic targeting
- Col I-J: `Audience Targeting` — detailed targeting description
- Col K: `Technical Targeting` — inventory/ISP/package details
- Col L: `Landing Page` — vanity + actual URL
- Col M: `Creative` — required ad sizes/formats
- Col N: `Pricing` — pricing model (CPM, CPC, etc.)
- Col O: `Est. Audience Size`
- Col P: `Bid/Estimation` — bid amount
- Col Q: `Est. Impressions`
- Col R: `Goal Weekly Freq` — frequency cap
- Col S: `Budget` — line item budget (CAD)

**Key parsing notes:**
- Platform names in col A may be merged across multiple rows (multiple line items per platform)
- The Blocking Chart tab is more useful for pacing (it has week-by-week active dates)
- The Media Plan tab is more useful for detailed targeting and creative specs
- Both tabs share the same header metadata (client, project, dates, budget)
- The spreadsheet ID for the example plan is: `1eAxbCs8GBYQXYCREq_YFIpm98cZpLmW-vsINlo9u_5M`

## GCP APIs Enabled
The following APIs are enabled on `point-blank-ada`:
- BigQuery API
- Cloud SQL Admin API
- Cloud Storage API
- Cloud Run Admin API
- Cloud Functions API
- Cloud Scheduler API
- Secret Manager API
- Google Sheets API
- Cloud Logging API
- Cloud Monitoring API
- Firebase Auth API
- Cloud Build API

## Service Account: CIP Sheets Reader
- **Email:** `cip-sheets-reader@point-blank-ada.iam.gserviceaccount.com`
- **Roles:** BigQuery Data Editor, BigQuery Job User
- **Key file:** `infrastructure/secrets/cip-sheets-reader.json` (gitignored — do NOT commit)
- **Purpose:** Reads media plan Google Sheets via Sheets API, writes parsed data to BigQuery CIP dataset
- **Shared spreadsheets:** The media plan spreadsheet (ID: `1eAxbCs8GBYQXYCREq_YFIpm98cZpLmW-vsINlo9u_5M`) is shared with this service account as Viewer
- **Auth pattern:** Use `gspread` with `service_account_from_dict()` or `service_account()` pointing to the key file. For BigQuery, use `google.cloud.bigquery.Client.from_service_account_json()`
- **Environment variable:** Set `GOOGLE_APPLICATION_CREDENTIALS=infrastructure/secrets/cip-sheets-reader.json` or load the key path from `.env`

## Phase 1 Deployment: IAP + Cloud Run
- **Auth strategy**: No in-app auth in Phase 1. Use Google Cloud Identity-Aware Proxy (IAP) on the Cloud Run service to restrict access to `@pointblankcreative.ca` Google Workspace accounts.
- **Phase 2**: Replace IAP-only with Firebase Auth for in-app role-based access (needed for client-facing dashboards with granular permissions).
- **IAP setup**: Enable IAP on the Cloud Run service, configure OAuth consent screen, add Workspace domain as allowed users. Single `gcloud` command once the service is deployed.

## SPRINT TASK LIST — Work through these in order. Do not stop between tasks unless something breaks.

Tasks 1-9 are complete. The frontend, backend, pipeline, pacing engine, and media plan sync are all built and tested locally. Work through Tasks 11-14 sequentially without pausing.

---

### Task 11: Slack Alert Integration
**The #1 reason the team wants CIP.** Replace the stub at `POST /api/alerts/dispatch` with real Slack delivery.

#### Backend: `backend/services/slack_alerts.py`
- Use the Slack Web API (`slack_sdk` Python package) with a bot token stored in env var `SLACK_BOT_TOKEN`
- For each unsent alert (where `slack_sent = FALSE`):
  1. Look up the project's `slack_channel_id` from `dim_projects`
  2. Format the alert as a Slack Block Kit message:
     - **Critical**: Red sidebar, bold title, budget/pacing numbers, link to CIP dashboard
     - **Warning**: Yellow sidebar, same structure
     - **Info**: Blue sidebar, lighter formatting
  3. Post to the channel via `chat.postMessage`
  4. Update the alert row: `slack_sent = TRUE`, `slack_channel_id = <channel>`
- If a project has no `slack_channel_id`, post to a default `#cip-alerts` channel
- Add a daily digest function: morning summary of all active alerts across projects, posted to `#cip-alerts`

#### Wire into daily job
- In `backend/services/daily_job.py`, add a 4th stage after staleness: dispatch unsent alerts to Slack
- Stage order: transform → pacing → staleness → slack dispatch

#### Update the dispatch endpoint
- `POST /api/alerts/dispatch` should call the real dispatch function now, not return a stub

#### Environment
- `SLACK_BOT_TOKEN` — will be provided at deploy time
- `SLACK_DEFAULT_CHANNEL` — fallback channel ID (default: `#cip-alerts`)

---

### Task 12: Admin UI — Project Onboarding
The team needs a web interface to create projects, link media plans, and assign Slack channels. No more CLI scripts.

#### New pages

**`/admin` — Admin dashboard**
- Link in sidebar (below Alerts)
- Cards/links to: New Project, Manage Projects, Trigger Pipeline

**`/admin/projects/new` — Create Project form**
- Fields: Project Code (text, YYNNN format with validation), Client (dropdown from `GET /api/projects/` unique clients, or type new), Project Name, Start Date, End Date, Net Budget, Media Plan Sheet URL (optional — extract sheet ID from URL), Slack Channel (text input for channel name or ID)
- On submit: `POST /api/admin/projects` (new endpoint, see below)
- Show progress indicator as backend runs the setup pipeline
- On success: redirect to the new project detail page

**`/admin/projects` — Project list/management**
- Table of all projects with: code, name, client, status, budget, sheet linked (yes/no), Slack channel
- Edit button per row → inline edit or modal for: budget, dates, status (active/paused/completed), Slack channel, Sheet URL
- "Re-sync media plan" button per project → `POST /api/admin/sync-media-plan?project_code=X`
- "Re-run pacing" button per project → `POST /api/pacing/{code}/run`

**`/admin/pipeline` — Pipeline control**
- "Run Daily Pipeline" button → `POST /api/admin/daily-run`
- "Run Full History Backfill" button → `POST /api/admin/run-transformation` with full history flag
- Show last run time, status, row counts from `ingestion_log`
- Data freshness table (already available from `GET /api/admin/data-freshness`)

#### New backend endpoints

**`POST /api/admin/projects`** — Create a new project
- Accept JSON body: `{ project_code, client_name, project_name, start_date, end_date, net_budget, media_plan_sheet_url, slack_channel_id }`
- Upsert into `dim_clients` (create client if new) and `dim_projects`
- If `media_plan_sheet_url` provided, extract sheet ID and trigger media plan sync
- Return the created project

**`PUT /api/admin/projects/{code}`** — Update project settings
- Accept partial updates to: budget, dates, status, slack_channel_id, media_plan_sheet_id
- Upsert into `dim_projects`

**`GET /api/admin/projects`** — List all projects with admin fields (sheet ID, Slack channel, etc.)
- Richer than `GET /api/projects/` — includes config fields not shown on the public dashboard

---

### Task 13: Data Pipeline Hardening
Small but important fixes to catch edge cases.

#### campaign_project_mapping fallback
Update the transformation SQL (both `transform_funnel_to_unified.sql` and `transform_funnel_to_unified_full_history.sql`):
- After the existing COALESCE project_code extraction, add a LEFT JOIN to `campaign_project_mapping` as a final fallback
- In the `enriched_data` CTE, change the project_code line to:
```sql
COALESCE(
  REGEXP_EXTRACT(campaign_name, r'(?:^|_|\s|-)(2[0-9]\d{3})(?:_|\s|-|$)'),
  REGEXP_EXTRACT(ad_set_name, r'(?:^|_|\s|-)(2[0-9]\d{3})(?:_|\s|-|$)'),
  cpm.project_code
) AS project_code,
```
- Join `campaign_project_mapping cpm` on `platform_id = cpm.platform_id AND campaign_name LIKE cpm.campaign_name_pattern`

#### Seed the known mappings
```sql
INSERT INTO `point-blank-ada.cip.campaign_project_mapping` (platform_id, campaign_name_pattern, project_code, created_by)
VALUES
  ('stackadapt', 'OPSEU - LCBO Strike%', '23061', 'manual'),
  ('stackadapt', 'OPSEU LBED SoFundMe%', '23061', 'manual');
```

#### Alert deduplication
In `backend/services/pacing.py`, before inserting a new alert, check if an identical active alert already exists (same project_code, alert_type, severity, created in the last 24h). Skip if duplicate.

#### Run full history backfill
After the pipeline hardening changes, run:
```bash
python -m ingestion.transformation.run --full-history
```
Verify StackAdapt/LinkedIn project_code coverage:
```sql
SELECT platform_id, COUNT(*) as rows, COUNTIF(project_code IS NOT NULL) as with_code
FROM `point-blank-ada.cip.fact_digital_daily`
WHERE platform_id IN ('stackadapt', 'linkedin')
GROUP BY 1
```

---

### Task 14: Deploy to Cloud Run + IAP + Cloud Scheduler

#### 1. Secret Manager
- Store the service account key: `gcloud secrets create cip-sheets-reader-key --data-file=infrastructure/secrets/cip-sheets-reader.json`
- Store the Slack bot token: `gcloud secrets create cip-slack-bot-token --data-file=-` (pipe from env or prompt)

#### 2. Backend Container
- Dockerfile base: `python:3.11-slim`
- Mount secrets via Secret Manager (NOT baked into image)
- Env vars: `GOOGLE_CLOUD_PROJECT=point-blank-ada`, `SHEETS_SERVICE_ACCOUNT_FILE=/secrets/cip-sheets-reader.json`, `BIGQUERY_LOCATION=northamerica-northeast1`, `SLACK_BOT_TOKEN` from Secret Manager
- Build: `gcloud builds submit --tag gcr.io/point-blank-ada/cip-backend`
- Deploy: `gcloud run deploy cip-backend --image gcr.io/point-blank-ada/cip-backend --region northamerica-northeast1 --platform managed`
- Mount secrets: `gcloud run services update cip-backend --update-secrets=/secrets/cip-sheets-reader.json=cip-sheets-reader-key:latest,SLACK_BOT_TOKEN=cip-slack-bot-token:latest`

#### 3. Frontend Container
- Build Next.js with `NEXT_PUBLIC_API_URL` pointing to the backend Cloud Run URL
- Build: `gcloud builds submit --tag gcr.io/point-blank-ada/cip-frontend`
- Deploy: `gcloud run deploy cip-frontend --image gcr.io/point-blank-ada/cip-frontend --region northamerica-northeast1 --platform managed`

#### 4. IAP Setup
- Configure OAuth consent screen (internal, `pointblankcreative.ca` domain)
- Enable IAP on both Cloud Run services
- Add `@pointblankcreative.ca` domain as IAP-secured Web App User
- No in-app auth code needed — IAP handles everything at the infrastructure level

#### 5. Cloud Scheduler
- `gcloud scheduler jobs create http cip-daily-run --schedule="0 6 * * *" --time-zone="America/Toronto" --uri="<backend-cloud-run-url>/api/admin/daily-run" --http-method=POST --oidc-service-account-email=cip-sheets-reader@point-blank-ada.iam.gserviceaccount.com`
- Grant `roles/run.invoker` to the service account for Cloud Scheduler auth

#### 6. CORS
- Update FastAPI CORS middleware to include the frontend Cloud Run domain
- If using IAP, the IAP proxy domain also needs to be allowed

#### 7. Verify
- Hit `GET <backend-url>/api/projects/` — should return 8 projects
- Hit `GET <backend-url>/api/pacing/26009` — should return pacing data
- Frontend should load and display live data
- Trigger Cloud Scheduler manually and verify the daily job runs

---

### Completion Criteria
All four tasks are done when:
- Slack alerts are posting to channels when the daily job runs
- Team members can create new projects from the web UI at `/admin/projects/new`
- The app is accessible at a Cloud Run URL, locked behind IAP
- Cloud Scheduler fires at 6 AM ET and the pipeline runs autonomously

---

## HOTFIX: Duplicate Rows + MERGE Key Bug — RESOLVED (2026-03-30)

This has been fixed. Transform SQL now filters to ad-level source rows only, maps creative IDs, and uses COALESCE in MERGE key. Table was truncated and backfilled: 315K → 102K rows.

### Root cause — Funnel.io multi-source architecture

Funnel.io uses MULTIPLE data sources per platform per ad account to pull different types of metrics:
- **Ad-level sources** ("Ad | Actions, Conversions, Ad Creative, Video") — creative/ad-level granularity with spend, impressions, clicks, conversions
- **Non-aggregation sources** ("Non-Aggregation | 1 Day / 7 Day / 30 Day | Ad Set / Campaign") — campaign or ad-set-level reach and frequency metrics with different attribution windows
- **Standard sources** — additional metric exports

All sources dump into the same `core_funnel_export.funnel_data` table. For a given platform+campaign+date, there may be 2-4 rows from different sources, each with different columns populated and others NULL.

Per-platform source counts observed in Funnel.io:
- Facebook Ads: 4 sources (3x Non-Aggregation windows + 1x Ad-level)
- Snapchat: 2 sources (campaign reach/frequency + ad-level conversions)
- LinkedIn: 2 sources (campaign reach/frequency + standard)
- Others: similar multi-source patterns

The current transform treats every source row as a separate data point, which creates duplicates. The MERGE key (`date + platform_id + campaign_id + ad_set_id + ad_id`) can't differentiate them when `ad_id` is NULL (because some sources don't have creative-level data), and BigQuery MERGE treats `NULL = NULL` as FALSE, so every re-run inserts more duplicates.

Current duplication in `fact_digital_daily`:
- Meta: 239K rows, should be ~93K
- Google Ads: 194K rows, should be ~57K
- Snapchat: 88K rows, should be ~20K
- TikTok: 85K rows, should be ~43K
- StackAdapt: 16K rows, should be ~8K
- LinkedIn: 156 rows, should be ~46

### Fix — Step 1: Understand the source data per platform

Before changing the SQL, query the source table to understand what each platform's sources provide. For each platform, run:
```sql
SELECT
  Creative_ID__<Platform> IS NOT NULL AS has_creative,
  COUNT(*) AS rows,
  COUNTIF(Spend__<Platform> IS NOT NULL) AS rows_with_spend,
  COUNTIF(Impressions__<Platform> IS NOT NULL) AS rows_with_impressions,
  COUNTIF(Reach__<Platform> IS NOT NULL) AS rows_with_reach
FROM `point-blank-ada.core_funnel_export.funnel_data`
WHERE Campaign_ID__<Platform> IS NOT NULL
  AND Date >= '2026-03-01'
GROUP BY 1
```

This tells you which source has spend data (the one CIP should use) vs. which has reach/frequency (can be ignored for now, or handled separately later).

### Fix — Step 2: Filter to ad-level source rows only

In both `transform_funnel_to_unified.sql` and `transform_funnel_to_unified_full_history.sql`, each platform's section needs to filter to ONLY the ad-level source — the one that has `Spend` populated at the most granular level available.

For each platform, add the creative/ad ID mapping AND filter:

**LinkedIn:**
```sql
Creative_ID__LinkedIn AS ad_id,
Creative_Name__LinkedIn AS ad_name,
```
Add to WHERE: `AND Spend__LinkedIn IS NOT NULL`

**Meta (Facebook Ads):**
Verify which column is the ad/creative ID (likely `Ad_ID__Facebook` or `Creative_ID__Facebook`), map it to `ad_id`.
Add to WHERE: `AND Spend__Facebook IS NOT NULL` (this filters out the Non-Aggregation sources which have NULL spend)

**Snapchat, TikTok, StackAdapt, Google Ads:**
Same pattern — find the creative/ad ID column, map it, and filter to rows where spend is not null.

Use this query to find ad-level ID columns per platform:
```sql
SELECT column_name
FROM `point-blank-ada.core_funnel_export.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = 'funnel_data'
  AND (LOWER(column_name) LIKE '%ad_id%' OR LOWER(column_name) LIKE '%creative_id%' OR LOWER(column_name) LIKE '%ad_name%' OR LOWER(column_name) LIKE '%creative_name%')
ORDER BY column_name
```

### Fix — Step 3: COALESCE the MERGE key

Change the MERGE ON clause from:
```sql
ON target.date = source.date
  AND target.platform_id = source.platform_id
  AND target.campaign_id = source.campaign_id
  AND target.ad_set_id = source.ad_set_id
  AND target.ad_id = source.ad_id
```
To:
```sql
ON target.date = source.date
  AND target.platform_id = source.platform_id
  AND COALESCE(target.campaign_id, '') = COALESCE(source.campaign_id, '')
  AND COALESCE(target.ad_set_id, '') = COALESCE(source.ad_set_id, '')
  AND COALESCE(target.ad_id, '') = COALESCE(source.ad_id, '')
```

### Fix — Step 4: Truncate and re-run full history backfill

The cleanest approach given the scope of duplication:
```sql
TRUNCATE TABLE `point-blank-ada.cip.fact_digital_daily`
```
Then re-run the full history backfill with the fixed SQL:
```bash
python -m ingestion.transformation.run --full-history
```

### Fix — Step 5: Verify

```sql
-- Row counts should be dramatically lower and total_rows = unique_keys
SELECT platform_id, COUNT(*) as total_rows,
  COUNT(DISTINCT CONCAT(date, '|', COALESCE(campaign_id,''), '|', COALESCE(ad_set_id,''), '|', COALESCE(ad_id,''))) as unique_keys
FROM `point-blank-ada.cip.fact_digital_daily`
GROUP BY platform_id
```

total_rows MUST equal unique_keys for every platform. If not, there are still duplicate source rows slipping through.

Also cross-check spend totals against platform UIs for at least 2 projects.

### Currency investigation (after dedup fix)

LinkedIn spend in CIP may still not match the LinkedIn UI exactly. Funnel.io may pass through local currency (CAD) values. After the backfill, compare CIP totals against platform UIs. If they still don't match:
- Check: `SELECT column_name FROM core_funnel_export.INFORMATION_SCHEMA.COLUMNS WHERE table_name = 'funnel_data' AND LOWER(column_name) LIKE '%currency%'`
- If Funnel.io provides currency, add a `currency` column to `fact_digital_daily` and store the original currency alongside the spend value

---

## SPRINT TASK LIST — Work through these in order. Do not stop between tasks unless something breaks.

### Task 15: Add Reddit + Pinterest to Transform SQL

Add two new platform sections to both `transform_funnel_to_unified.sql` and `transform_funnel_to_unified_full_history.sql`. Follow the exact pattern of the existing platform sections. Use `AND Spend__<Platform> IS NOT NULL` to filter out reach/frequency-only source rows (same pattern used for all other platforms after the hotfix).

#### Reddit
```sql
UNION ALL

-- =========================================================================
-- REDDIT
-- =========================================================================
SELECT
  CAST(Date AS DATE) AS date,
  'reddit' AS platform_id,
  Campaign_ID__Reddit AS campaign_id,
  Campaign_Name__Reddit AS campaign_name,
  Ad_Group_ID__Reddit AS ad_set_id,
  Ad_Group_Name__Reddit AS ad_set_name,
  Ad_ID__Reddit AS ad_id,
  Ad_Name__Reddit AS ad_name,
  Account_ID__Reddit AS account_id,
  Account_Name__Reddit AS account_name,
  CAST(Cost__Reddit AS NUMERIC) AS spend,
  CAST(Impressions__Reddit AS INT64) AS impressions,
  CAST(Clicks__Reddit AS INT64) AS clicks,
  CAST(NULL AS INT64) AS reach,
  CAST(NULL AS FLOAT64) AS frequency,
  CAST(Video_Starts__Reddit AS INT64) AS video_views,
  CAST(Video_Watches_100__Reddit AS INT64) AS video_completions,
  CAST(Key_Conversion_Total_Count__Reddit AS NUMERIC) AS conversions,
  CAST(NULL AS INT64) AS engagements
FROM
  `point-blank-ada.core_funnel_export.funnel_data`
WHERE
  Date IS NOT NULL
  AND Campaign_ID__Reddit IS NOT NULL
  AND Campaign_Name__Reddit IS NOT NULL
  AND Ad_ID__Reddit IS NOT NULL
```

#### Pinterest
Pinterest uses `Pin_ID__Pinterest` as the ad-level identifier (not `Ad_ID`). It has no `Clicks__Pinterest` — use `Paid_Outbound_Clicks__Pinterest` instead. No dedicated `Ad_Name` column exists.

```sql
UNION ALL

-- =========================================================================
-- PINTEREST
-- =========================================================================
SELECT
  CAST(Date AS DATE) AS date,
  'pinterest' AS platform_id,
  Campaign_ID__Pinterest AS campaign_id,
  Campaign_Name__Pinterest AS campaign_name,
  Ad_Group_ID__Pinterest AS ad_set_id,
  Ad_Group_Name__Pinterest AS ad_set_name,
  Pin_ID__Pinterest AS ad_id,
  CAST(NULL AS STRING) AS ad_name,
  Advertiser_ID__Pinterest AS account_id,
  Advertiser_Name__Pinterest AS account_name,
  CAST(Spend__Pinterest AS NUMERIC) AS spend,
  CAST(Paid_impressions__Pinterest AS INT64) AS impressions,
  CAST(Paid_Outbound_Clicks__Pinterest AS INT64) AS clicks,
  CAST(NULL AS INT64) AS reach,
  CAST(NULL AS FLOAT64) AS frequency,
  CAST(Paid_video_views__Pinterest AS INT64) AS video_views,
  CAST(Paid_video_watched_at_100__Pinterest AS INT64) AS video_completions,
  CAST(Conversions__Pinterest AS NUMERIC) AS conversions,
  CAST(Paid_engagements__Pinterest AS INT64) AS engagements
FROM
  `point-blank-ada.core_funnel_export.funnel_data`
WHERE
  Date IS NOT NULL
  AND Campaign_ID__Pinterest IS NOT NULL
  AND Campaign_Name__Pinterest IS NOT NULL
  AND Pin_ID__Pinterest IS NOT NULL
```

After adding both platform sections to both SQL files, truncate and re-run full history backfill:
```bash
python -m ingestion.transformation.run --full-history
```

Verify:
```sql
SELECT platform_id, COUNT(*) as rows, SUM(spend) as total_spend
FROM `point-blank-ada.cip.fact_digital_daily`
WHERE platform_id IN ('reddit', 'pinterest')
GROUP BY platform_id
```

Expected: reddit ~633 rows with spend, pinterest ~165 rows with spend.

---

### Task 16: Update Cloud Scheduler to Twice Daily

The Funnel.io → BigQuery sync now runs at 2 AM and 2 PM PT. Update the CIP pipeline scheduler to run at 2:30 AM and 2:30 PM PT to pick up data shortly after each sync.

2:30 AM PT = 5:30 AM ET. 2:30 PM PT = 5:30 PM ET.

Update or replace the existing Cloud Scheduler job:
```bash
# Delete the old single daily job
gcloud scheduler jobs delete cip-daily-run \
  --project=point-blank-ada \
  --location=northamerica-northeast1 \
  --quiet

# Create morning run
gcloud scheduler jobs create http cip-morning-run \
  --project=point-blank-ada \
  --location=northamerica-northeast1 \
  --schedule="30 5 * * *" \
  --time-zone="America/Toronto" \
  --uri="https://cip-backend-807520113440.northamerica-northeast1.run.app/api/admin/daily-run" \
  --http-method=POST \
  --oidc-service-account-email=cip-sheets-reader@point-blank-ada.iam.gserviceaccount.com

# Create afternoon run
gcloud scheduler jobs create http cip-afternoon-run \
  --project=point-blank-ada \
  --location=northamerica-northeast1 \
  --schedule="30 17 * * *" \
  --time-zone="America/Toronto" \
  --uri="https://cip-backend-807520113440.northamerica-northeast1.run.app/api/admin/daily-run" \
  --http-method=POST \
  --oidc-service-account-email=cip-sheets-reader@point-blank-ada.iam.gserviceaccount.com
```

Also update `infrastructure/deploy.sh` to reflect the new schedule (replace the single scheduler section with two jobs).

---

### Task 17: Fix Deployment Configuration

Several issues were hit during the first manual deploy. Fix these so future deploys (manual or CI/CD) work cleanly:

1. **Backend Dockerfile** — Already correct (`PORT=8000`, uvicorn on 8000). But `deploy.sh` does not pass `--port=8000` to `gcloud run deploy`. Add `--port=8000` to the backend deploy command.

2. **Frontend Dockerfile** — Already correct (`PORT=3000`, node server.js). But `deploy.sh` does not pass `--port=3000` to the frontend deploy command. Add `--port=3000`.

3. **CORS format** — `deploy.sh` line 184 sets `CORS_ORIGINS=${FRONTEND_URL}` as a plain string, but `backend/config.py` has `cors_origins: list[str]` which Pydantic parses as JSON. Change to: `CORS_ORIGINS=["${FRONTEND_URL}"]`

4. **Frontend build-arg** — `deploy.sh` uses `--build-arg=NEXT_PUBLIC_API_URL=...` which `gcloud builds submit` doesn't support. Replace the frontend build command to use the `cloudbuild.yaml`:
```bash
gcloud builds submit \
  --project="${PROJECT_ID}" \
  --config=frontend/cloudbuild.yaml \
  --substitutions=_NEXT_PUBLIC_API_URL=${BACKEND_URL},_IMAGE_TAG=${FRONTEND_IMAGE} \
  ./frontend
```

5. **GitHub Actions workflow** — Same CORS issue exists in `.github/workflows/deploy.yml` line 151. Change `CORS_ORIGINS=${FRONTEND_URL}` to `CORS_ORIGINS=["${FRONTEND_URL}"]`. Also add `--port=8000` to the backend deploy step and `--port=3000` to the frontend deploy step.

---

### Task 18: Redeploy to Production

After Tasks 15-17, redeploy both backend and frontend to get the hotfix code, UI improvements, new platforms, and config fixes live.

**Backend:** Rebuild and deploy (the image needs to include the updated transform SQL):
```bash
gcloud builds submit --project=point-blank-ada --tag=gcr.io/point-blank-ada/cip-backend --timeout=600
gcloud run deploy cip-backend \
  --project=point-blank-ada \
  --image=gcr.io/point-blank-ada/cip-backend \
  --region=northamerica-northeast1 \
  --platform=managed \
  --port=8000 \
  --service-account=cip-sheets-reader@point-blank-ada.iam.gserviceaccount.com \
  --set-env-vars='GOOGLE_CLOUD_PROJECT=point-blank-ada,GCP_PROJECT_ID=point-blank-ada,GCP_REGION=northamerica-northeast1,BIGQUERY_DATASET=cip,SHEETS_SERVICE_ACCOUNT_FILE=/secrets/cip-sheets-reader.json,APP_ENV=production' \
  --update-secrets='/secrets/cip-sheets-reader.json=cip-sheets-reader-key:latest,SLACK_BOT_TOKEN=cip-slack-bot-token:latest' \
  --memory=1Gi --cpu=1 --timeout=300 --min-instances=0 --max-instances=5 \
  --allow-unauthenticated
```

**Frontend:** Rebuild with the backend URL and deploy:
```bash
gcloud builds submit \
  --project=point-blank-ada \
  --config=frontend/cloudbuild.yaml \
  --substitutions=_NEXT_PUBLIC_API_URL=https://cip-backend-807520113440.northamerica-northeast1.run.app,_IMAGE_TAG=gcr.io/point-blank-ada/cip-frontend \
  ./frontend
gcloud run deploy cip-frontend \
  --project=point-blank-ada \
  --image=gcr.io/point-blank-ada/cip-frontend \
  --region=northamerica-northeast1 \
  --platform=managed \
  --port=3000 \
  --set-env-vars=NODE_ENV=production \
  --memory=512Mi --cpu=1 --timeout=60 --min-instances=0 --max-instances=3 \
  --allow-unauthenticated
```

**Update CORS:**
```bash
gcloud run services update cip-backend \
  --project=point-blank-ada \
  --region=northamerica-northeast1 \
  --port=8000 \
  --update-env-vars='CORS_ORIGINS=["https://cip-frontend-807520113440.northamerica-northeast1.run.app"],FRONTEND_URL=https://cip-frontend-807520113440.northamerica-northeast1.run.app'
```

**Verify:**
- `curl https://cip-backend-807520113440.northamerica-northeast1.run.app/health` returns 200
- `curl https://cip-backend-807520113440.northamerica-northeast1.run.app/api/projects/` returns projects
- Frontend loads at `https://cip-frontend-807520113440.northamerica-northeast1.run.app`
- Reddit and Pinterest data visible in the dashboard

---

### Completion Criteria (Tasks 15-18)
- Reddit and Pinterest rows appear in `fact_digital_daily` with correct spend
- Cloud Scheduler has two jobs: 5:30 AM ET and 5:30 PM ET
- `deploy.sh` and GitHub Actions workflow have correct port, CORS, and build-arg config
- Both backend and frontend are redeployed with all fixes live

---

### Task 19: Bug Fixes — Overview, Project Detail, Performance, Campaign Filtering

Four bugs to fix. Work through all four before redeploying.

#### Bug 1: "No Data" badges on all campaign cards in overview

**Files:** `frontend/src/components/pacing-badge.tsx`, `frontend/src/app/page.tsx`

The PacingBadge shows "No Data" when `pacing_percentage` is null. This happens when the pacing engine hasn't run yet, OR when a project has data but no budget_tracking rows. Fix:
- When `pacing_percentage` is null but the project HAS spend data (`total_spend > 0`), show "Pacing Pending" (or similar) in a neutral colour instead of "No Data"
- When `pacing_percentage` is null AND `total_spend == 0`, show "No Data"
- When `pacing_percentage` is a number, show the actual pacing badge as designed

#### Bug 2: "Unknown Project" name in project detail view

**File:** `frontend/src/app/project/[code]/page.tsx`

The project name displays "Unknown Project" as fallback when `project_name` is null. For auto-discovered projects that haven't been provisioned via /admin, the name in `dim_projects` may not exist. Fix:
- Change the fallback from `"Unknown Project"` to `Project ${code}` so users at least see the project code
- If the project was auto-discovered (exists in fact_digital_daily but not in dim_projects), use the ad_set_name or campaign group name from the data as the display name

#### Bug 3: 7d/14d/30d/all date range controls in Performance tab don't work

**Files:** `backend/routers/performance.py`, `frontend/src/app/project/[code]/performance-tab.tsx`

The frontend sends `?days=7` (or 14, 30, 365) to the performance endpoint, but the backend only reads `start_date` and `end_date` query parameters. The `days` parameter is never converted into a date range, so all data is returned regardless of button selection. Fix in the backend:

```python
@router.get("/{project_code}")
async def get_performance(
    project_code: str,
    start_date: str | None = Query(None),
    end_date: str | None = Query(None),
    days: int | None = Query(None),
    platform: str | None = Query(None),
):
    if days and not start_date:
        from datetime import date as d, timedelta
        end_date = end_date or d.today().isoformat()
        start_date = (d.fromisoformat(end_date) - timedelta(days=days)).isoformat()
    # ... rest of existing function using start_date/end_date
```

#### Bug 4: Completed/ended campaigns still showing in Campaign Overview

**File:** `frontend/src/app/page.tsx`

The overview page calculates `activeProjects` (filtered by `status === "active"` and `days_remaining >= 0`) for the KPI summary, but the project card grid renders ALL projects. The grid should only show active projects. Fix:
- Change the project card map to use `activeProjects` instead of `projects`
- Add a "Show completed" toggle or a separate "Completed Campaigns" section below the active grid so users can still access historical projects if needed

**IMPORTANT context on BCGEU (25013):** This project has been inactive since December 2025 (Christmas). It should NOT appear in the active campaign grid. However, its `end_date` in `dim_projects` is currently `2026-03-31` and `status` is `active` — both are wrong. The end_date in the system may not always reflect reality (clients stop running ads before the booked end date).

**Backend fix (in `daily_job.py`, run before pacing):**
1. Auto-complete: `UPDATE dim_projects SET status = 'completed' WHERE end_date < CURRENT_DATE() AND status = 'active'` — catches any project whose booked end_date has passed
2. **DO NOT use stale/no-spend detection.** The previous 30-day stale detection logic was too aggressive — it incorrectly marked active campaigns as completed when they had pauses in spending. Only use end_date to auto-complete. If a campaign stops spending before its end_date, the team will manually mark it completed via the admin UI.
3. **If stale detection already exists in the code, REMOVE IT.** Only the end_date check should remain.

---

### Completion Criteria (Task 19)
- Campaign overview only shows active, in-flight campaigns by default
- A "Show completed" toggle or separate section exists for viewing historical campaigns
- Project detail page shows the project name (or project code as fallback), never "Unknown Project"
- Date range buttons in Performance tab filter the data correctly
- Pacing badges show meaningful status, not "No Data" when spend data exists
- Daily pipeline auto-marks projects as `completed` ONLY when end_date has passed (no stale/no-spend detection — too aggressive)
- Project statuses and end_dates are correct in dim_projects

---

### Task 20: Media Plan Sync Fixes

The media plan sync (`backend/services/media_plan_sync.py`) crashes on the Endo (25055) media plan. Three issues:

1. **Template/example tabs**: The sheet has a "Blocking Chart Example" tab that the parser picks up first (it matches `"blocking" in title and "chart" in title`). Fix: skip tabs with "example" or "template" in the name, or prefer tabs without those words.

2. **Multi-flight plans**: The sheet has "Media Plan Flight ONE" and "Media Plan Flight TWO" — two separate flight tabs. The parser only processes one `media_plan_ws` (whichever it finds last). Fix: collect ALL tabs matching "media plan" and merge their lines.

3. **Offset metadata rows**: The blocking chart has 2 empty rows before the metadata starts (row 3 = Client, row 4 = Project, row 5 = Run Dates). The parser likely expects metadata starting at row 1. Fix: scan for the metadata labels rather than assuming fixed row positions.

4. **Missing platforms in PLATFORM_MAP**: Pinterest and Reddit are not in the platform normalisation map. Add:
   ```python
   "pinterest": "pinterest",
   "reddit": "reddit",
   ```

5. **CORS dual-URL fix**: The deploy workflow (`deploy.yml`) has already been updated locally to include both Cloud Run URL formats in CORS. Commit this change.

6. **Stale detection removal**: If `_auto_complete_projects()` in `daily_job.py` contains the 30-day no-spend detection query, REMOVE it entirely. Only keep the `end_date < CURRENT_DATE()` check.

### Completion Criteria (Task 20)
- Media plan sync works for sheets with template/example tabs (skips them)
- Multi-flight media plans have all flight lines synced
- Pinterest and Reddit are in the PLATFORM_MAP
- deploy.yml includes both Cloud Run URL formats in CORS
- Auto-complete only uses end_date, no stale detection

---

## PHASE 2 — UI Enhancements (not started)

Phase 2 focuses on additional frontend/UI work. CI/CD is now live (completed in Phase 1).

### UI improvements (from initial user testing 2026-03-29)
- **Unprovisioned project prompt**: When a user clicks on an auto-discovered project (one that appeared from data but hasn't been formally provisioned via /admin/projects/new), the project detail page should show a prominent call-to-action prompting the user to set it up — e.g. "This campaign was detected automatically. Set up project details to enable pacing alerts and media plan tracking." with a button linking to the project creation form pre-filled with the project code.
- **Project configuration feedback**: The /admin/projects/new form submits but doesn't give clear feedback about what happened. Need success/error states, and the project detail page should reflect the configuration immediately.
- Additional UI items TBD as team starts using the tool.

### Phase 2 Task: GitHub Actions CI/CD

Replace the manual `deploy.sh` workflow with GitHub Actions:

**Environments:**
- **staging**: auto-deploys on push to `main`. Separate Cloud Run services (`cip-backend-staging`, `cip-frontend-staging`). Can optionally point at the same BigQuery dataset or a staging dataset.
- **production**: deploys on merge to `production` branch (or tagged release). The current Cloud Run services.

**Workflow file (`.github/workflows/deploy.yml`):**
- Trigger: push to `main` → build + deploy to staging. Push to `production` → build + deploy to prod.
- Steps: checkout → authenticate to GCP (Workload Identity Federation preferred over SA key) → build container → push to Artifact Registry → deploy to Cloud Run → health check
- Secrets: stored in GitHub Actions secrets (GCP project ID, Workload Identity provider, etc.)
- The existing `deploy.sh` is a good reference for the gcloud commands.

**IAP note:** Both staging and prod should be behind IAP. Staging can use the same OAuth consent screen.

---

## PHASE 3 — Custom API Connectors + Dynamic Transform (replacing Funnel.io)

Two tracks:

### Dynamic platform detection in the transform layer
Replace the hardcoded per-platform UNION ALL with an auto-detection approach:
1. Scan `INFORMATION_SCHEMA.COLUMNS` for `core_funnel_export.funnel_data` to find all platform suffixes (columns ending in `__PlatformName`)
2. For each detected platform, dynamically map `Spend__<Platform>` → `spend`, `Campaign_ID__<Platform>` → `campaign_id`, `Creative_ID__<Platform>` → `ad_id`, etc.
3. Filter to rows where spend is not null (solving the multi-source problem generically)
4. New platforms added in Funnel.io are picked up automatically on next transform run — zero code changes

This means if Frazer adds Reddit, Spotify, or any other platform in Funnel.io, CIP ingests it automatically.

### Direct API connectors
Replace Funnel.io entirely with direct API connectors for Meta, Google Ads, LinkedIn, StackAdapt, TikTok, Snapchat. This eliminates the $19,080 USD/year Funnel.io cost. Each connector authenticates directly with the platform API and writes to BigQuery.

Not started — will be scoped after Phase 2.
