# CIP Sprint Plan — Phase 1 Foundation (March 28, 2026)

> **STATUS: COMPLETED** — All tasks below were finished and deployed by April 4, 2026. This document is preserved for historical reference. For current project status, see [CLAUDE.md](/CLAUDE.md).

## Sprint Goal (Achieved ✓)
Deploy the foundation of the Campaign Intelligence Platform: BigQuery schema, data transformation from Funnel.io, pacing engine, backend API, Slack alerts, and frontend dashboard. By end of day, active campaigns should be visible with pacing status and alerts flowing to Slack.

## Prerequisites (Frazer to complete)
1. Ensure `point-blank-ada` GCP project has BigQuery, Cloud Run, Cloud SQL, Cloud Functions, Cloud Scheduler, Cloud Storage, Secret Manager APIs enabled
2. Create service account `cip-app@point-blank-ada.iam.gserviceaccount.com` with BigQuery Admin + Cloud SQL Client roles
3. Ensure Cursor has the ADA Primary Repository folder open as the workspace
4. Have a Slack bot token ready (or we create a Slack app during the sprint)
5. Have Google Sheets media plan URLs ready for 2-3 active campaigns (for testing media plan sync)

## Task Sequence for Cursor

### Task 1: Project Scaffolding (15 min)
Create the full repository structure inside ADA Primary Repository:
- Python backend (FastAPI) with requirements.txt, Dockerfile, docker-compose.yml
- Next.js frontend with package.json, tailwind config, TypeScript config
- Infrastructure folder (already has BigQuery DDL)
- Ingestion folder (already has transformation SQL)
- Tests folder structure
- .env.example with all needed environment variables
- .gitignore

**Cursor instruction:** "Set up the project scaffolding for the CIP platform following the structure in CLAUDE.md. Create a Python FastAPI backend in /backend, a Next.js 14 frontend in /frontend, and supporting infrastructure. Include requirements.txt, Dockerfile, docker-compose.yml, .env.example, and .gitignore. Do NOT install packages yet — just create the file structure and configuration files."

### Task 2: Execute BigQuery DDL (5 min — Frazer runs manually)
Frazer runs `infrastructure/bigquery/schema.sql` in the BigQuery console to create the dataset and all tables.

### Task 3: Backend Foundation (30 min)
Build the FastAPI backend with:
- main.py with CORS, lifespan, health check
- config.py reading from environment variables
- BigQuery client wrapper (read from cip dataset)
- Pydantic models for all API responses
- Auth middleware stub (accept all for now, add Firebase later)
- Router stubs for: /projects, /performance, /pacing, /alerts, /traditional

**Cursor instruction:** "Build the FastAPI backend foundation in /backend following CLAUDE.md. Create main.py, config.py, a BigQuery client service, Pydantic response models, and router stubs for projects, performance, pacing, alerts, and traditional endpoints. The BigQuery client should connect to project 'point-blank-ada' dataset 'cip'. Include a health check endpoint. Skip auth for now (add a TODO). Make sure all imports work and the server starts with `uvicorn main:app --reload`."

### Task 4: Transformation Layer (30 min)
Create the Python orchestrator that runs the transformation SQL:
- ingestion/transformation/run.py — reads the SQL file, executes against BigQuery
- Supports both daily (last 7 days) and full-history modes
- Logs results to ingestion_log table
- Can be triggered via API endpoint or CLI

**Cursor instruction:** "Create the Python transformation orchestrator at ingestion/transformation/run.py. It should: (1) read the SQL from transform_funnel_to_unified.sql, (2) execute it against BigQuery using the google-cloud-bigquery client, (3) support --mode=daily (default, last 7 days) and --mode=full (all history) via CLI arg, (4) log each run to the cip.ingestion_log table with row counts and status, (5) print a summary of rows processed per platform. Also add a /api/admin/run-transformation POST endpoint in the backend that triggers this. Reference the SQL files already in ingestion/transformation/."

### Task 5: Pacing Engine (45 min) ⭐ CRITICAL PATH
The pacing engine compares actual spend (from fact_digital_daily) against planned spend (from media_plan_lines + blocking_chart_weeks). This is the #1 business-critical feature.

Build in backend/services/pacing.py:
- Calculate even pacing: planned_spend_to_date = (budget / total_active_days) × elapsed_active_days
- Account for blocking chart: only count weeks marked is_active=true
- For each media plan line with a matching line_code or campaign mapping:
  - Sum actual spend from fact_digital_daily
  - Calculate pacing percentage = actual / planned * 100
  - Calculate remaining budget and daily spend required
  - Generate alerts if thresholds breached (>115% warning, >130% critical, <85% warning, <70% critical)
- Write results to budget_tracking table
- Write alerts to alerts table
- Return structured response for the dashboard

**Cursor instruction:** "Build the pacing engine in backend/services/pacing.py. This is the core business logic. It should: (1) Query media_plan_lines joined with blocking_chart_weeks to get planned budgets and active weeks per line, (2) Query fact_digital_daily to get actual cumulative spend per project/platform/campaign, (3) Calculate even pacing adjusted for blocking chart (only count active weeks), (4) For each line: compute pacing_percentage, remaining_budget, daily_budget_required, (5) Generate alerts based on thresholds (>115% pacing_over warning, >130% critical, <85% pacing_under warning, <70% critical, plus budget_exceeded if actual > total planned, flight_ending if <7 days and >15% unspent), (6) Write/update budget_tracking table, (7) Write new alerts to alerts table, (8) Create a /api/pacing/{project_code} GET endpoint and a /api/pacing/run POST endpoint to trigger calculation. Use BigQuery for all queries. This is the most important feature — get it right."

### Task 6: Media Plan Sync (30 min)
Build the Google Sheets sync to populate media_plans, media_plan_lines, and blocking_chart_weeks.

**Cursor instruction:** "Build the media plan sync service in backend/services/media_plan_sync.py. It should: (1) Accept a Google Sheets URL or sheet_id + project_code, (2) Use the Google Sheets API (gspread or google-api-python-client) to read the Media Plan and Blocking Chart tabs, (3) Parse metadata from header rows (client name, project name, dates, net budget), (4) Parse line items from the data table (site/network, flight dates, audience, budget, pricing, impressions), (5) Parse the blocking chart tab to determine which weeks each line is active, (6) Write to media_plans, media_plan_lines, and blocking_chart_weeks in BigQuery, (7) Track versions — only increment if content changed, (8) Create a /api/media-plans/{project_code}/sync POST endpoint. Handle column name variations (Site/Network vs Platform, Audience Name vs Audience, etc). See CLAUDE.md for conventions."

### Task 7: Slack Integration (30 min)
Build the Slack notification system for pacing alerts and daily summaries.

**Cursor instruction:** "Build the Slack integration in backend/services/slack.py. It should: (1) Use the Slack Web API (slack_sdk) to post messages, (2) Support Block Kit formatting for rich messages, (3) Have message templates for: pacing alerts (warning/critical), budget exceeded, daily summary (yesterday's spend + cumulative + pacing status per line), data freshness alerts, (4) Route messages to the correct channel based on project's slack_channel_id from dim_projects, (5) Fall back to a default channel (configurable) if no project channel mapped, (6) Update the alerts table with slack_sent=true and slack_message_ts after sending, (7) Create a /api/alerts/dispatch POST endpoint to send unsent alerts. The Slack bot token comes from environment variable SLACK_BOT_TOKEN."

### Task 8: Alert Dispatch Job (15 min)
Create the daily orchestration that runs: transformation → pacing calculation → alert dispatch.

**Cursor instruction:** "Create a daily job orchestrator at backend/services/daily_job.py. It should run the full daily pipeline in sequence: (1) Run transformation (daily mode — last 7 days), (2) Run pacing calculations for all active projects, (3) Dispatch any unsent alerts to Slack, (4) Generate and send daily summaries to each project channel, (5) Log the overall run status. Create a /api/admin/daily-run POST endpoint and also make it runnable as a standalone script (python -m backend.services.daily_job). Add error handling so each step continues even if a previous step partially fails."

### Task 9: Frontend Foundation (45 min)
Set up the Next.js frontend with the core dashboard views.

**Cursor instruction:** "Set up the Next.js 14 frontend in /frontend using the App Router. Install and configure: Tailwind CSS, Recharts, TanStack Table, Lucide React icons. Create these pages: (1) / — Overview showing all active projects with pacing status badges (green/amber/red), total spend, days remaining, budget gauge, (2) /project/[code] — Project detail with tabs for Pacing, Performance, and Alerts, (3) /project/[code]/pacing — Pacing tab showing KPI cards (total budget, spent, remaining, pacing %), per-line pacing bars, blocking chart Gantt visualization, (4) /project/[code]/performance — Performance tab with date picker, spend over time chart, platform breakdown, campaign table, (5) /alerts — Alert dashboard with filtering. Use a shared API client that fetches from the FastAPI backend (default localhost:8000). Use a consistent dark theme. Make it look professional and modern. All data should be real from the API — no mock data."

### Task 10: Project Setup & Testing (30 min)
Wire everything together with real data.

**Cursor instruction:** "Create a project setup script at scripts/setup_project.py that: (1) Creates a project in dim_projects given a project_code, name, client_id, dates, budget, and slack_channel_id, (2) Triggers a media plan sync if a sheet_id is provided, (3) Runs the transformation for that project's date range, (4) Runs pacing calculations. Test with project 25013 (BCGEU Bargaining) — it has confirmed data in BigQuery with ~$89K in Meta spend across multiple campaigns. Also create scripts/seed_test_data.py that inserts a few projects into dim_projects for testing."

## After the Sprint
- Review what we've built and test with real campaigns
- Run the full history transformation to populate fact_digital_daily with all historical data
- Set up Cloud Scheduler for daily automated runs
- Add Firebase authentication
- Deploy to Cloud Run
- Begin Phase 2 connector work (starting with Meta)

## Key Files Reference
- CLAUDE.md — Project context for Cursor
- infrastructure/bigquery/schema.sql — All table DDL
- ingestion/transformation/transform_funnel_to_unified.sql — Daily transformation
- ingestion/transformation/transform_funnel_to_unified_full_history.sql — Full migration
- backend/ — FastAPI application
- frontend/ — Next.js dashboard
