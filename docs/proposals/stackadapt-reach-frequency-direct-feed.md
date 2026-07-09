# Design proposal — Restore StackAdapt reach & frequency via a direct API feed

Asana: `1215990005858637` (High) · Supersedes old Asana `1214991008124165` · Vault decision 2026-05-20, **Option B (supplement, not replace)**.

> **Status: design / hand-off — API validated 2026-07-09.** The StackAdapt `reachFrequency` API has been exercised end-to-end against production (read-only key) and the schema/behaviour below is confirmed, not assumed. Still **not implemented** — the build needs a new BigQuery dataset/table, a daily ETL, a 30-day backfill, and ±2% reconciliation, which sit outside the autonomous frontend/isolated-backend zone and require Frazer to provision the dataset + confirm the runtime key grant. See "What cannot be done autonomously" at the end.

## Problem (one line)

StackAdapt reach/frequency from Funnel are a 1-day per-creative field (wrong by 7–10×) and frequency is effectively hardcoded, so v1 hides both behind a stopgap. We must pull the real numbers from the StackAdapt Reporting API and restore them **without** disturbing Funnel-sourced spend / impressions / clicks.

## Current-state findings (file:line evidence)

### Where the wrong numbers originate (ingestion / transform)
- `ingestion/transformation/transform_funnel_to_unified.sql:202-203` — StackAdapt reach/frequency are mapped from Funnel columns:
  - `CAST(Unique_impressions_1_Day_Creative__StackAdapt AS INT64) AS reach` — a **1-day, per-creative** unique-impressions field, NOT deduplicated multi-day campaign reach → the 7–10× overcount.
  - `CAST(Frequency_1_Day_Creative__StackAdapt AS FLOAT64) AS frequency` — 1-day per-creative frequency (near-constant upstream).
  - Join key at `:190` (`Campaign_ID__StackAdapt AS campaign_id`), gate at `:232-233`. The full-history variant (`transform_funnel_to_unified_full_history.sql`) and `ingestion/transformation/adset_transform.py` carry the same mapping.
- Compare Meta at `:58-59` (`Reach___7_Day_Ad_Set__Facebook_Ads`, `Frequency___7_Day_Ad_Set__Facebook_Ads`) and Snapchat/TikTok 7-day fields — those are trustworthy; StackAdapt is the outlier.

### The v1 hidden-metric stopgap (removed by this ticket)
Lives entirely in `backend/routers/performance.py`, keyed on one constant:
- `RF_EXCLUDED_PLATFORMS == {"stackadapt"}` and `RF_EXCLUDED_NOTE = "StackAdapt reach/frequency hidden pending direct API integration."` (`performance.py:91-104`).
- SQL-side exclusion: conditional-NULL projection `IF(f.platform_id IN UNNEST(@rf_excluded), NULL, f.reach)` / `...f.frequency` on totals/daily/platform/campaign (`:587-588`, `:600-601`, `:631-632`, `:721-722`, `:753-754`); `NOT IN UNNEST(@rf_excluded)` guard on every `fact_adset_daily` rollup (`:658-698`).
- Python-side null-out on every per-row breakdown so the frontend renders an em-dash (`:340-350` adsets, `:1023-1027` by_platform, `:1048-1052` campaigns).
- `reach_note` append + `metric_platforms`/`reach_platforms` filtering (`:301-308`, `:734-737`, `:860-895`).
- Regression suites that pin the current behaviour and must be flipped/retired: `tests/test_performance_rf_stopgap.py` (its head note: "When the direct-API supplement ships and RF_EXCLUDED_PLATFORMS is emptied, delete this file"), `tests/test_performance_rf_provenance.py`, `tests/test_performance_adset_reach.py`.

### External-API integration reference pattern
- `backend/services/creative_assets.py` is the template: `httpx` client, `STACKADAPT_GRAPHQL_URL = "https://api.stackadapt.com/graphql"` (`:57`), Meta Graph base (`:54`), `MAX_PAGES` paging guard (`:61`), `HTTP_TIMEOUT` (`:59`), log-and-continue, no-ops when tokens unset.
- Secrets already wired: `backend/config.py:52-53` (`meta_access_token`, `stackadapt_api_key`); env `META_ACCESS_TOKEN` / `STACKADAPT_API_KEY` from Secret Manager `cip-meta-token` / `cip-stackadapt-key` (CLAUDE.md Phase 19; `.github/workflows/deploy.yml`).
- Daily wiring: `backend/services/daily_job.py:230-249` runs `creative_assets.run_sync()` as a best-effort stage (never raises). A StackAdapt R&F sync would be a sibling stage here.
- BQ table conventions: `infrastructure/bigquery/migrations/2026-06-11_creative_assets.sql` — `CREATE TABLE IF NOT EXISTS` in `point-blank-ada.cip`, `CLUSTER BY`, `OPTIONS(description=...)`, verification queries + commented rollback, run via `bq query --location=northamerica-northeast1`.

### How diagnostics D1/D2 consume reach/frequency
- Engine sources platform reach/frequency from `fact_adset_daily` (campaign-grain reach in `fact_digital_daily` is unreliable): `backend/services/diagnostics/engine.py:383-493` — `MAX(reach)` per (platform, campaign, reach_window), 7d>1d window priority.
- `backend/services/diagnostics/persuasion/distribution.py`: D1 Reach Attainment reads `p.reach` per platform (`:242-255`), guard `check_has_reach_data`; D2 Frequency Adequacy reads `p.frequency`/`p.impressions` (`:358-383`); D3/D4/D5 also consume reach/frequency.
- For a **StackAdapt-only** campaign today the engine gets StackAdapt's inflated Funnel reach (or, where the adset rollup is thin, no reach) → D1/D2 misfire or guard-fail. **Note:** the diagnostics engine reads `fact_adset_daily` directly and is **NOT** gated by the performance-router `RF_EXCLUDED_PLATFORMS` stopgap — so it is presently scoring against wrong StackAdapt numbers. This is the second half of the acceptance criterion ("D1/D2 fire correctly for StackAdapt-only campaigns") and must be wired to the new source.

## Live API validation (2026-07-09, read-only production key)

Verified end-to-end against `https://api.stackadapt.com/graphql` before locking the design. Findings:

- **Dedicated `reachFrequency` query** returns a `ReachFrequencyStatsConnection`. Per-node fields we use:
  - `campaign { id name }`, `channel`
  - `uniqueImpressions` → **individual reach** · `frequency` → individual frequency · `impressions`
  - `periodResidentialUniqueImp` → **household reach** · `periodResidentialFrequency` → household frequency · `periodResidentialImp` → household impressions ("residential" = household)
  - `periodStart` / `periodEnd` (ISO8601Date, campaign timezone)
- **Filter** `ReachFrequencyFilters!`: `campaignIds: [ID!]` (batch many per call), `startTime`/`endTime` (ISO8601DateTime), and `period: Int!` — **must be one of {1, 3, 7, 14, 30}**.
- **`period` is a fixed CALENDAR bucket size, not a rolling window.** `startTime`/`endTime` only select *which* calendar buckets return; they do not define the dedup window. Proven live:
  - period=30, requested 06-09→07-09 → returned full calendar months **June 01–30** (uniq 60,643) and **July 01–31** (uniq 30,229).
  - period=30, requested only 06-12→06-18 → returned the **whole** June 01–30 bucket (uniq 60,643), not the sub-range.
  - period=7 → fixed calendar weeks; period=1 → clean per-day rows.
  - **Consequence: true flight-to-date dedup reach for a multi-bucket flight is NOT obtainable** (reach is non-additive; buckets are calendar-fixed). FTD is dropped from scope — see the headline definition below.
- **Household cutoff:** `periodResidential*` is captured **from 2026-06-03 onward**; earlier periods return 0. Individual (`uniqueImpressions`) has full history.
- **Join key confirmed:** `campaign.id` is the SA-native numeric id (e.g. `3272754` = "26023 - Decision Makers and Staff - Video Reach"); campaign names carry our project codes. This equals Funnel's `Campaign_ID__StackAdapt` (spot-check against `cip.fact_*` at build time).
- **Auth/endpoint:** `Authorization: Bearer <token>` at `https://api.stackadapt.com/graphql`. Reuse `settings.stackadapt_api_key` (already wired). GraphQL errors return **HTTP 200** with an `errors[]` envelope — must be parsed from the body, not the status code.
- **Rate limits are a non-issue** for this workload: cost-based leaky bucket (40,000 budget, 8,000/sec restore, 40,000 max query cost). All 14 live campaigns × daily × 30 days ≈ ~420 nodes (one page); a few hundred points against a 40,000 bucket.

## Proposed architecture

### New dataset + table
**New dataset** `point-blank-ada.cip_stackadapt` (region `northamerica-northeast1`) to keep Funnel's contract clean. Single table `cip_stackadapt.stackadapt_reach_frequency`, one row per (campaign, grain, calendar bucket):

| column | type | source / notes |
|---|---|---|
| `campaign_id` | STRING NOT NULL | `campaign.id`; joins on `Campaign_ID__StackAdapt` (= `fact_*.campaign_id` for SA rows) |
| `campaign_name` | STRING | `campaign.name` (debug/reconciliation aid) |
| `channel` | STRING | `channel` (often null) |
| `period_days` | INT64 NOT NULL | grain: **1 (daily)**, **7 (weekly)**, **30 (monthly)** |
| `period_start` | DATE NOT NULL | `periodStart` — calendar bucket start |
| `period_end` | DATE NOT NULL | `periodEnd` — calendar bucket end |
| `reach_individual` | INT64 | `uniqueImpressions` (AI-113: individual reach) |
| `frequency_individual` | FLOAT64 | `frequency` |
| `reach_household` | INT64 | `periodResidentialUniqueImp` (AI-113: household reach; 0 before 2026-06-03) |
| `frequency_household` | FLOAT64 | `periodResidentialFrequency` (0 before 2026-06-03) |
| `impressions` | INT64 | `impressions` (SA-reported; Funnel stays SoT for the surfaced impressions figure) |
| `impressions_household` | INT64 | `periodResidentialImp` |
| `fetched_at` | TIMESTAMP NOT NULL | run stamp |

`CLUSTER BY campaign_id`, `OPTIONS(description=...)`. Primary grain = **(campaign_id, period_days, period_start)**. MERGE upsert on that key (current/most-recent buckets are re-fetched daily and overwrite).

### Grains stored (windows, revised)
Three native calendar grains — **daily (period=1)** for the trend sparkline, **weekly (period=7)** for reporting (frequently requested), **monthly (period=30)** as the headline. No `ftd` grain (unsupported — see validation above).

### Headline reach/frequency (replaces the dash; feeds diagnostics)
The **current calendar-month bucket** (period_days=30, `period_start = DATE_TRUNC(CURRENT_DATE(), MONTH)`), month-to-date, refreshed daily. For the many short single-month flights that IS the flight reach; for multi-month flights the UI shows each month with an honest "StackAdapt reports reach per calendar month; months can't be summed" note (consistent with the platform's existing honesty-guard voice). Individual is primary; household shown where available (≥ 2026-06-03).

### Daily ETL module
New `backend/services/stackadapt_rf_sync.py` modelled on `creative_assets.py`:
- `httpx` POST to `https://api.stackadapt.com/graphql`, `Authorization: Bearer {settings.stackadapt_api_key}`.
- No-op when the key is unset; log-and-continue; time-budgeted like the 240s creative sync.
- Fetch reach/frequency (individual & household) for all SA campaigns delivering in the window, **batched via `campaignIds: [...]`**, once per grain (period ∈ {1, 7, 30}). Daily incremental refreshes the current month, recent weeks, and recent days; the 30-day backfill pulls daily + overlapping weekly/monthly buckets.
- **Error handling:** parse the HTTP-200 `errors[]` envelope; on a throttle error honor `throttle.retryAfterInSeconds` from the response (do NOT use a fixed/exponential backoff — the server tells us the wait). Keep `X-GraphQL-Query-Cost: true` on in dev to log `actualCost`. Per-run circuit breaker (stop after N consecutive failures) like the Phase 19 delivery-estimate breaker.
- `load_table_from_json()` MERGE into the new table (cross-region-safe pattern — never `INSERT ... SELECT` across regions, CLAUDE.md §4.1).
- Register a new stage in `daily_job.py` (~`:230`, sibling of creative_assets), best-effort, never raises.
- Admin trigger `POST /api/admin/stackadapt-rf/sync?force=true` mirroring the creative-assets admin route.

### Join path + router changes (surface real numbers)
- `backend/routers/performance.py`: replace the stopgap. Instead of NULLing StackAdapt R&F, `LEFT JOIN cip_stackadapt.stackadapt_reach_frequency` (current-month bucket for the headline; weekly/daily grains available for reporting/trend) on `campaign_id`, and COALESCE the SA-direct reach/frequency over the Funnel column **for StackAdapt rows only**; Funnel remains untouched for spend/impressions/clicks. Set `RF_EXCLUDED_PLATFORMS = set()` once the join is live (that alone flips the note / metric_platforms logic off).
- Surface `reach_individual` vs `reach_household` separately (new model fields) — the AI-113 "currently collapsed" ask.
- Update `backend/models/performance.py` for the household/individual split.
- Flip/retire `tests/test_performance_rf_stopgap.py`, `test_performance_rf_provenance.py`, `test_performance_adset_reach.py`; add tests asserting SA reach now flows and individual ≠ household is preserved.

### Diagnostics wiring
- `backend/services/diagnostics/engine.py:369-493` — for StackAdapt, prefer the new table's **current-month** reach/frequency over `fact_adset_daily`, so D1/D2/D3/D4 score StackAdapt-only campaigns on real numbers ("current reach standing"). Individual reach is the D1/D4 input; household is informational. No change to signal math; only the source of `p.reach`/`p.frequency` for SA.

## Open questions — all resolved via live validation

1. **API rate limits.** Cost-based leaky bucket, hugely over-budget for our workload. Client: batch campaigns per query, parse the HTTP-200 `errors[]` envelope, honor `throttle.retryAfterInSeconds`, per-run circuit breaker. (Replaces the earlier "sequential per-campaign + 429 backoff" guess — this API returns 200 + a retry hint, not 429.)
2. **Flight-to-date support.** **Not supported** as a dedup figure — `period` is a fixed calendar bucket ({1,3,7,14,30}) and reach is non-additive. Resolved by defining the headline as the **current calendar-month bucket** and surfacing weekly/daily grains; FTD dropped from scope.
3. **Schema location.** **New dataset** `cip_stackadapt`, same region (`northamerica-northeast1`) so joins to `cip.fact_*` stay in-region and DML rules hold.

## Proposed CLAUDE.md source-of-truth rule (to add when built)

> **Reach/Frequency source of truth.** Spend, impressions, and clicks always come from Funnel (`fact_digital_daily` / `fact_adset_daily`). Reach and frequency come from Funnel's 7-day platform fields for every platform EXCEPT StackAdapt. For StackAdapt, reach and frequency come from the direct StackAdapt `reachFrequency` API feed in `cip_stackadapt.stackadapt_reach_frequency` (joined on `Campaign_ID__StackAdapt`), because Funnel's `Unique_impressions_1_Day_Creative__StackAdapt` is a 1-day per-creative field that overcounts true dedup reach by 7–10×. StackAdapt reports dedup reach only in fixed calendar buckets (daily/weekly/monthly); the campaign headline is the **current calendar-month** bucket, never a summed flight-to-date figure (reach is non-additive). Reach is stored and shown as individual and household ("residential") separately (never collapsed); household data exists only from 2026-06-03 onward. Never reintroduce the Funnel StackAdapt reach/frequency columns into any user-facing aggregate.

## File-touch list (eventual build)
- NEW `infrastructure/bigquery/migrations/2026-07-09_stackadapt_rf.sql` (dataset + table DDL).
- NEW `backend/services/stackadapt_rf_sync.py` (daily ETL).
- `backend/services/daily_job.py` (register stage).
- `backend/routers/admin.py` (admin sync trigger).
- `backend/config.py` (reuse `stackadapt_api_key`; possibly a dataset-name setting).
- `backend/routers/performance.py` (empty `RF_EXCLUDED_PLATFORMS`, add SA-direct join, household/individual split).
- `backend/models/performance.py` (+ possibly `models/creative.py`) — individual vs household fields.
- `backend/services/diagnostics/engine.py` (SA reach/freq source for D1/D2).
- `ingestion/transformation/transform_funnel_to_unified.sql` / `_full_history.sql` / `adset_transform.py` — optionally stop mapping the misleading SA reach/freq, or keep but never surface (decide during build).
- Frontend: em-dash rendering already handles nulls; add individual/household display + drop stopgap copy once backend ships (`frontend/src/app/project/[code]/performance-tab.tsx` + perf primitives).
- Tests: retire/flip the three stopgap suites above; new `test_stackadapt_rf_sync.py`, diagnostics D1/D2 SA-only cases; `CLAUDE.md` rule.

## Phased build plan (~1–2 weeks, matches ticket)
1. **Schema + secrets** — create `cip_stackadapt` dataset + table (Frazer/infra), confirm runtime SA key grant. (API contract already validated.)
2. **ETL module** — `stackadapt_rf_sync.py` against the `reachFrequency` query; store daily/weekly/monthly grains, individual + household; wire daily stage + admin trigger. (Response shape / field mapping confirmed 2026-07-09.)
3. **Backfill** — 30-day historical pull across 3 pilot projects (EN + FR).
4. **Read path** — router current-month join + model split; remove stopgap; flip tests.
5. **Diagnostics** — engine SA source swap (current-month bucket); verify D1/D2 fire for SA-only campaigns.
6. **Validate ±2%** — reconcile ADA reach/freq vs the StackAdapt R&F report across the 3 projects.
7. **Docs** — CLAUDE.md source-of-truth rule.

## What cannot be done autonomously in this sandbox
- ✅ **Resolved by the 2026-07-09 live test:** API contract, schema, field mapping (individual = `uniqueImpressions`, household = `periodResidentialUniqueImp`), calendar-bucket behaviour, join key, rate-limit reality, and the FTD limitation are all confirmed — no longer open questions.
- Cannot create the `cip_stackadapt` BigQuery dataset/table or run the 30-day backfill (no BQ write / `bq` access here).
- Cannot grant the runtime service account access to the SA key, or validate the ±2% acceptance against a live StackAdapt R&F report.
- The build still touches a **new BigQuery dataset/table + ingestion/ETL + a `.sql` migration** — outside the frontend / isolated-backend zone. Per the autonomy boundary this **parks** for Frazer: I build it and hand off a draft PR; I never deploy it.
