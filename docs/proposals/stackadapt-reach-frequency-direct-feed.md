# Design proposal — Restore StackAdapt reach & frequency via a direct API feed

Asana: `1215990005858637` (High) · Supersedes old Asana `1214991008124165` · Vault decision 2026-05-20, **Option B (supplement, not replace)**.

> **Status: design / hand-off.** This is a scoped technical design produced by the ticket resolver. It is **not** implemented — the build needs a new external StackAdapt API dependency, a new BigQuery dataset/table, a 30-day backfill, and ±2% live-data validation, all of which sit outside the autonomous frontend/isolated-backend zone and require Frazer to provision credentials + the dataset. See "What cannot be done autonomously" at the end.

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

## Proposed architecture

### New dataset + table
Recommend a **new dataset** `point-blank-ada.cip_stackadapt` (region `northamerica-northeast1`) to keep Funnel's contract clean, per the ticket. Single table `cip_stackadapt.stackadapt_reach_frequency`:

| column | type | notes |
|---|---|---|
| `campaign_id` | STRING NOT NULL | joins on `Campaign_ID__StackAdapt` (= `fact_*.campaign_id` for SA rows) |
| `date` | DATE NOT NULL | report snapshot date (FTD rows use `flight_end` or run date) |
| `window` | STRING NOT NULL | `1d` \| `7d` \| `30d` \| `ftd` (flight-to-date) |
| `reach_individual` | INT64 | AI-113: individual (device/cookie) reach |
| `reach_household` | INT64 | AI-113: household reach, surfaced separately |
| `frequency` | FLOAT64 | true dedup frequency for the window |
| `impressions` | INT64 | SA-reported, for provenance/reconciliation only (Funnel stays SoT for the surfaced impressions figure) |
| `fetched_at` | TIMESTAMP NOT NULL | run stamp |

`CLUSTER BY campaign_id`, `OPTIONS(description=...)`. Primary grain = (campaign_id, date, window). MERGE upsert on that key.

### Daily ETL module
New `backend/services/stackadapt_rf_sync.py` modelled on `creative_assets.py`:
- `httpx` against the StackAdapt Reporting API (GraphQL `https://api.stackadapt.com/graphql`), auth via `settings.stackadapt_api_key`.
- No-op when the key is unset; log-and-continue; `MAX_PAGES` paging guard; time-budgeted like the 240s creative sync.
- For each active StackAdapt campaign_id, request reach + frequency (individual & household) for windows 1d/7d/30d/ftd; `load_table_from_json()` MERGE into the new table (cross-region-safe pattern — never `INSERT ... SELECT` across regions, CLAUDE.md §4.1).
- Register a new stage in `daily_job.py` (~`:230`, sibling of creative_assets), best-effort, never raises.
- Admin trigger `POST /api/admin/stackadapt-rf/sync?force=true` mirroring the creative-assets admin route.

### Join path + router changes (surface real numbers)
- `backend/routers/performance.py`: replace the stopgap. Instead of NULLing StackAdapt R&F, `LEFT JOIN cip_stackadapt.stackadapt_reach_frequency` (7d window default, ftd for totals) on `campaign_id` and COALESCE the SA-direct reach/frequency over the Funnel column **for StackAdapt rows only**; Funnel remains untouched for spend/impressions/clicks. Set `RF_EXCLUDED_PLATFORMS = set()` once the join is live (that alone flips the note / metric_platforms logic off).
- Surface `reach_individual` vs `reach_household` separately (new model fields) — the AI-113 "currently collapsed" ask.
- Update `backend/models/performance.py` for the household/individual split.
- Flip/retire `tests/test_performance_rf_stopgap.py`, `test_performance_rf_provenance.py`, `test_performance_adset_reach.py`; add tests asserting SA reach now flows and individual ≠ household is preserved.

### Diagnostics wiring
- `backend/services/diagnostics/engine.py:369-493` — for StackAdapt, prefer the new table's reach/frequency over `fact_adset_daily` (LEFT JOIN or a supplemental fetch keyed by campaign_id/window), so D1/D2/D3/D4 score StackAdapt-only campaigns correctly. Individual reach is the D1/D4 input; household is informational. No change to signal math; only the source of `p.reach`/`p.frequency` for SA.

## Resolved recommendations for the OPEN QUESTIONS

1. **API rate limits.** Conservative client: sequential per-campaign requests with a small sleep, `MAX_PAGES` cap, `httpx` timeout, exponential backoff on 429, and a per-run circuit breaker (stop after N consecutive failures — same shape as the Phase 19 delivery-estimate breaker). Cache flight-to-date daily; only active campaigns are queried. Actual QPS ceilings must be confirmed against StackAdapt's published limits + our key tier — **cannot be verified without credentials.**
2. **Flight-to-date support.** Reach is **not additive**, so FTD cannot be synthesized by summing daily reach. If the Reporting API exposes an arbitrary date-range aggregate, request `flight_start..today` per campaign and store as `window='ftd'`. If it only offers fixed windows, store the largest native window (30d) as the flight proxy with an honest provenance note. **Recommendation:** use the API's native custom-range reach if available; otherwise 30d proxy. Requires live API exploration to finalize.
3. **Schema location.** **New dataset** `cip_stackadapt` (not a table inside `cip`), per "keep Funnel's contract clean." Same region (`northamerica-northeast1`) so joins to `cip.fact_*` stay in-region and DML rules hold; read-path cross-dataset joins in the same region are fine.

## Proposed CLAUDE.md source-of-truth rule (to add when built)

> **Reach/Frequency source of truth.** Spend, impressions, and clicks always come from Funnel (`fact_digital_daily` / `fact_adset_daily`). Reach and frequency come from Funnel's 7-day platform fields for every platform EXCEPT StackAdapt. For StackAdapt, reach and frequency come from the direct StackAdapt Reporting API feed in `cip_stackadapt.stackadapt_reach_frequency` (joined on `Campaign_ID__StackAdapt`), because Funnel's `Unique_impressions_1_Day_Creative__StackAdapt` is a 1-day per-creative field that overcounts true dedup reach by 7–10×. StackAdapt reach is reported as individual and household separately (never collapsed). Never reintroduce the Funnel StackAdapt reach/frequency columns into any user-facing aggregate.

## File-touch list (eventual build)
- NEW `infrastructure/bigquery/migrations/2026-07-08_stackadapt_rf.sql` (dataset + table DDL).
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
1. **Schema + secrets** — create `cip_stackadapt` dataset + table (Frazer/infra), confirm API key grant.
2. **ETL module** — `stackadapt_rf_sync.py` against live API; validate response shape, windows, individual/household split; wire daily stage + admin trigger.
3. **Backfill** — 30-day historical pull across 3 pilot projects (EN + FR).
4. **Read path** — router join + model split; remove stopgap; flip tests.
5. **Diagnostics** — engine SA source swap; verify D1/D2 fire for SA-only campaigns.
6. **Validate ±2%** — reconcile ADA reach/freq vs the StackAdapt R&F report across the 3 projects.
7. **Docs** — CLAUDE.md source-of-truth rule.

## What cannot be done autonomously in this sandbox
- No StackAdapt API credentials / no ability to discover real rate limits, response schema, individual-vs-household field names, or flight-to-date support (Q1–Q3 hinge on live API access).
- Cannot create the `cip_stackadapt` BigQuery dataset/table or run the 30-day backfill (no BQ write / `bq` access here).
- Cannot validate the ±2% acceptance against a live StackAdapt R&F report.
- This is squarely a **new external API dependency + new BigQuery dataset/table + ingestion/ETL change + live-data validation** — outside the frontend / isolated-backend zone. Per the autonomy boundary this is a hard **PARK** for Frazer.
