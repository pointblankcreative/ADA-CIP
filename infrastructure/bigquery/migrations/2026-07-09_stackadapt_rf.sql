-- ============================================================================
-- Migration: StackAdapt reach & frequency direct feed — 2026-07-09
-- Asana 1215990005858637 · design: docs/proposals/stackadapt-reach-frequency-direct-feed.md
-- ============================================================================
-- Funnel's StackAdapt reach/frequency are a 1-day per-creative field that
-- overcounts true dedup reach by 7-10x, so they sit hidden behind a stopgap
-- in routers/performance.py. This migration stands up the destination for a
-- direct StackAdapt `reachFrequency` API feed:
--
--   * NEW dataset  `point-blank-ada.cip_stackadapt` (region
--     northamerica-northeast1) — kept apart from `cip` so Funnel's contract
--     stays clean, same region so joins back to cip.fact_* stay in-region.
--   * NEW table    `cip_stackadapt.stackadapt_reach_frequency` — one row per
--     (campaign, grain, calendar bucket). Written by
--     backend/services/stackadapt_rf_sync.py, which loads returned rows via
--     load_table_from_json into a staging table and MERGEs on the primary
--     grain (campaign_id, period_days, period_start). Because both live in
--     northamerica-northeast1 the MERGE is in-region (CLAUDE.md §4.1: no
--     cross-region DML — the load-then-MERGE pattern honours that).
--
-- StackAdapt reports dedup reach only in FIXED CALENDAR buckets
-- (period_days ∈ {1 daily, 7 weekly, 30 monthly}); reach is non-additive, so
-- buckets are never summed. Individual reach (`uniqueImpressions`) has full
-- history; household / "residential" reach exists only from 2026-06-03 onward
-- (0 before that — stored as-is).
--
-- Run against prod via `bq` CLI:
--   bq query --project_id=point-blank-ada \
--            --location=northamerica-northeast1 \
--            --use_legacy_sql=false \
--            < 2026-07-09_stackadapt_rf.sql
--
-- Rollback is at the bottom of this file (commented out).
-- ============================================================================


-- ── 1. New dataset: cip_stackadapt ──────────────────────────────────────────
-- Same region as `cip` so the read-path join stays in-region.

CREATE SCHEMA IF NOT EXISTS `point-blank-ada.cip_stackadapt`
OPTIONS(
  location='northamerica-northeast1',
  description='StackAdapt direct-API feeds kept apart from the Funnel-sourced `cip` dataset. Currently holds reach/frequency (stackadapt_reach_frequency) because Funnel StackAdapt reach/frequency overcount true dedup reach by 7-10x.'
);


-- ── 2. New table: stackadapt_reach_frequency ────────────────────────────────
-- Primary grain (campaign_id, period_days, period_start); the sync MERGEs on
-- that key (current/most-recent buckets are re-fetched daily and overwrite).

CREATE TABLE IF NOT EXISTS `point-blank-ada.cip_stackadapt.stackadapt_reach_frequency` (
  campaign_id STRING NOT NULL,
  campaign_name STRING,
  channel STRING,
  period_days INT64 NOT NULL,
  period_start DATE NOT NULL,
  period_end DATE NOT NULL,
  reach_individual INT64,
  frequency_individual FLOAT64,
  reach_household INT64,
  frequency_household FLOAT64,
  impressions INT64,
  impressions_household INT64,
  fetched_at TIMESTAMP NOT NULL
)
CLUSTER BY campaign_id
OPTIONS(
  description='StackAdapt reach/frequency direct feed (Asana 1215990005858637). One row per (campaign_id, period_days, period_start) calendar bucket; period_days ∈ {1 daily, 7 weekly, 30 monthly}. reach_individual/frequency_individual from uniqueImpressions/frequency; reach_household/frequency_household/impressions_household from periodResidential* (0 before 2026-06-03). campaign_id = StackAdapt campaign.id (= Funnel Campaign_ID__StackAdapt). Reach is non-additive across buckets — never SUM across period_start. Written by backend/services/stackadapt_rf_sync.py via load_table_from_json + MERGE.'
);


-- ── 3. Verification queries ─────────────────────────────────────────────────
-- Run these after the migration to confirm success.

-- Dataset + table exist with the right schema:
-- SELECT column_name, data_type, is_nullable
-- FROM `point-blank-ada.cip_stackadapt.INFORMATION_SCHEMA.COLUMNS`
-- WHERE table_name = 'stackadapt_reach_frequency'
-- ORDER BY ordinal_position;

-- After the first sync run (POST /api/admin/stackadapt-rf/sync), check rows
-- landed across the three grains:
-- SELECT period_days, COUNT(*) AS n, MIN(period_start) AS earliest,
--        MAX(period_end) AS latest
-- FROM `point-blank-ada.cip_stackadapt.stackadapt_reach_frequency`
-- GROUP BY period_days
-- ORDER BY period_days;

-- Spot-check the current calendar-month headline bucket for a campaign:
-- SELECT campaign_id, campaign_name, reach_individual, frequency_individual,
--        reach_household, frequency_household
-- FROM `point-blank-ada.cip_stackadapt.stackadapt_reach_frequency`
-- WHERE period_days = 30
--   AND period_start = DATE_TRUNC(CURRENT_DATE(), MONTH)
-- ORDER BY reach_individual DESC;


-- ============================================================================
-- ROLLBACK (commented out — uncomment and run to reverse)
-- ============================================================================
-- DROP TABLE IF EXISTS `point-blank-ada.cip_stackadapt.stackadapt_reach_frequency`;
-- DROP SCHEMA IF EXISTS `point-blank-ada.cip_stackadapt`;
-- ============================================================================
