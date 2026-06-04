-- ============================================================================
-- Migration: AI-102 — first-class `clicks_all` column — 2026-06-03
-- ============================================================================
-- Adds `clicks_all` to fact_digital_daily as a labeled sibling of the
-- canonical `clicks` column:
--
--   clicks      — UNCHANGED. The platform's destination-intent click.
--                 Meta: Link_Clicks; TikTok: Clicks_Destination;
--                 Snapchat: Swipes; Pinterest: Paid_Outbound_Clicks;
--                 Google/StackAdapt/LinkedIn/Reddit: platform clicks.
--                 Same definition at every grain (verified in the AI-102
--                 investigation — the "grain bug" as filed did not reproduce).
--   clicks_all  — NEW. All clicks including on-platform actions.
--                 Meta: Clicks_all; TikTok: Clicks_All; NULL for platforms
--                 without the concept. NULL on pre-backfill rows.
--
-- The same deploy remaps Meta `engagements` Clicks_all → Post_Engagement
-- (and TikTok engagements → NULL) in the transform SQL; that remap needs no
-- DDL — the column already exists — but it DOES need the FULL-mode backfill
-- below to reach historical rows.
--
-- fact_adset_daily carries no click columns (reach/frequency/impressions/
-- video only) and needs no ALTER — confirmed in the AI-102 investigation.
--
-- ORDERING IS LOAD-BEARING:
--   1. Run this ALTER TABLE FIRST (before deploying the transform change).
--      transformation.py loads via load_table_from_json with WRITE_APPEND and
--      no schema-update option — an unknown `clicks_all` key in the payload
--      fails the load. The column is additive + nullable, so running it
--      before deploy is safe: current code simply never writes it.
--   2. Merge/deploy the backend (new transform SQL + router exposure).
--   3. Trigger a FULL-mode transformation backfill
--      (POST /api/admin/run-transformation?mode=full). Daily mode only
--      rewrites the trailing 7 days; FULL mode TRUNCATEs and reloads all
--      history, populating clicks_all AND applying the engagements remap
--      everywhere in one run.
--
-- Run against prod via `bq` CLI (cip dataset is in Montreal —
-- northamerica-northeast1; staging and prod share it):
--   bq query --project_id=point-blank-ada \
--            --location=northamerica-northeast1 \
--            --use_legacy_sql=false \
--            < 2026-06-03_clicks_all.sql
--
-- Rollback is at the bottom of this file (commented out).
-- ============================================================================


-- ── 1. fact_digital_daily: add clicks_all ───────────────────────────────────
-- Idempotent (IF NOT EXISTS); additive nullable column, no rewrite.

ALTER TABLE `point-blank-ada.cip.fact_digital_daily`
  ADD COLUMN IF NOT EXISTS clicks_all INT64;


-- ── 2. Verification queries ─────────────────────────────────────────────────
-- Run these after the migration / after the FULL backfill to confirm success.

-- (a) Column exists:
-- SELECT column_name, data_type, is_nullable
-- FROM `point-blank-ada.cip.INFORMATION_SCHEMA.COLUMNS`
-- WHERE table_name = 'fact_digital_daily' AND column_name = 'clicks_all';

-- (b) AFTER the FULL backfill — 26018 Meta flight-to-date ground truth
--     (canonical clicks unchanged; clicks_all = the value previously hiding
--     under `engagements`; engagements now = post_engagement):
-- SELECT
--   SUM(clicks)          AS clicks,           -- ≈ 7,363 as of 2026-06-02 (link clicks, unchanged)
--   SUM(clicks_all)      AS clicks_all,       -- ≈ 26,012 as of 2026-06-02
--   SUM(engagements)     AS engagements,      -- ≈ 12,736 as of 2026-06-02 (= post_engagement)
--   SUM(post_engagement) AS post_engagement   -- engagements must equal this for Meta
-- FROM `point-blank-ada.cip.fact_digital_daily`
-- WHERE project_code = '26018' AND platform_id = 'meta';

-- (c) Grain consistency (campaign vs adset vs ad sums must agree):
-- SELECT 'campaign' AS grain, SUM(s) AS clicks, SUM(sa) AS clicks_all FROM (
--   SELECT campaign_id, SUM(clicks) s, SUM(clicks_all) sa
--   FROM `point-blank-ada.cip.fact_digital_daily`
--   WHERE project_code = '26018' AND platform_id = 'meta' GROUP BY 1)
-- UNION ALL
-- SELECT 'ad', SUM(s), SUM(sa) FROM (
--   SELECT ad_id, SUM(clicks) s, SUM(clicks_all) sa
--   FROM `point-blank-ada.cip.fact_digital_daily`
--   WHERE project_code = '26018' AND platform_id = 'meta' GROUP BY 1);

-- (d) No NULL clicks_all left on Meta/TikTok rows after the FULL backfill:
-- SELECT platform_id, COUNTIF(clicks_all IS NULL) AS null_rows, COUNT(*) AS total
-- FROM `point-blank-ada.cip.fact_digital_daily`
-- WHERE platform_id IN ('meta', 'tiktok')
-- GROUP BY platform_id;
-- Expected: null_rows = 0 (NULL source values load as 0 via CAST… actually
-- NULL source stays NULL — expect null_rows ≈ rows whose Funnel source had
-- no Clicks_all value, which should be rare; investigate if > 1%).

-- (e) vw_fact_digital_daily is SELECT * — it picks up clicks_all on its next
--     self-healing refresh (every transformation run). No action needed:
-- SELECT clicks_all FROM `point-blank-ada.cip.vw_fact_digital_daily` LIMIT 1;


-- ============================================================================
-- ROLLBACK (commented out — uncomment and run to reverse)
-- ============================================================================
-- ALTER TABLE `point-blank-ada.cip.fact_digital_daily` DROP COLUMN IF EXISTS clicks_all;
-- (The engagements remap reverses by reverting the transform SQL and
--  re-running the FULL-mode backfill.)
-- ============================================================================
