-- ============================================================================
-- Migration: Creative assets + ad-set targeting (Phase 19) — 2026-06-11
-- ============================================================================
-- Adds the two tables behind the creative thumbnail + audience persona
-- features:
--
--   * `creative_assets`  — sync ledger for ad stills pulled from Meta /
--     StackAdapt into GCS (creative-assets/ prefix in the shared resources
--     bucket). One row per creative variant; the sync MERGEs on variant.
--   * `adset_targeting`  — plain-English personas + pool sizes rendered
--     from Meta ad-set targeting specs, keyed by the audiences/matrix
--     endpoint's slug so the read path joins for free.
--
-- Both tables are written by backend/services/creative_assets.py and read
-- additively by backend/routers/creative.py. The endpoints degrade
-- gracefully when the tables are absent, so this migration can land
-- before or after the backend deploy.
--
-- Run against prod via `bq` CLI:
--   bq query --project_id=point-blank-ada \
--            --location=northamerica-northeast1 \
--            --use_legacy_sql=false \
--            < 2026-06-11_creative_assets.sql
--
-- Rollback is at the bottom of this file (commented out).
-- ============================================================================


-- ── 1. New table: creative_assets ───────────────────────────────────────────
-- status: 'stored' | 'no_match' | 'fetch_failed'. Non-stored rows retry on
-- later runs, at most once per UTC day (checked_at guard in the sync).

CREATE TABLE IF NOT EXISTS `point-blank-ada.cip.creative_assets` (
  variant STRING NOT NULL,
  project_code STRING,
  source_platform STRING,
  gcs_path STRING,
  status STRING NOT NULL,
  checked_at TIMESTAMP NOT NULL
)
CLUSTER BY variant
OPTIONS(
  description='Creative thumbnail sync ledger (Phase 19). One row per creative variant; gcs_path is an object under creative-assets/ in the shared resources bucket (signed-URL reads only). status: stored | no_match | fetch_failed.'
);


-- ── 2. New table: adset_targeting ───────────────────────────────────────────
-- saturation is derived at read time (reach / pool_size), never stored.

CREATE TABLE IF NOT EXISTS `point-blank-ada.cip.adset_targeting` (
  audience_key STRING NOT NULL,
  platform_id STRING NOT NULL,
  persona STRING,
  pool_size INT64,
  fetched_at TIMESTAMP NOT NULL
)
CLUSTER BY audience_key
OPTIONS(
  description='Ad-set targeting personas (Phase 19). audience_key = the audiences/matrix slug of ad_set_name + platform_id; persona is a deterministic plain-language render of the Meta targeting spec; pool_size from delivery_estimate.'
);


-- ── 3. Verification queries ─────────────────────────────────────────────────
-- Run these after the migration to confirm success.

-- Check both tables exist with the right schema:
-- SELECT table_name, column_name, data_type, is_nullable
-- FROM `point-blank-ada.cip.INFORMATION_SCHEMA.COLUMNS`
-- WHERE table_name IN ('creative_assets', 'adset_targeting')
-- ORDER BY table_name, ordinal_position;

-- After the first sync run (POST /api/admin/creative-assets/sync), check
-- the ledger filled and statuses look sane:
-- SELECT status, COUNT(*) AS n
-- FROM `point-blank-ada.cip.creative_assets`
-- GROUP BY status;

-- And that personas landed for Meta ad sets:
-- SELECT audience_key, persona, pool_size, fetched_at
-- FROM `point-blank-ada.cip.adset_targeting`
-- ORDER BY fetched_at DESC LIMIT 20;


-- ============================================================================
-- ROLLBACK (commented out — uncomment and run to reverse)
-- ============================================================================
-- DROP TABLE IF EXISTS `point-blank-ada.cip.creative_assets`;
-- DROP TABLE IF EXISTS `point-blank-ada.cip.adset_targeting`;
-- ============================================================================
