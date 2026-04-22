-- ============================================================================
-- Migration: FFS Entries — 2026-04-20
-- ============================================================================
-- Adds the `ffs_entries` table (one row per form per project) and two FK-ish
-- columns on `media_plan_lines` so lines can link to an entry and opt out
-- of propagation via an override flag.
--
-- Data model is documented in /Projects--00002-ADA/FFS Wizard Spec.md.
--
-- Run against prod via `bq` CLI:
--   bq query --project_id=point-blank-ada \
--            --location=northamerica-northeast1 \
--            --use_legacy_sql=false \
--            < 2026-04-20_ffs_entries.sql
--
-- Rollback is at the bottom of this file (commented out).
-- ============================================================================


-- ── 1. New table: ffs_entries ───────────────────────────────────────────────
-- One row per form (landing page or platform lead form) per project.
-- Clustered on project_code because every read is scoped to a project.

CREATE TABLE IF NOT EXISTS `point-blank-ada.cip.ffs_entries` (
  entry_id         STRING    NOT NULL,             -- UUID, generated server-side
  project_code     STRING    NOT NULL,
  label            STRING,                         -- user-facing, e.g. "underfunded.ca main"
  lp_url           STRING,                         -- canonical URL; NULL if platform form
  is_platform_form BOOL      DEFAULT FALSE,
  platform_id      STRING,                         -- 'meta' | 'linkedin' | 'tiktok' when is_platform_form
  ffs_inputs       JSON      NOT NULL,             -- raw wizard answers
  ffs_score        FLOAT64   NOT NULL,             -- server-computed via compute_ffs()
  created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  updated_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  created_by       STRING,                         -- PB email from IAP header
  PRIMARY KEY (entry_id) NOT ENFORCED
)
CLUSTER BY project_code
OPTIONS(
  description='Form Friction Score entries. One row per form (landing page or platform lead form) per project. Linked to media_plan_lines via media_plan_lines.ffs_entry_id. Populated by the FFS wizard in the project Settings tab.'
);


-- ── 2. New columns on media_plan_lines ──────────────────────────────────────
-- ffs_entry_id: FK to ffs_entries.entry_id. NULL = no entry linked (fallback
--               to generic F-pillar benchmarks, same as today).
-- ffs_override: TRUE = this line has a custom ffs_score/ffs_inputs that must
--               NOT be clobbered by entry propagation. Default FALSE so that
--               linked lines auto-sync from their entry.
--
-- NOTE: BigQuery disallows `ADD COLUMN ... DEFAULT` on an existing table —
--       adding a defaulted column requires a 3-step dance (add column, set
--       default, backfill nulls). The UPDATE below is trivial (~27 rows).

ALTER TABLE `point-blank-ada.cip.media_plan_lines`
  ADD COLUMN IF NOT EXISTS ffs_entry_id STRING,
  ADD COLUMN IF NOT EXISTS ffs_override BOOL;

ALTER TABLE `point-blank-ada.cip.media_plan_lines`
  ALTER COLUMN ffs_override SET DEFAULT FALSE;

UPDATE `point-blank-ada.cip.media_plan_lines`
  SET ffs_override = FALSE
  WHERE ffs_override IS NULL;


-- ── 3. Verification queries ─────────────────────────────────────────────────
-- Run these after the migration to confirm success.

-- Check the new table exists and has the right schema:
-- SELECT table_name, column_name, data_type, is_nullable
-- FROM `point-blank-ada.cip.INFORMATION_SCHEMA.COLUMNS`
-- WHERE table_name = 'ffs_entries'
-- ORDER BY ordinal_position;

-- Check the new columns on media_plan_lines:
-- SELECT column_name, data_type, is_nullable
-- FROM `point-blank-ada.cip.INFORMATION_SCHEMA.COLUMNS`
-- WHERE table_name = 'media_plan_lines'
--   AND column_name IN ('ffs_entry_id', 'ffs_override');

-- Confirm no existing rows were disturbed:
-- SELECT
--   COUNT(*)                            AS total_lines,
--   COUNTIF(ffs_entry_id IS NULL)       AS lines_without_entry,
--   COUNTIF(ffs_override = FALSE)       AS lines_not_overridden,
--   COUNTIF(ffs_score IS NOT NULL)      AS lines_with_existing_ffs
-- FROM `point-blank-ada.cip.media_plan_lines`;
-- Expected: lines_without_entry = total_lines, lines_not_overridden = total_lines,
--          lines_with_existing_ffs = 0 (since nothing populates ffs_score today).


-- ============================================================================
-- ROLLBACK (commented out — uncomment and run to reverse)
-- ============================================================================
-- ALTER TABLE `point-blank-ada.cip.media_plan_lines`
--   DROP COLUMN IF EXISTS ffs_entry_id,
--   DROP COLUMN IF EXISTS ffs_override;
--
-- DROP TABLE IF EXISTS `point-blank-ada.cip.ffs_entries`;
-- ============================================================================
