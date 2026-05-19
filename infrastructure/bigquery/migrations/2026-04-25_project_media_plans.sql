-- ============================================================================
-- Migration: Multi-plan support — 2026-04-25
-- ============================================================================
-- Adds `project_media_plans`, the join table that lets a single project
-- register multiple media plan sheets (e.g. a multi-flight campaign with one
-- sheet per phase). Backfills the new table from the legacy single-sheet
-- column `dim_projects.media_plan_sheet_id` so existing projects keep working
-- without intervention.
--
-- See the design ticket: project 25013 (BCGEU) has three sheets totalling
-- $2.24M, but CIP was only reading one because the data model assumed one
-- media plan per project.
--
-- Run against prod via `bq` CLI:
--   bq query --project_id=point-blank-ada \
--            --location=northamerica-northeast1 \
--            --use_legacy_sql=false \
--            < 2026-04-25_project_media_plans.sql
--
-- Rollback is at the bottom of this file (commented out).
-- ============================================================================


-- ── 1. New table: project_media_plans ───────────────────────────────────────
-- Clustered on project_code because every read is scoped to a project.

CREATE TABLE IF NOT EXISTS `point-blank-ada.cip.project_media_plans` (
  project_code   STRING    NOT NULL,
  sheet_id       STRING    NOT NULL,
  phase_label    STRING,                                 -- e.g. "Phase 1", "Pre-writ", "GOTV"
  display_order  INT64,                                  -- ascending; ties broken by created_at
  is_active      BOOL      DEFAULT TRUE,
  created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY project_code
OPTIONS(
  description='Join table mapping projects to one-or-more media plan sheets. Replaces the single-sheet assumption baked into dim_projects.media_plan_sheet_id. Keyed on (project_code, sheet_id).'
);


-- ── 2. Backfill from dim_projects.media_plan_sheet_id ───────────────────────
-- For every project that currently has a sheet ID set, insert one row into
-- project_media_plans with phase_label=NULL and display_order=1. Skip rows
-- that already exist (idempotent: this migration is safe to re-run).

INSERT INTO `point-blank-ada.cip.project_media_plans`
  (project_code, sheet_id, phase_label, display_order, is_active, created_at)
SELECT
  p.project_code,
  p.media_plan_sheet_id  AS sheet_id,
  NULL                   AS phase_label,
  1                      AS display_order,
  TRUE                   AS is_active,
  CURRENT_TIMESTAMP()    AS created_at
FROM `point-blank-ada.cip.dim_projects` p
LEFT JOIN `point-blank-ada.cip.project_media_plans` pmp
  ON pmp.project_code = p.project_code
  AND pmp.sheet_id   = p.media_plan_sheet_id
WHERE p.media_plan_sheet_id IS NOT NULL
  AND p.media_plan_sheet_id != ''
  AND pmp.project_code IS NULL;  -- not already present


-- ── 3. Verification queries ─────────────────────────────────────────────────
-- Run these after the migration to confirm success.

-- Check the new table exists with the right schema:
-- SELECT column_name, data_type, is_nullable
-- FROM `point-blank-ada.cip.INFORMATION_SCHEMA.COLUMNS`
-- WHERE table_name = 'project_media_plans'
-- ORDER BY ordinal_position;

-- Confirm every legacy single-sheet project was backfilled:
-- SELECT
--   p.project_code,
--   p.media_plan_sheet_id,
--   pmp.sheet_id          AS pmp_sheet_id,
--   pmp.is_active
-- FROM `point-blank-ada.cip.dim_projects` p
-- LEFT JOIN `point-blank-ada.cip.project_media_plans` pmp
--   ON pmp.project_code = p.project_code
--   AND pmp.sheet_id   = p.media_plan_sheet_id
-- WHERE p.media_plan_sheet_id IS NOT NULL AND p.media_plan_sheet_id != ''
-- ORDER BY p.project_code;
-- Expected: pmp_sheet_id = media_plan_sheet_id and is_active = TRUE for every row.

-- Spot-check 25013 (the multi-plan project the migration was built for):
-- SELECT * FROM `point-blank-ada.cip.project_media_plans`
-- WHERE project_code = '25013' ORDER BY display_order, created_at;
-- After this migration: 1 row (the legacy sheet). The remaining two sheets
-- need to be added through the admin UI Plans section once it ships.


-- ============================================================================
-- ROLLBACK (commented out — uncomment and run to reverse)
-- ============================================================================
-- DROP TABLE IF EXISTS `point-blank-ada.cip.project_media_plans`;
-- ============================================================================
