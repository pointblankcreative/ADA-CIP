-- Cleanup stale media_plan_lines rows that survived from before
-- _delete_old_versions() shipped (2026-04-12 in commit 0a6cd71).
--
-- Background:
-- Sync used to write new lines without purging old ones. The dedup CTE
-- (PARTITION BY line_id ORDER BY sync_version DESC) correctly survives
-- across syncs that REUSE line_ids, but each sync invented brand-new
-- line_ids prefixed by the new plan_id, so nothing collided and old
-- syncs' lines accumulated forever. Project 26009 is the only known
-- project still carrying this residue (5 stale plan_ids, $415k of
-- inflated budget).
--
-- The fix uses media_plans.is_current as the truth signal — already
-- correctly maintained per project. We delete any media_plan_lines row
-- whose (project_code, plan_id) doesn't match a media_plans row with
-- is_current = TRUE.
--
-- Idempotent and safe to re-run. Run once before the ADAC-37 backfill,
-- and once more after if any new historical syncs surface in the wipe.

-- ── Step 1: Verify the scope before deleting ─────────────────────────
-- Always run this first. Compare the row count to your expectation.

SELECT
  mpl.project_code,
  mpl.plan_id,
  COUNT(*) AS rows_to_delete,
  SUM(IFNULL(mpl.budget, 0)) AS budget_being_dropped
FROM `point-blank-ada.cip.media_plan_lines` mpl
LEFT JOIN `point-blank-ada.cip.media_plans` mp
  ON mp.project_code = mpl.project_code
 AND mp.plan_id      = mpl.plan_id
 AND mp.is_current   = TRUE
WHERE mp.plan_id IS NULL  -- no matching is_current=TRUE row
GROUP BY mpl.project_code, mpl.plan_id
ORDER BY mpl.project_code, mpl.plan_id;

-- Expected output (as of 2026-04-25):
--   26009 plan-26009-001         2 rows   $75,000
--   26009 plan-26009-06436895    3 rows   $84,993
--   26009 plan-26009-132c055f    3 rows   $84,993
--   26009 plan-26009-95b30d03    3 rows   $84,993
--   26009 plan-26009-d54db4f2    3 rows   $84,993
-- Five plan_ids, 14 rows, ~$415k of stale budget.


-- ── Step 2: Delete the stale rows ────────────────────────────────────
-- Run this after confirming the scope above. Wraps the same JOIN
-- predicate; only deletes what the verify step previewed.

DELETE FROM `point-blank-ada.cip.media_plan_lines` mpl
WHERE NOT EXISTS (
  SELECT 1
  FROM `point-blank-ada.cip.media_plans` mp
  WHERE mp.project_code = mpl.project_code
    AND mp.plan_id      = mpl.plan_id
    AND mp.is_current   = TRUE
);


-- ── Step 3: Verify post-cleanup ──────────────────────────────────────
-- Run after the DELETE. Every project should now show exactly 1 plan_id
-- per row in media_plan_lines. Re-running Step 1 should return 0 rows.

WITH mpl_dedup AS (
  SELECT * EXCEPT(_rn) FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY line_id ORDER BY sync_version DESC) AS _rn
    FROM `point-blank-ada.cip.media_plan_lines`
  ) WHERE _rn = 1
)
SELECT
  project_code,
  COUNT(DISTINCT plan_id) AS distinct_plan_ids,
  COUNT(*) AS surviving_lines,
  SUM(IFNULL(budget, 0)) AS budget_total
FROM mpl_dedup
GROUP BY project_code
ORDER BY project_code;

-- Expected output (every project should show distinct_plan_ids = 1):
--   25034    1   16     25,500.01
--   25042    1    4     34,855.00
--   25049    1    3     10,399.89
--   25055    1    6     46,452.49
--   26009    1    3     84,992.83  ← was 17 lines / $499,964 before cleanup


-- ── Step 4 (optional): Also clean up orphaned blocking_chart_weeks ───
-- blocking_chart_weeks doesn't carry plan_id, only line_id. After
-- Step 2 deletes stale media_plan_lines rows, any blocking_chart_weeks
-- rows that pointed at those line_ids become orphans. Drop them.

SELECT
  bcw.project_code,
  COUNT(*) AS orphan_rows_to_delete
FROM `point-blank-ada.cip.blocking_chart_weeks` bcw
LEFT JOIN `point-blank-ada.cip.media_plan_lines` mpl USING (line_id)
WHERE mpl.line_id IS NULL
GROUP BY bcw.project_code
ORDER BY bcw.project_code;

-- If non-empty, run:
--   DELETE FROM `point-blank-ada.cip.blocking_chart_weeks` bcw
--   WHERE NOT EXISTS (
--     SELECT 1 FROM `point-blank-ada.cip.media_plan_lines` mpl
--     WHERE mpl.line_id = bcw.line_id
--   );
