-- Cleanup script: remove duplicate media_plan_lines and blocking_chart_weeks
-- left behind by failed _delete_old_versions calls during media plan sync.
--
-- Run this ONCE in BigQuery console to fix existing data.
-- After this, the retry logic in _delete_old_versions + dedup CTEs in pacing
-- queries will prevent recurrence.
--
-- Safe to run multiple times — it's idempotent.

-- Step 1: For each project, find the latest sync_version in media_plans
-- where is_current = TRUE and delete all older media_plan_lines.

DELETE FROM `point-blank-ada.cip.media_plan_lines`
WHERE STRUCT(project_code, sync_version) NOT IN (
    SELECT AS STRUCT project_code, MAX(sync_version) AS sync_version
    FROM `point-blank-ada.cip.media_plan_lines`
    GROUP BY project_code
);

-- Step 2: Same for blocking_chart_weeks.

DELETE FROM `point-blank-ada.cip.blocking_chart_weeks`
WHERE STRUCT(project_code, sync_version) NOT IN (
    SELECT AS STRUCT project_code, MAX(sync_version) AS sync_version
    FROM `point-blank-ada.cip.blocking_chart_weeks`
    GROUP BY project_code
);

-- Step 3: Mark all but the latest media_plan per project as non-current.

UPDATE `point-blank-ada.cip.media_plans`
SET is_current = FALSE
WHERE is_current = TRUE
  AND STRUCT(project_code, sync_version) NOT IN (
    SELECT AS STRUCT project_code, MAX(sync_version) AS sync_version
    FROM `point-blank-ada.cip.media_plans`
    WHERE is_current = TRUE
    GROUP BY project_code
  );

-- Verification: these should all return 0 or 1 row per project.

-- SELECT project_code, COUNT(*) AS cnt
-- FROM `point-blank-ada.cip.media_plans`
-- WHERE is_current = TRUE
-- GROUP BY project_code
-- HAVING cnt > 1;
--
-- SELECT project_code, line_id, COUNT(*) AS cnt
-- FROM `point-blank-ada.cip.media_plan_lines`
-- GROUP BY project_code, line_id
-- HAVING cnt > 1;
