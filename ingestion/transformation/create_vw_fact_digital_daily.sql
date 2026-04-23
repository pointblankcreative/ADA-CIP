-- =============================================================================
-- VIEW: vw_fact_digital_daily
-- =============================================================================
-- Live view over fact_digital_daily that adds a `line_codes` column
-- (ARRAY<STRING>) holding the media-plan #XX codes parsed out of ad_set_name.
--
-- No schema change on the underlying table; no backfill required. The view
-- recomputes on every query, which is cheap because fact_digital_daily is
-- already aggregated to one row per (date, platform, campaign, adset, ad).
--
-- The regex MUST stay in sync with
-- `extract_line_codes_from_adset_name` in backend/services/media_plan_sync.py
-- (BQ_LINE_CODE_REGEX). Paired tests in backend/tests/test_media_plan_sync.py
-- verify the Python side matches the intended BQ behaviour.
--
-- Examples (from real Squamish 25034 data):
--   "#11 viewers BC, #12 list, followers, lookalikes BC" → ["#11", "#12"]
--   "#09 North Van Engagers"                              → ["#09"]
--   "Conversions CA"                                      → []   (no code)
-- =============================================================================

CREATE OR REPLACE VIEW `{project}.{dataset}.vw_fact_digital_daily` AS
SELECT
  *,
  IFNULL(REGEXP_EXTRACT_ALL(ad_set_name, r'#\d+[A-Za-z]?'), []) AS line_codes
FROM
  `{project}.{dataset}.fact_digital_daily`;
