-- ==============================================================================
-- SEED: Industry Benchmarks for Canadian Political/Labour/Issues Advertising
--
-- Placeholder values — to be refined with Frazer's research data.
-- These cover cross-platform defaults. Platform-specific benchmarks can be
-- added later by inserting additional rows with platform_id set.
-- ==============================================================================

DELETE FROM `point-blank-ada.cip.benchmarks`
WHERE benchmark_type = 'industry' AND source = 'industry_research';

INSERT INTO `point-blank-ada.cip.benchmarks`
  (benchmark_id, benchmark_type, scope, objective_type, platform_id, creative_format,
   metric_name, metric_unit, p25, p50, p75, sample_size, source, notes, valid_from, valid_to)
VALUES
  -- ── Awareness: cross-platform ──────────────────────────────────
  ('ind-aw-ctr', 'industry', 'canadian_political', 'awareness', NULL, NULL,
   'ctr', 'percentage', 0.003, 0.005, 0.008, 150, 'industry_research',
   'Canadian political/labour awareness campaigns 2024-2026', '2024-01-01', NULL),

  ('ind-aw-cpm', 'industry', 'canadian_political', 'awareness', NULL, NULL,
   'cpm', 'currency_cad', 8.0, 12.0, 18.0, 150, 'industry_research',
   'Canadian political/labour awareness campaigns 2024-2026', '2024-01-01', NULL),

  ('ind-aw-vcr', 'industry', 'canadian_political', 'awareness', NULL, NULL,
   'vcr', 'percentage', 0.40, 0.55, 0.70, 80, 'industry_research',
   'Video completion rate for awareness video campaigns', '2024-01-01', NULL),

  ('ind-aw-freq', 'industry', 'canadian_political', 'awareness', NULL, NULL,
   'frequency', 'ratio', 2.5, 4.0, 6.0, 100, 'industry_research',
   'Average frequency over campaign flight', '2024-01-01', NULL),

  -- ── Conversion: cross-platform ─────────────────────────────────
  ('ind-cv-ctr', 'industry', 'canadian_political', 'conversion', NULL, NULL,
   'ctr', 'percentage', 0.008, 0.012, 0.020, 120, 'industry_research',
   'Canadian political/labour conversion campaigns 2024-2026', '2024-01-01', NULL),

  ('ind-cv-cpm', 'industry', 'canadian_political', 'conversion', NULL, NULL,
   'cpm', 'currency_cad', 10.0, 15.0, 25.0, 120, 'industry_research',
   'Canadian political/labour conversion campaigns 2024-2026', '2024-01-01', NULL),

  ('ind-cv-cpc', 'industry', 'canadian_political', 'conversion', NULL, NULL,
   'cpc', 'currency_cad', 1.50, 2.50, 4.00, 120, 'industry_research',
   'Canadian political/labour conversion campaigns 2024-2026', '2024-01-01', NULL),

  ('ind-cv-cpa', 'industry', 'canadian_political', 'conversion', NULL, NULL,
   'cpa', 'currency_cad', 15.0, 30.0, 60.0, 90, 'industry_research',
   'Canadian political/labour conversion campaigns 2024-2026', '2024-01-01', NULL),

  ('ind-cv-convrate', 'industry', 'canadian_political', 'conversion', NULL, NULL,
   'conversion_rate', 'percentage', 0.015, 0.030, 0.050, 90, 'industry_research',
   'Conversion rate (conversions / clicks)', '2024-01-01', NULL),

  -- ── Labour-specific scope ──────────────────────────────────────
  ('ind-lab-aw-ctr', 'industry', 'canadian_labour', 'awareness', NULL, NULL,
   'ctr', 'percentage', 0.003, 0.006, 0.009, 60, 'industry_research',
   'Canadian labour union awareness campaigns', '2024-01-01', NULL),

  ('ind-lab-aw-cpm', 'industry', 'canadian_labour', 'awareness', NULL, NULL,
   'cpm', 'currency_cad', 7.0, 11.0, 16.0, 60, 'industry_research',
   'Canadian labour union awareness campaigns', '2024-01-01', NULL),

  ('ind-lab-cv-cpa', 'industry', 'canadian_labour', 'conversion', NULL, NULL,
   'cpa', 'currency_cad', 12.0, 25.0, 50.0, 40, 'industry_research',
   'Canadian labour union conversion campaigns (petition, signup)', '2024-01-01', NULL),

  ('ind-lab-cv-ctr', 'industry', 'canadian_labour', 'conversion', NULL, NULL,
   'ctr', 'percentage', 0.010, 0.015, 0.025, 40, 'industry_research',
   'Canadian labour union conversion campaigns', '2024-01-01', NULL),

  -- ── Issues-specific scope ──────────────────────────────────────
  ('ind-iss-aw-ctr', 'industry', 'canadian_issues', 'awareness', NULL, NULL,
   'ctr', 'percentage', 0.002, 0.004, 0.007, 50, 'industry_research',
   'Canadian issues/advocacy awareness campaigns', '2024-01-01', NULL),

  ('ind-iss-aw-cpm', 'industry', 'canadian_issues', 'awareness', NULL, NULL,
   'cpm', 'currency_cad', 9.0, 14.0, 22.0, 50, 'industry_research',
   'Canadian issues/advocacy awareness campaigns', '2024-01-01', NULL),

  ('ind-iss-cv-cpa', 'industry', 'canadian_issues', 'conversion', NULL, NULL,
   'cpa', 'currency_cad', 20.0, 40.0, 75.0, 30, 'industry_research',
   'Canadian issues/advocacy conversion campaigns', '2024-01-01', NULL),

  -- ── Platform-specific: Meta awareness ──────────────────────────
  ('ind-meta-aw-ctr', 'industry', 'canadian_political', 'awareness', 'meta', NULL,
   'ctr', 'percentage', 0.004, 0.007, 0.012, 80, 'industry_research',
   'Meta awareness campaigns — typically higher CTR than average', '2024-01-01', NULL),

  ('ind-meta-aw-cpm', 'industry', 'canadian_political', 'awareness', 'meta', NULL,
   'cpm', 'currency_cad', 6.0, 10.0, 15.0, 80, 'industry_research',
   'Meta awareness campaigns', '2024-01-01', NULL),

  -- ── Platform-specific: Meta conversion ─────────────────────────
  ('ind-meta-cv-cpa', 'industry', 'canadian_political', 'conversion', 'meta', NULL,
   'cpa', 'currency_cad', 10.0, 22.0, 45.0, 60, 'industry_research',
   'Meta conversion campaigns', '2024-01-01', NULL),

  -- ── Platform-specific: Google Ads conversion ───────────────────
  ('ind-gads-cv-cpc', 'industry', 'canadian_political', 'conversion', 'google_ads', NULL,
   'cpc', 'currency_cad', 1.00, 2.00, 3.50, 50, 'industry_research',
   'Google Ads search/display conversion campaigns', '2024-01-01', NULL),

  ('ind-gads-cv-cpa', 'industry', 'canadian_political', 'conversion', 'google_ads', NULL,
   'cpa', 'currency_cad', 18.0, 35.0, 65.0, 50, 'industry_research',
   'Google Ads conversion campaigns', '2024-01-01', NULL),

  -- ── Platform-specific: LinkedIn ────────────────────────────────
  ('ind-li-aw-cpm', 'industry', 'canadian_political', 'awareness', 'linkedin', NULL,
   'cpm', 'currency_cad', 15.0, 25.0, 40.0, 30, 'industry_research',
   'LinkedIn awareness campaigns — premium inventory', '2024-01-01', NULL),

  ('ind-li-cv-cpc', 'industry', 'canadian_political', 'conversion', 'linkedin', NULL,
   'cpc', 'currency_cad', 3.00, 5.50, 9.00, 25, 'industry_research',
   'LinkedIn conversion campaigns', '2024-01-01', NULL);
