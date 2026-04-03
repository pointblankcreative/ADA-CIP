-- ==============================================================================
-- SEED: Industry & Cross-Client Benchmarks
--
-- Data sources:
--   Industry: 2025-2026 published benchmarks (WordStream, WebFX, AdAmigo,
--             Lebesgue) adjusted for Canadian market and political/advocacy vertical
--   Cross-client: Computed from point-blank-ada.cip.fact_digital_daily on 2026-04-02
--
-- Run this script to reset benchmarks to known-good values.
-- It is idempotent — safe to re-run.
-- ==============================================================================

-- Clear existing industry + cross-client benchmarks
DELETE FROM `point-blank-ada.cip.benchmarks`
WHERE benchmark_type IN ('industry', 'cross_client');

INSERT INTO `point-blank-ada.cip.benchmarks`
  (benchmark_id, benchmark_type, scope, objective_type, platform_id, creative_format,
   metric_name, metric_unit, p25, p50, p75, sample_size, source, notes, valid_from, valid_to)
VALUES

  -- ═══════════════════════════════════════════════════════════════════════════
  -- TIER 1: INDUSTRY BENCHMARKS — Cross-platform defaults
  -- These are used by the frontend KPI cards (platform_id = NULL)
  -- ═══════════════════════════════════════════════════════════════════════════

  -- Awareness
  ('ind_xplat_awr_ctr', 'industry', 'canadian_political', 'awareness', NULL, NULL,
   'ctr', 'percentage', 0.003, 0.005, 0.008, NULL, 'industry_research',
   'Cross-platform default; based on Meta as primary platform', '2025-01-01', NULL),
  ('ind_xplat_awr_cpm', 'industry', 'canadian_political', 'awareness', NULL, NULL,
   'cpm', 'currency_cad', 6.0, 9.0, 14.0, NULL, 'industry_research',
   'Cross-platform default; Canada CPM ~$14 general, awareness trends lower', '2025-01-01', NULL),
  ('ind_xplat_awr_cpc', 'industry', 'canadian_political', 'awareness', NULL, NULL,
   'cpc', 'currency_cad', 1.00, 2.00, 4.00, NULL, 'industry_research',
   'Awareness CPC higher due to lower CTR', '2025-01-01', NULL),
  ('ind_xplat_awr_vcr', 'industry', 'canadian_political', 'awareness', NULL, NULL,
   'vcr', 'percentage', 0.40, 0.55, 0.70, NULL, 'industry_research',
   'Video completion rate; dedicated video view campaigns', '2025-01-01', NULL),
  ('ind_xplat_awr_frequency', 'industry', 'canadian_political', 'awareness', NULL, NULL,
   'frequency', 'ratio', 1.5, 2.5, 4.0, NULL, 'industry_research',
   'Awareness campaigns aim for higher frequency', '2025-01-01', NULL),

  -- Conversion
  ('ind_xplat_conv_ctr', 'industry', 'canadian_political', 'conversion', NULL, NULL,
   'ctr', 'percentage', 0.008, 0.012, 0.020, NULL, 'industry_research',
   'Conversion campaigns: higher CTR from intent-based targeting', '2025-01-01', NULL),
  ('ind_xplat_conv_cpm', 'industry', 'canadian_political', 'conversion', NULL, NULL,
   'cpm', 'currency_cad', 12.0, 18.0, 28.0, NULL, 'industry_research',
   'Conversion CPM premium vs awareness; Canada-adjusted', '2025-01-01', NULL),
  ('ind_xplat_conv_cpc', 'industry', 'canadian_political', 'conversion', NULL, NULL,
   'cpc', 'currency_cad', 0.80, 1.50, 2.50, NULL, 'industry_research',
   'Traffic/conversion campaigns typically lower CPC', '2025-01-01', NULL),
  ('ind_xplat_conv_cpa', 'industry', 'canadian_political', 'conversion', NULL, NULL,
   'cpa', 'currency_cad', 8.0, 20.0, 40.0, NULL, 'industry_research',
   'Political/advocacy CPA; petition/signup conversions', '2025-01-01', NULL),
  ('ind_xplat_conv_convrate', 'industry', 'canadian_political', 'conversion', NULL, NULL,
   'conversion_rate', 'percentage', 0.02, 0.05, 0.10, NULL, 'industry_research',
   'Landing page conversion rate', '2025-01-01', NULL),
  ('ind_xplat_conv_frequency', 'industry', 'canadian_political', 'conversion', NULL, NULL,
   'frequency', 'ratio', 1.0, 1.8, 3.0, NULL, 'industry_research',
   'Conversion campaigns typically lower frequency', '2025-01-01', NULL),

  -- ═══════════════════════════════════════════════════════════════════════════
  -- TIER 1: INDUSTRY BENCHMARKS — Meta-specific
  -- ═══════════════════════════════════════════════════════════════════════════

  -- Meta Awareness
  ('ind_meta_awr_ctr', 'industry', 'canadian_political', 'awareness', 'meta', NULL,
   'ctr', 'percentage', 0.003, 0.005, 0.008, NULL, 'industry_research',
   '2025-2026 industry reports adjusted for Canadian political/advocacy', '2025-01-01', NULL),
  ('ind_meta_awr_cpm', 'industry', 'canadian_political', 'awareness', 'meta', NULL,
   'cpm', 'currency_cad', 6.0, 9.0, 14.0, NULL, 'industry_research',
   'Canada CPM ~$14 general; awareness campaigns trend lower', '2025-01-01', NULL),
  ('ind_meta_awr_cpc', 'industry', 'canadian_political', 'awareness', 'meta', NULL,
   'cpc', 'currency_cad', 1.00, 2.00, 4.00, NULL, 'industry_research',
   'Awareness CPC higher due to lower CTR', '2025-01-01', NULL),
  ('ind_meta_awr_vcr', 'industry', 'canadian_political', 'awareness', 'meta', NULL,
   'vcr', 'percentage', 0.40, 0.55, 0.70, NULL, 'industry_research',
   'Video view campaign completions; varies widely by video length', '2025-01-01', NULL),

  -- Meta Conversion
  ('ind_meta_conv_ctr', 'industry', 'canadian_political', 'conversion', 'meta', NULL,
   'ctr', 'percentage', 0.008, 0.012, 0.020, NULL, 'industry_research',
   'Conversion campaigns: higher CTR due to intent-based targeting', '2025-01-01', NULL),
  ('ind_meta_conv_cpm', 'industry', 'canadian_political', 'conversion', 'meta', NULL,
   'cpm', 'currency_cad', 12.0, 18.0, 28.0, NULL, 'industry_research',
   'Conversion CPM premium vs awareness; Canada-adjusted', '2025-01-01', NULL),
  ('ind_meta_conv_cpc', 'industry', 'canadian_political', 'conversion', 'meta', NULL,
   'cpc', 'currency_cad', 0.80, 1.50, 2.50, NULL, 'industry_research',
   'Traffic/conversion campaigns typically lower CPC', '2025-01-01', NULL),
  ('ind_meta_conv_cpa', 'industry', 'canadian_political', 'conversion', 'meta', NULL,
   'cpa', 'currency_cad', 8.0, 20.0, 40.0, NULL, 'industry_research',
   'Political/advocacy CPA lower than ecommerce; petition/signup conversions', '2025-01-01', NULL),
  ('ind_meta_conv_convrate', 'industry', 'canadian_political', 'conversion', 'meta', NULL,
   'conversion_rate', 'percentage', 0.02, 0.05, 0.10, NULL, 'industry_research',
   'Landing page conversion rate; political signup forms tend higher', '2025-01-01', NULL),

  -- ═══════════════════════════════════════════════════════════════════════════
  -- TIER 3: CROSS-CLIENT BENCHMARKS — Meta, all PB clients
  -- Source: point-blank-ada.cip.fact_digital_daily, computed 2026-04-02
  -- 45 campaigns total (12 awareness, 16 conversion), >$50 spend each
  -- ═══════════════════════════════════════════════════════════════════════════

  -- Awareness (n=12 campaigns)
  ('xc_meta_awr_ctr', 'cross_client', 'all_clients', 'awareness', 'meta', NULL,
   'ctr', 'percentage', 0.0014, 0.0043, 0.0065, 12, 'cross_client',
   'PB historical Meta awareness campaigns, computed 2026-04-02', '2026-04-02', NULL),
  ('xc_meta_awr_cpm', 'cross_client', 'all_clients', 'awareness', 'meta', NULL,
   'cpm', 'currency_cad', 7.38, 8.75, 11.68, 12, 'cross_client',
   'PB historical Meta awareness campaigns, computed 2026-04-02', '2026-04-02', NULL),
  ('xc_meta_awr_cpc', 'cross_client', 'all_clients', 'awareness', 'meta', NULL,
   'cpc', 'currency_cad', 1.33, 3.14, 3.28, 12, 'cross_client',
   'PB historical Meta awareness campaigns, computed 2026-04-02', '2026-04-02', NULL),
  ('xc_meta_awr_vcr', 'cross_client', 'all_clients', 'awareness', 'meta', NULL,
   'vcr', 'percentage', 0.053, 0.084, 0.240, 12, 'cross_client',
   'PB historical; lower than industry — may include non-video campaigns', '2026-04-02', NULL),

  -- Conversion (n=16 campaigns)
  ('xc_meta_conv_ctr', 'cross_client', 'all_clients', 'conversion', 'meta', NULL,
   'ctr', 'percentage', 0.0070, 0.0131, 0.0156, 16, 'cross_client',
   'PB historical Meta conversion campaigns, computed 2026-04-02', '2026-04-02', NULL),
  ('xc_meta_conv_cpm', 'cross_client', 'all_clients', 'conversion', 'meta', NULL,
   'cpm', 'currency_cad', 13.84, 18.32, 27.93, 16, 'cross_client',
   'PB historical Meta conversion campaigns, computed 2026-04-02', '2026-04-02', NULL),
  ('xc_meta_conv_cpc', 'cross_client', 'all_clients', 'conversion', 'meta', NULL,
   'cpc', 'currency_cad', 1.20, 1.88, 2.31, 16, 'cross_client',
   'PB historical Meta conversion campaigns, computed 2026-04-02', '2026-04-02', NULL),
  ('xc_meta_conv_cpa', 'cross_client', 'all_clients', 'conversion', 'meta', NULL,
   'cpa', 'currency_cad', 3.40, 9.29, 13.34, 16, 'cross_client',
   'PB historical Meta conversion campaigns, computed 2026-04-02', '2026-04-02', NULL),
  ('xc_meta_conv_convrate', 'cross_client', 'all_clients', 'conversion', 'meta', NULL,
   'conversion_rate', 'percentage', 0.146, 0.224, 0.276, 16, 'cross_client',
   'PB historical; higher than industry due to petition/signup conversions', '2026-04-02', NULL);
