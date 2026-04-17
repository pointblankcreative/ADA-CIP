-- ==============================================================================
-- Campaign Intelligence Platform (CIP) - Phase 1 BigQuery Schema
-- Project: point-blank-ada
-- Dataset: cip
-- Region: northamerica-northeast1 (Montreal)
-- ==============================================================================
-- This DDL creates all tables needed for Phase 1 of the CIP.
-- Uses CREATE TABLE IF NOT EXISTS for idempotency.
-- Includes partitioning and clustering where specified for performance.
-- ==============================================================================

-- Create dataset
CREATE SCHEMA IF NOT EXISTS `point-blank-ada.cip`
OPTIONS(location='northamerica-northeast1');


-- ==============================================================================
-- DIMENSION TABLES
-- ==============================================================================

-- Clients dimension table
CREATE TABLE IF NOT EXISTS `point-blank-ada.cip.dim_clients` (
  client_id STRING NOT NULL,
  client_name STRING NOT NULL,
  client_short_name STRING,
  primary_contact_name STRING,
  primary_contact_email STRING,
  currency STRING DEFAULT 'CAD',
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
OPTIONS(
  description='Dimension table for campaign clients and account information'
);


-- Projects dimension table
CREATE TABLE IF NOT EXISTS `point-blank-ada.cip.dim_projects` (
  project_code STRING NOT NULL,
  client_id STRING,
  project_name STRING NOT NULL,
  campaign_type STRING,
  start_date DATE,
  end_date DATE,
  net_budget NUMERIC,
  currency STRING DEFAULT 'CAD',
  status STRING DEFAULT 'planning',
  media_plan_sheet_id STRING,
  media_plan_tab_name STRING,  -- Preferred tab name for media plan sync
  slack_channel_id STRING,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
OPTIONS(
  description='Dimension table for projects (campaigns). project_code is YYNNN format primary key.'
);


-- Platforms dimension table
CREATE TABLE IF NOT EXISTS `point-blank-ada.cip.dim_platforms` (
  platform_id STRING NOT NULL,
  platform_name STRING NOT NULL,
  platform_type STRING NOT NULL,
  api_base_url STRING,
  lookback_days INT64 DEFAULT 3,
  data_delay_days INT64 DEFAULT 0
)
OPTIONS(
  description='Dimension table for advertising platforms (Meta, Google Ads, LinkedIn, etc.)'
);


-- Traditional vendors dimension table
CREATE TABLE IF NOT EXISTS `point-blank-ada.cip.dim_traditional_vendors` (
  vendor_id STRING NOT NULL,
  vendor_name STRING NOT NULL,
  vendor_system STRING,
  format_id STRING,
  typical_report_format STRING,
  post_report_expected BOOL DEFAULT TRUE,
  post_report_reminder_days INT64 DEFAULT 30,
  contact_name STRING,
  contact_email STRING
)
OPTIONS(
  description='Dimension table for traditional media vendors (radio, TV, streaming, etc.)'
);


-- ==============================================================================
-- MEDIA PLAN TABLES
-- ==============================================================================

-- Media plans table
CREATE TABLE IF NOT EXISTS `point-blank-ada.cip.media_plans` (
  plan_id STRING NOT NULL,
  project_code STRING NOT NULL,
  sheet_id STRING NOT NULL,
  sheet_name STRING DEFAULT 'Media Plan',
  client_name STRING,
  project_name STRING,
  start_date DATE,
  end_date DATE,
  net_budget NUMERIC,
  version INT64 DEFAULT 1,
  is_current BOOL DEFAULT TRUE,
  synced_at TIMESTAMP,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
OPTIONS(
  description='Media plans synced from Google Sheets. Records the planned allocation of budget across channels and flights.'
);


-- Media plan lines table
CREATE TABLE IF NOT EXISTS `point-blank-ada.cip.media_plan_lines` (
  line_id STRING NOT NULL,
  plan_id STRING NOT NULL,
  project_code STRING NOT NULL,
  line_code STRING,
  platform_id STRING,
  site_network STRING,
  channel_category STRING,
  flight_start DATE,
  flight_end DATE,
  objective STRING,
  audience_name STRING,
  audience_targeting STRING,
  landing_page STRING,
  pricing_model STRING,
  bid_rate NUMERIC,
  budget NUMERIC NOT NULL,
  estimated_impressions INT64,
  frequency_cap STRING,
  geo_targeting STRING,
  is_traditional BOOL DEFAULT FALSE,
  -- Form Friction Score (FFS) — collected via dashboard wizard, used by diagnostic engine
  ffs_score FLOAT64,                           -- Computed Form Friction Score (0-100)
  ffs_inputs JSON,                             -- Raw wizard inputs: {field_count, required_count, field_types, clicks_to_submit, form_position, has_autofill, is_platform_form}
  audience_type STRING,                        -- member_list | retargeting | lookalike_warm | lookalike_cold | prospecting (for audience temperature adjustment)
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
OPTIONS(
  description='Individual line items from media plans. Maps to actual campaigns via line_code.'
);


-- Blocking chart weeks table (flight activation by week)
CREATE TABLE IF NOT EXISTS `point-blank-ada.cip.blocking_chart_weeks` (
  id STRING NOT NULL,
  line_id STRING NOT NULL,
  project_code STRING NOT NULL,
  week_start DATE NOT NULL,
  is_active BOOL NOT NULL
)
OPTIONS(
  description='Blocking chart: week-by-week activation status for each media plan line.'
);


-- ==============================================================================
-- FACT TABLES
-- ==============================================================================

-- Digital media daily performance (PARTITIONED by date, CLUSTERED by project_code and platform_id)
CREATE TABLE IF NOT EXISTS `point-blank-ada.cip.fact_digital_daily` (
  date DATE NOT NULL,
  project_code STRING,
  platform_id STRING NOT NULL,
  account_id STRING,
  account_name STRING,
  campaign_id STRING NOT NULL,
  campaign_name STRING NOT NULL,
  ad_set_id STRING,
  ad_set_name STRING,
  ad_id STRING,
  ad_name STRING,
  line_code STRING,
  spend NUMERIC DEFAULT 0,
  impressions INT64 DEFAULT 0,
  clicks INT64 DEFAULT 0,
  conversions NUMERIC DEFAULT 0,
  video_views INT64 DEFAULT 0,
  video_completions INT64 DEFAULT 0,
  reach INT64 DEFAULT 0,
  frequency FLOAT64 DEFAULT 0,
  engagements INT64 DEFAULT 0,
  cpm FLOAT64,
  cpc FLOAT64,
  ctr FLOAT64,
  -- Diagnostic signal columns (added for CIP diagnostic engine)
  video_views_3s INT64 DEFAULT 0,              -- Facebook: n_3_Second_Video_Views; platform-adjusted "start" for video completion scoring
  thruplay INT64 DEFAULT 0,                    -- Facebook: Video_thruplay (15s+ or completion); also maps to video_completions for backwards compat
  video_q25 INT64 DEFAULT 0,                   -- Facebook: Video_Watches_at_25; StackAdapt: Video_completed_25
  video_q50 INT64 DEFAULT 0,                   -- Facebook: Video_Watches_at_50; StackAdapt: Video_completed_50
  video_q75 INT64 DEFAULT 0,                   -- Facebook: Video_Watches_at_75; StackAdapt: Video_completed_75
  video_q100 INT64 DEFAULT 0,                  -- Facebook: Video_Watches_at_100; StackAdapt: same as video_completions (95%)
  post_engagement INT64 DEFAULT 0,             -- Facebook: Post_Engagement (includes passive video views — diagnostic engine strips these)
  post_reactions INT64 DEFAULT 0,              -- Facebook: Post_Reactions
  post_comments INT64 DEFAULT 0,              -- Facebook: Post_Comments
  outbound_clicks INT64 DEFAULT 0,             -- Facebook: Outbound_Clicks; Pinterest: Paid_Outbound_Clicks
  landing_page_views INT64 DEFAULT 0,          -- Facebook: Landing_Page_Views
  registrations NUMERIC DEFAULT 0,             -- Facebook: Website_Registrations_Completed (mobilization/petition campaigns)
  leads NUMERIC DEFAULT 0,                     -- Facebook: Website_Leads (lead gen campaigns)
  on_platform_leads NUMERIC DEFAULT 0,         -- Facebook: On_Facebook_Leads (in-platform lead forms)
  contacts NUMERIC DEFAULT 0,                  -- Facebook: Website_Contacts
  donations NUMERIC DEFAULT 0,                 -- Facebook: Website_Donations
  campaign_objective STRING,                   -- Platform-reported objective (Facebook: Campaign_Objective; used by diagnostic for signal selection)
  viewability_measured INT64 DEFAULT 0,        -- StackAdapt: Measured_impressions
  viewability_viewed INT64 DEFAULT 0,          -- StackAdapt: Viewed_measured_impressions
  ingestion_source STRING DEFAULT 'funnel_transform',
  loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY date
CLUSTER BY project_code, platform_id
OPTIONS(
  description='Fact table for daily digital media performance. Partitioned by date for query efficiency.'
);


-- DOOH (Digital Out Of Home) daily performance (PARTITIONED by date, CLUSTERED by project_code)
CREATE TABLE IF NOT EXISTS `point-blank-ada.cip.fact_dooh_daily` (
  date DATE NOT NULL,
  project_code STRING,
  platform_id STRING DEFAULT 'perion',
  campaign_id STRING,
  campaign_name STRING,
  screen_id STRING,
  screen_name STRING,
  venue_category STRING,
  media_owner STRING,
  network_name STRING,
  city STRING,
  region STRING,
  latitude FLOAT64,
  longitude FLOAT64,
  spot_length_seconds INT64,
  plays INT64 DEFAULT 0,
  audience_impressions INT64 DEFAULT 0,
  spend NUMERIC DEFAULT 0,
  dsp_fee NUMERIC DEFAULT 0,
  cpm FLOAT64,
  pacing_vs_even FLOAT64,
  loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY date
CLUSTER BY project_code
OPTIONS(
  description='Fact table for daily DOOH (Perion) performance. Partitioned by date and clustered by project.'
);


-- ==============================================================================
-- TRADITIONAL MEDIA TABLES
-- ==============================================================================

-- Traditional media buys table
CREATE TABLE IF NOT EXISTS `point-blank-ada.cip.traditional_buys` (
  buy_id STRING NOT NULL,
  project_code STRING NOT NULL,
  vendor_id STRING,
  vendor_name STRING NOT NULL,
  contract_number STRING,
  station_call_sign STRING,
  buy_type STRING,
  start_date DATE,
  end_date DATE,
  total_spots INT64,
  net_cost NUMERIC,
  gross_cost NUMERIC,
  target_demographic STRING,
  document_id STRING,
  status STRING DEFAULT 'booked',
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
OPTIONS(
  description='Traditional media buy records (radio, linear TV, streaming, podcasts).'
);


-- Traditional buy line items table
CREATE TABLE IF NOT EXISTS `point-blank-ada.cip.traditional_buy_lines` (
  line_id STRING NOT NULL,
  buy_id STRING NOT NULL,
  description STRING,
  day_pattern STRING,
  time_start STRING,
  time_end STRING,
  spot_length_seconds INT64,
  rate NUMERIC,
  rate_type STRING,
  spots_per_week INT64,
  total_spots INT64,
  total_cost NUMERIC,
  grps FLOAT64,
  ratings FLOAT64,
  weekly_spots STRING
)
OPTIONS(
  description='Detailed line items within traditional media buys. weekly_spots is JSON mapping dates to spot counts.'
);


-- Traditional media actuals (reconciliation) table
CREATE TABLE IF NOT EXISTS `point-blank-ada.cip.traditional_actuals` (
  actual_id STRING NOT NULL,
  buy_id STRING NOT NULL,
  line_id STRING,
  actual_spots INT64,
  actual_grps FLOAT64,
  actual_cost NUMERIC,
  makegood_spots INT64,
  makegood_accepted BOOL,
  spots_variance INT64,
  cost_variance NUMERIC,
  notes STRING,
  document_id STRING,
  verified_at TIMESTAMP,
  verified_by STRING
)
OPTIONS(
  description='Post-report actuals for traditional media buys. Tracks variance from booked to delivered.'
);


-- ==============================================================================
-- OPERATIONAL TABLES
-- ==============================================================================

-- Budget tracking table (materialized view, rebuilt daily, PARTITIONED by date, CLUSTERED by project_code)
CREATE TABLE IF NOT EXISTS `point-blank-ada.cip.budget_tracking` (
  date DATE NOT NULL,
  project_code STRING NOT NULL,
  line_id STRING NOT NULL,
  line_code STRING,
  platform_id STRING,
  channel_category STRING,
  planned_budget NUMERIC,
  planned_spend_to_date NUMERIC,
  actual_spend_to_date NUMERIC,
  remaining_budget NUMERIC,
  remaining_days INT64,
  pacing_percentage FLOAT64,
  daily_budget_required NUMERIC,
  is_over_pacing BOOL,
  is_under_pacing BOOL,
  loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY date
CLUSTER BY project_code
OPTIONS(
  description='Daily budget pacing summary. Materialized view rebuilt daily after pacing calculations.'
);


-- Document registry table (for tracking uploaded files and reports)
CREATE TABLE IF NOT EXISTS `point-blank-ada.cip.document_registry` (
  document_id STRING NOT NULL,
  project_code STRING,
  file_path STRING NOT NULL,
  file_name STRING NOT NULL,
  document_type STRING,
  vendor_format STRING,
  parse_status STRING DEFAULT 'pending',
  parse_confidence FLOAT64,
  uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  parsed_at TIMESTAMP,
  verified_at TIMESTAMP,
  uploaded_by STRING
)
OPTIONS(
  description='Registry of uploaded documents (traditional buy sheets, post-reports, media plans, etc.).'
);


-- Ingestion log table (tracks data connector runs)
CREATE TABLE IF NOT EXISTS `point-blank-ada.cip.ingestion_log` (
  log_id STRING NOT NULL,
  source_platform STRING NOT NULL,
  connector_name STRING NOT NULL,
  run_started_at TIMESTAMP NOT NULL,
  run_completed_at TIMESTAMP,
  status STRING DEFAULT 'running',
  rows_fetched INT64 DEFAULT 0,
  rows_upserted INT64 DEFAULT 0,
  date_range_start DATE,
  date_range_end DATE,
  error_message STRING
)
OPTIONS(
  description='Log of data ingestion runs from platform connectors.'
);


-- Alerts table (for monitoring and escalation)
CREATE TABLE IF NOT EXISTS `point-blank-ada.cip.alerts` (
  alert_id STRING NOT NULL,
  project_code STRING,
  alert_type STRING NOT NULL,
  severity STRING NOT NULL,
  title STRING NOT NULL,
  message STRING NOT NULL,
  metadata STRING,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  acknowledged_at TIMESTAMP,
  acknowledged_by STRING,
  resolved_at TIMESTAMP,
  slack_sent BOOL DEFAULT FALSE,
  slack_channel_id STRING,
  slack_message_ts STRING
)
OPTIONS(
  description='Alert records for pacing, budget, data staleness, and operational issues.'
);


-- Campaign to project code mapping table (manual overrides for auto-mapping failures)
CREATE TABLE IF NOT EXISTS `point-blank-ada.cip.campaign_project_mapping` (
  mapping_id STRING NOT NULL,
  platform_id STRING NOT NULL,
  campaign_id STRING NOT NULL,
  campaign_name STRING,
  project_code STRING NOT NULL,
  mapped_by STRING,
  mapped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
OPTIONS(
  description='Manual campaign-to-project mappings for when auto-extraction fails.'
);


-- ==============================================================================
-- BENCHMARKS
-- ==============================================================================

CREATE TABLE IF NOT EXISTS `point-blank-ada.cip.benchmarks` (
  benchmark_id STRING NOT NULL,
  benchmark_type STRING NOT NULL,
  scope STRING NOT NULL,
  objective_type STRING NOT NULL,
  platform_id STRING,
  creative_format STRING,
  metric_name STRING NOT NULL,
  metric_unit STRING NOT NULL,
  p25 FLOAT64,
  p50 FLOAT64,
  p75 FLOAT64,
  sample_size INT64,
  source STRING,
  notes STRING,
  valid_from DATE,
  valid_to DATE,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
OPTIONS(
  description='Benchmark data for campaign performance comparison. Supports industry, client, and cross-client tiers.'
);


-- ==============================================================================
-- PROJECT GA4 URL MAPPINGS
-- ==============================================================================

CREATE TABLE IF NOT EXISTS `point-blank-ada.cip.project_ga4_urls` (
  id STRING NOT NULL,
  project_code STRING NOT NULL,
  ga4_property_id STRING NOT NULL,
  url_pattern STRING NOT NULL,
  label STRING,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  created_by STRING
)
OPTIONS(
  description='Maps projects to GA4 property/URL patterns for web analytics integration.'
);


-- ==============================================================================
-- TABLE: fact_ga4_daily
-- Daily GA4 web analytics from Funnel.io, pivoted by event type.
-- Cross-region: source data in US, this table in northamerica-northeast1.
-- ==============================================================================

CREATE TABLE IF NOT EXISTS `point-blank-ada.cip.fact_ga4_daily` (
  date DATE NOT NULL,
  ga4_property_id STRING NOT NULL,
  property_name STRING,
  session_source STRING,
  session_medium STRING,
  session_campaign STRING,
  sessions INT64 DEFAULT 0,
  page_views INT64 DEFAULT 0,
  first_visits INT64 DEFAULT 0,
  key_events FLOAT64 DEFAULT 0,
  sign_ups INT64 DEFAULT 0,
  scroll_events INT64 DEFAULT 0,
  click_events INT64 DEFAULT 0,
  form_starts INT64 DEFAULT 0,
  form_submits INT64 DEFAULT 0,
  user_engagements INT64 DEFAULT 0,
  total_event_count INT64 DEFAULT 0,
  ingestion_source STRING DEFAULT 'funnel_transform',
  loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY date
CLUSTER BY ga4_property_id, session_source
OPTIONS(
  description='Daily GA4 web analytics from Funnel.io, pivoted by event type. One row per date/property/source/medium/campaign.'
);


-- ==============================================================================
-- TABLE: fact_adset_daily
-- Reach / frequency at ad set or campaign grain (separate from fact_digital_daily ad rows).
-- E3: Dedup Strategy Documentation
--
-- This table has no unique constraint. Duplicates are possible if adset_transform runs
-- multiple times or if the transform logic encounters the same data in successive loads.
--
-- Dedup is enforced at QUERY time (not at table level) using a post-load DELETE with
-- ROW_NUMBER() OVER (PARTITION BY date, project_code, platform_id, campaign_id, ad_set_id ORDER BY loaded_at DESC).
-- This keeps only the most recent _load_timestamp row for each ad-set date combination.
-- The dedup runs as part of adset_transform after load_table_from_json completes.
--
-- Consumers should query this table directly; dedup is automatic post-load.
-- ==============================================================================

CREATE TABLE IF NOT EXISTS `point-blank-ada.cip.fact_adset_daily` (
  date DATE NOT NULL,
  project_code STRING,
  platform_id STRING NOT NULL,
  account_id STRING,
  campaign_id STRING NOT NULL,
  campaign_name STRING NOT NULL,
  ad_set_id STRING,
  ad_set_name STRING,
  reach INT64,
  frequency FLOAT64,
  reach_window STRING DEFAULT '7d',
  impressions INT64,
  video_views INT64,
  video_completions INT64,
  ingestion_source STRING DEFAULT 'funnel_transform',
  loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY date
CLUSTER BY project_code, platform_id
OPTIONS(
  description='Daily reach/frequency from Funnel.io at ad-set or campaign grain. Not additive across rows. Dedup enforced post-load via ROW_NUMBER().'
);


-- ==============================================================================
-- SEED DATA: Platform Dimension
-- ==============================================================================

-- Insert standard platform configurations
INSERT INTO `point-blank-ada.cip.dim_platforms` (platform_id, platform_name, platform_type, api_base_url, lookback_days, data_delay_days)
VALUES
  ('meta', 'Meta (Facebook/Instagram)', 'digital', 'https://graph.instagram.com', 3, 0),
  ('google_ads', 'Google Ads', 'digital', 'https://googleads.googleapis.com', 3, 1),
  ('linkedin', 'LinkedIn', 'digital', 'https://api.linkedin.com', 7, 1),
  ('stackadapt', 'StackAdapt', 'digital', 'https://api.stackadapt.com', 3, 1),
  ('tiktok', 'TikTok', 'digital', 'https://business-api.tiktok.com', 3, 1),
  ('snapchat', 'Snapchat', 'digital', 'https://adsapi.snapchat.com', 3, 2),
  ('perion', 'Perion DOOH', 'dooh', NULL, 1, 1);

-- ==============================================================================
-- CREATIVE VARIANT ALIASES — manual overrides for grouping ads by creative
-- ==============================================================================

CREATE TABLE IF NOT EXISTS `point-blank-ada.cip.creative_variant_aliases` (
  alias_id STRING NOT NULL,
  project_code STRING NOT NULL,
  ad_name_pattern STRING NOT NULL,
  platform_id STRING,
  creative_variant STRING NOT NULL,
  created_by STRING,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
OPTIONS(
  description='Manual overrides for grouping ads by creative variant name. ad_name_pattern can be exact or SQL LIKE pattern.'
);

-- ==============================================================================
-- DIAGNOSTIC ENGINE TABLES
-- ==============================================================================

-- Diagnostic signal results (daily per-campaign health scores)
CREATE TABLE IF NOT EXISTS `point-blank-ada.cip.fact_diagnostic_signals` (
  id STRING NOT NULL,                          -- UUID
  project_code STRING NOT NULL,
  campaign_type STRING NOT NULL,               -- 'persuasion' | 'conversion'
  evaluation_date DATE NOT NULL,               -- date this diagnostic covers
  flight_day INT64,                            -- day N of flight (1-indexed)
  flight_total_days INT64,                     -- total flight length

  -- Level 1: Health Score
  health_score FLOAT64,                        -- 0-100 or NULL
  health_status STRING,                        -- STRONG | WATCH | ACTION | NULL

  -- Level 2: Pillar Scores (JSON for flexibility across campaign types)
  -- Persuasion: {"distribution": {"score": 81, "status": "STRONG"}, "attention": {...}, "resonance": {...}}
  -- Conversion: {"acquisition": {...}, "funnel": {...}, "quality": {...}}
  pillars JSON,

  -- Level 3: Individual Signals (JSON array)
  -- [{"id": "D1", "name": "Reach Attainment", "score": 100, "status": "STRONG",
  --   "raw_value": 2.22, "benchmark": 1.0, "floor": 0.5,
  --   "diagnostic": "Reach at 2.2x pacing...", "guard_passed": true,
  --   "guard_reason": null, "inputs": {"actual_reach": 39098, "pro_rated_reach": 17631}}]
  signals JSON,

  -- Level 4: Efficiency Layer
  -- {"cpm": 11.74, "cpc": 8.91, "cpa": null, "cpcv": 0.29, "pacing_pct": 87}
  efficiency JSON,

  -- Critical Alerts fired this evaluation
  -- [{"type": "zero_conversion_24h", "severity": "critical", "message": "..."}]
  alerts JSON,

  -- Metadata
  platforms JSON,                              -- ["facebook", "stackadapt"]
  line_ids JSON,                               -- media plan line IDs included
  computed_at TIMESTAMP NOT NULL,
  spec_version STRING NOT NULL                 -- "1.1"
)
PARTITION BY evaluation_date
CLUSTER BY project_code, campaign_type
OPTIONS(
  description='Daily diagnostic signal results from the CIP diagnostic engine. One row per project/campaign_type/date.'
);


-- ==============================================================================
-- END OF SCHEMA DEFINITION
-- ==============================================================================
