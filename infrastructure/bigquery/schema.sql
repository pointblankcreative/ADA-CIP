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
-- END OF SCHEMA DEFINITION
-- ==============================================================================
