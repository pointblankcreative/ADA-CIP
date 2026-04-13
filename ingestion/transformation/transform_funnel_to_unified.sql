-- =============================================================================
-- FUNNEL DATA TO UNIFIED FACT_DIGITAL_DAILY TRANSFORMATION
-- =============================================================================
-- Purpose: Transform platform-specific data from core_funnel_export.funnel_data
--          into normalized digital_daily facts in point-blank-ada.cip
--
-- Source:  point-blank-ada.core_funnel_export.funnel_data (US region)
--          - 1,463 columns with platform-specific suffixes
--          - Each row belongs to exactly one platform (identified by non-null cols)
--
-- Target:  point-blank-ada.cip.fact_digital_daily (Montreal region)
--          - Normalized fact table with unified metric definitions
--
-- Logic:   1. Identify platform for each row
--          2. Map platform-specific columns to normalized schema
--          3. Extract project_code from campaign names (regex)
--          4. Calculate derived metrics (CPM, CPC, CTR) safely
--          5. UPSERT via MERGE for idempotency
--
-- Key Mappings:
--   - Impressions, clicks, spend, video_views, conversions
--   - Platform-specific suffixes: __Facebook_Ads, __Google_Ads, __StackAdapt,
--                                 __TikTok, __Snapchat, __LinkedIn,
--                                 __Reddit, __Pinterest
--   - Each platform provides different column availability
--
-- MERGE Key: date + platform_id + campaign_id + COALESCE(ad_set_id, '')
--            + COALESCE(ad_id, '')
--
-- Filter:   Date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
--           Only non-null platform identifiers (campaign_name or campaign_id)
--
-- Run Modes:
--   - DAILY:        Processes last 7 days (default)
--   - FULL HISTORY: Use transform_funnel_to_unified_full_history.sql
--
-- =============================================================================

WITH platform_data AS (
  -- =========================================================================
  -- META FACEBOOK ADS
  -- =========================================================================
  SELECT
    CAST(Date AS DATE) AS date,
    'meta' AS platform_id,
    Campaign_ID__Facebook_Ads AS campaign_id,
    Campaign_Name__Facebook_Ads AS campaign_name,
    Ad_Set_ID__Facebook_Ads AS ad_set_id,
    Ad_Set_Name__Facebook_Ads AS ad_set_name,
    Ad_ID__Facebook_Ads AS ad_id,
    Ad_Name__Facebook_Ads AS ad_name,
    Ad_Account_ID__Facebook_Ads AS account_id,
    Ad_Account_Name__Facebook_Ads AS account_name,
    CAST(Amount_Spent__Facebook_Ads AS NUMERIC) AS spend,
    CAST(Impressions__Facebook_Ads AS INT64) AS impressions,
    CAST(Link_Clicks__Facebook_Ads AS INT64) AS clicks,
    CAST(Reach___7_Day_Ad_Set__Facebook_Ads AS INT64) AS reach,
    CAST(Frequency___7_Day_Ad_Set__Facebook_Ads AS FLOAT64) AS frequency,
    CAST(Video_Plays__Facebook_Ads AS INT64) AS video_views,
    CAST(Video_thruplay__Facebook_Ads AS INT64) AS video_completions,
    -- Only count conversions for conversion-objective campaigns; zero for awareness/reach campaigns
    -- even if result_value is populated. COALESCE handles both non-conversion rows and NULL values.
    COALESCE(
      CASE WHEN Campaign_Objective__Facebook_Ads IN (
             'CONVERSIONS', 'OUTCOME_LEADS', 'LEAD_GENERATION'
           )
           THEN CAST(Campaign_Result_value__Facebook_Ads AS NUMERIC)
           ELSE 0
      END,
      0
    ) AS conversions,
    CAST(Clicks_all__Facebook_Ads AS INT64) AS engagements,
    -- Diagnostic signal columns
    CAST(n_3_Second_Video_Views__Facebook_Ads AS INT64) AS video_views_3s,
    CAST(Video_thruplay__Facebook_Ads AS INT64) AS thruplay,
    CAST(Video_Watches_at_25__Facebook_Ads AS INT64) AS video_q25,
    CAST(Video_Watches_at_50__Facebook_Ads AS INT64) AS video_q50,
    CAST(Video_Watches_at_75__Facebook_Ads AS INT64) AS video_q75,
    CAST(Video_Watches_at_100__Facebook_Ads AS INT64) AS video_q100,
    CAST(Post_Engagement__Facebook_Ads AS INT64) AS post_engagement,
    CAST(Post_Reactions__Facebook_Ads AS INT64) AS post_reactions,
    CAST(Post_Comments__Facebook_Ads AS INT64) AS post_comments,
    CAST(Outbound_Clicks__Facebook_Ads AS INT64) AS outbound_clicks,
    CAST(Landing_Page_Views__Facebook_Ads AS INT64) AS landing_page_views,
    CAST(Website_Registrations_Completed__Facebook_Ads AS NUMERIC) AS registrations,
    CAST(Website_Leads__Facebook_Ads AS NUMERIC) AS leads,
    CAST(On_Facebook_Leads__Facebook_Ads AS NUMERIC) AS on_platform_leads,
    CAST(Website_Contacts__Facebook_Ads AS NUMERIC) AS contacts,
    CAST(Website_Donations__Facebook_Ads AS NUMERIC) AS donations,
    Campaign_Objective__Facebook_Ads AS campaign_objective,
    CAST(NULL AS INT64) AS viewability_measured,
    CAST(NULL AS INT64) AS viewability_viewed
  FROM
    `point-blank-ada.core_funnel_export.funnel_data`
  WHERE
    Date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
    AND Campaign_ID__Facebook_Ads IS NOT NULL
    AND Campaign_Name__Facebook_Ads IS NOT NULL
    AND Ad_ID__Facebook_Ads IS NOT NULL

  UNION ALL

  -- =========================================================================
  -- GOOGLE ADS (aggregated across network segments: YOUTUBE, CONTENT, SEARCH)
  -- =========================================================================
  SELECT
    date, platform_id, campaign_id, campaign_name, ad_set_id, ad_set_name,
    ad_id, ad_name, account_id, account_name,
    SUM(spend) AS spend,
    SUM(impressions) AS impressions,
    SUM(clicks) AS clicks,
    CAST(NULL AS INT64) AS reach,
    CAST(NULL AS FLOAT64) AS frequency,
    SUM(video_views) AS video_views,
    SUM(video_completions) AS video_completions,
    SUM(conversions) AS conversions,
    SUM(engagements) AS engagements,
    -- Diagnostic signal columns (Google Ads: limited availability)
    CAST(NULL AS INT64) AS video_views_3s,
    CAST(NULL AS INT64) AS thruplay,
    CAST(NULL AS INT64) AS video_q25,
    CAST(NULL AS INT64) AS video_q50,
    CAST(NULL AS INT64) AS video_q75,
    CAST(NULL AS INT64) AS video_q100,
    CAST(NULL AS INT64) AS post_engagement,
    CAST(NULL AS INT64) AS post_reactions,
    CAST(NULL AS INT64) AS post_comments,
    CAST(NULL AS INT64) AS outbound_clicks,
    CAST(NULL AS INT64) AS landing_page_views,
    SUM(conversions) AS registrations,          -- Google Ads: all conversions mapped generically
    CAST(0 AS NUMERIC) AS leads,
    CAST(0 AS NUMERIC) AS on_platform_leads,
    CAST(0 AS NUMERIC) AS contacts,
    CAST(0 AS NUMERIC) AS donations,
    CAST(NULL AS STRING) AS campaign_objective,
    CAST(NULL AS INT64) AS viewability_measured,
    CAST(NULL AS INT64) AS viewability_viewed
  FROM (
    SELECT
      CAST(Date AS DATE) AS date,
      'google_ads' AS platform_id,
      Campaign_ID__Google_Ads AS campaign_id,
      Campaign__Google_Ads AS campaign_name,
      Ad_Group_ID__Google_Ads AS ad_set_id,
      Ad_Group_Name__Google_Ads AS ad_set_name,
      Ad_ID__Google_Ads AS ad_id,
      Ad_Name__Google_Ads AS ad_name,
      Ad_Account_Customer_ID__Google_Ads AS account_id,
      Ad_Account_Name__Google_Ads AS account_name,
      CAST(Cost__Google_Ads AS NUMERIC) AS spend,
      CAST(Impressions__Google_Ads AS INT64) AS impressions,
      CAST(Clicks__Google_Ads AS INT64) AS clicks,
      CAST(Video_views__Google_Ads AS INT64) AS video_views,
      CAST(TrueView_Views__Google_Ads AS INT64) AS video_completions,
      CAST(Conversions__Google_Ads AS NUMERIC) AS conversions,
      CAST(Engagements__Google_Ads AS INT64) AS engagements
    FROM
      `point-blank-ada.core_funnel_export.funnel_data`
    WHERE
      Date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
      AND Campaign_ID__Google_Ads IS NOT NULL
      AND Campaign__Google_Ads IS NOT NULL
      AND Ad_ID__Google_Ads IS NOT NULL
  )
  GROUP BY date, platform_id, campaign_id, campaign_name, ad_set_id, ad_set_name,
           ad_id, ad_name, account_id, account_name

  UNION ALL

  -- =========================================================================
  -- STACKADAPT
  -- =========================================================================
  SELECT
    CAST(Date AS DATE) AS date,
    'stackadapt' AS platform_id,
    Campaign_ID__StackAdapt AS campaign_id,
    Campaign__StackAdapt AS campaign_name,
    Campaign_group_ID__StackAdapt AS ad_set_id,
    Campaign_group__StackAdapt AS ad_set_name,
    Creative_ID__StackAdapt AS ad_id,
    Creative__StackAdapt AS ad_name,
    Advertiser_ID__StackAdapt AS account_id,
    Advertiser_name__StackAdapt AS account_name,
    CAST(Cost__StackAdapt AS NUMERIC) AS spend,
    CAST(Impressions__StackAdapt AS INT64) AS impressions,
    CAST(Clicks__StackAdapt AS INT64) AS clicks,
    CAST(Unique_impressions_1_Day_Creative__StackAdapt AS INT64) AS reach,
    CAST(Frequency_1_Day_Creative__StackAdapt AS FLOAT64) AS frequency,
    CAST(Video_started__StackAdapt AS INT64) AS video_views,
    CAST(Video_completed_95__StackAdapt AS INT64) AS video_completions,
    CAST(Conversions__StackAdapt AS NUMERIC) AS conversions,
    CAST(0 AS INT64) AS engagements,
    -- Diagnostic signal columns (StackAdapt: video quartiles + viewability available)
    CAST(Video_started__StackAdapt AS INT64) AS video_views_3s,  -- CTV: intentional start = meaningful start
    CAST(Video_completed_95__StackAdapt AS INT64) AS thruplay,   -- 95% completion = thruplay equivalent
    CAST(Video_completed_25__StackAdapt AS INT64) AS video_q25,
    CAST(Video_completed_50__StackAdapt AS INT64) AS video_q50,
    CAST(Video_completed_75__StackAdapt AS INT64) AS video_q75,
    CAST(Video_completed_95__StackAdapt AS INT64) AS video_q100, -- SA reports 95%, not 100%
    CAST(NULL AS INT64) AS post_engagement,
    CAST(NULL AS INT64) AS post_reactions,
    CAST(NULL AS INT64) AS post_comments,
    CAST(NULL AS INT64) AS outbound_clicks,
    CAST(NULL AS INT64) AS landing_page_views,
    CAST(0 AS NUMERIC) AS registrations,
    CAST(0 AS NUMERIC) AS leads,
    CAST(0 AS NUMERIC) AS on_platform_leads,
    CAST(0 AS NUMERIC) AS contacts,
    CAST(0 AS NUMERIC) AS donations,
    CAST(NULL AS STRING) AS campaign_objective,
    CAST(Measured_impressions__StackAdapt AS INT64) AS viewability_measured,
    CAST(Viewed_measured_impressions__StackAdapt AS INT64) AS viewability_viewed
  FROM
    `point-blank-ada.core_funnel_export.funnel_data`
  WHERE
    Date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
    AND Campaign_ID__StackAdapt IS NOT NULL
    AND Campaign__StackAdapt IS NOT NULL

  UNION ALL

  -- =========================================================================
  -- TIKTOK
  -- =========================================================================
  SELECT
    CAST(Date AS DATE) AS date,
    'tiktok' AS platform_id,
    Campaign_ID__TikTok AS campaign_id,
    Campaign_name__TikTok AS campaign_name,
    Adgroup_ID__TikTok AS ad_set_id,
    Adgroup_name__TikTok AS ad_set_name,
    Ad_ID__TikTok AS ad_id,
    Ad_name__TikTok AS ad_name,
    Advertiser_ID__TikTok AS account_id,
    Advertiser_name__TikTok AS account_name,
    CAST(Total_cost__TikTok AS NUMERIC) AS spend,
    CAST(Impressions__TikTok AS INT64) AS impressions,
    CAST(Clicks_Destination__TikTok AS INT64) AS clicks,
    CAST(Reach___7_Day_Adgroup__TikTok AS INT64) AS reach,
    CAST(Frequency___7_Day_Adgroup__TikTok AS FLOAT64) AS frequency,
    CAST(NULL AS INT64) AS video_views,
    CAST(NULL AS INT64) AS video_completions,
    CAST(Conversions__TikTok AS NUMERIC) AS conversions,
    CAST(Clicks_All__TikTok AS INT64) AS engagements,
    -- Diagnostic signal columns (TikTok: TODO extend when Funnel.io columns confirmed)
    CAST(NULL AS INT64) AS video_views_3s,
    CAST(NULL AS INT64) AS thruplay,
    CAST(NULL AS INT64) AS video_q25,
    CAST(NULL AS INT64) AS video_q50,
    CAST(NULL AS INT64) AS video_q75,
    CAST(NULL AS INT64) AS video_q100,
    CAST(NULL AS INT64) AS post_engagement,
    CAST(NULL AS INT64) AS post_reactions,
    CAST(NULL AS INT64) AS post_comments,
    CAST(NULL AS INT64) AS outbound_clicks,
    CAST(NULL AS INT64) AS landing_page_views,
    CAST(0 AS NUMERIC) AS registrations,
    CAST(0 AS NUMERIC) AS leads,
    CAST(0 AS NUMERIC) AS on_platform_leads,
    CAST(0 AS NUMERIC) AS contacts,
    CAST(0 AS NUMERIC) AS donations,
    CAST(NULL AS STRING) AS campaign_objective,
    CAST(NULL AS INT64) AS viewability_measured,
    CAST(NULL AS INT64) AS viewability_viewed
  FROM
    `point-blank-ada.core_funnel_export.funnel_data`
  WHERE
    Date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
    AND Campaign_ID__TikTok IS NOT NULL
    AND Campaign_name__TikTok IS NOT NULL
    AND Ad_ID__TikTok IS NOT NULL

  UNION ALL

  -- =========================================================================
  -- SNAPCHAT
  -- =========================================================================
  SELECT
    CAST(Date AS DATE) AS date,
    'snapchat' AS platform_id,
    Campaign_ID__Snapchat AS campaign_id,
    Campaign_Name__Snapchat AS campaign_name,
    Squad_ID__Snapchat AS ad_set_id,
    Squad_Name__Snapchat AS ad_set_name,
    Ad_ID__Snapchat AS ad_id,
    Ad_Name__Snapchat AS ad_name,
    Account_ID__Snapchat AS account_id,
    Ad_account_name__Snapchat AS account_name,
    CAST(Spend__Snapchat AS NUMERIC) AS spend,
    CAST(Impressions__Snapchat AS INT64) AS impressions,
    CAST(Swipes__Snapchat AS INT64) AS clicks,
    CAST(Reach___7_Day_Campaign__Snapchat AS INT64) AS reach,
    CAST(Frequency___7_Day_Campaign__Snapchat AS FLOAT64) AS frequency,
    CAST(Video_Views_time_based__Snapchat AS INT64) AS video_views,
    CAST(NULL AS INT64) AS video_completions,
    CAST(Leads__Snapchat AS NUMERIC) AS conversions,
    CAST(0 AS INT64) AS engagements,
    -- Diagnostic signal columns (Snapchat: TODO extend when Funnel.io columns confirmed)
    CAST(NULL AS INT64) AS video_views_3s,
    CAST(NULL AS INT64) AS thruplay,
    CAST(NULL AS INT64) AS video_q25,
    CAST(NULL AS INT64) AS video_q50,
    CAST(NULL AS INT64) AS video_q75,
    CAST(NULL AS INT64) AS video_q100,
    CAST(NULL AS INT64) AS post_engagement,
    CAST(NULL AS INT64) AS post_reactions,
    CAST(NULL AS INT64) AS post_comments,
    CAST(NULL AS INT64) AS outbound_clicks,
    CAST(NULL AS INT64) AS landing_page_views,
    CAST(0 AS NUMERIC) AS registrations,
    CAST(Leads__Snapchat AS NUMERIC) AS leads,  -- Snapchat reports leads natively
    CAST(0 AS NUMERIC) AS on_platform_leads,
    CAST(0 AS NUMERIC) AS contacts,
    CAST(0 AS NUMERIC) AS donations,
    CAST(NULL AS STRING) AS campaign_objective,
    CAST(NULL AS INT64) AS viewability_measured,
    CAST(NULL AS INT64) AS viewability_viewed
  FROM
    `point-blank-ada.core_funnel_export.funnel_data`
  WHERE
    Date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
    AND Campaign_ID__Snapchat IS NOT NULL
    AND Campaign_Name__Snapchat IS NOT NULL
    AND Ad_ID__Snapchat IS NOT NULL

  UNION ALL

  -- =========================================================================
  -- LINKEDIN
  -- =========================================================================
  SELECT
    CAST(Date AS DATE) AS date,
    'linkedin' AS platform_id,
    Campaign_ID__LinkedIn AS campaign_id,
    Campaign__LinkedIn AS campaign_name,
    Campaign_Group_ID__LinkedIn AS ad_set_id,
    Campaign_Group__LinkedIn AS ad_set_name,
    Creative_ID__LinkedIn AS ad_id,
    Creative_Name__LinkedIn AS ad_name,
    CAST(NULL AS STRING) AS account_id,
    CAST(NULL AS STRING) AS account_name,
    CAST(Spend__LinkedIn AS NUMERIC) AS spend,
    CAST(Impressions__LinkedIn AS INT64) AS impressions,
    CAST(Clicks__LinkedIn AS INT64) AS clicks,
    CAST(NULL AS INT64) AS reach,
    CAST(NULL AS FLOAT64) AS frequency,
    CAST(NULL AS INT64) AS video_views,
    CAST(NULL AS INT64) AS video_completions,
    CAST(Conversions__LinkedIn AS NUMERIC) AS conversions,
    CAST(Action_Clicks__LinkedIn AS INT64) AS engagements,
    -- Diagnostic signal columns (LinkedIn: TODO extend when Funnel.io columns confirmed)
    CAST(NULL AS INT64) AS video_views_3s,
    CAST(NULL AS INT64) AS thruplay,
    CAST(NULL AS INT64) AS video_q25,
    CAST(NULL AS INT64) AS video_q50,
    CAST(NULL AS INT64) AS video_q75,
    CAST(NULL AS INT64) AS video_q100,
    CAST(NULL AS INT64) AS post_engagement,
    CAST(NULL AS INT64) AS post_reactions,
    CAST(NULL AS INT64) AS post_comments,
    CAST(NULL AS INT64) AS outbound_clicks,
    CAST(NULL AS INT64) AS landing_page_views,
    CAST(0 AS NUMERIC) AS registrations,
    CAST(Conversions__LinkedIn AS NUMERIC) AS leads,  -- LinkedIn lead gen
    CAST(0 AS NUMERIC) AS on_platform_leads,
    CAST(0 AS NUMERIC) AS contacts,
    CAST(0 AS NUMERIC) AS donations,
    CAST(NULL AS STRING) AS campaign_objective,
    CAST(NULL AS INT64) AS viewability_measured,
    CAST(NULL AS INT64) AS viewability_viewed
  FROM
    `point-blank-ada.core_funnel_export.funnel_data`
  WHERE
    Date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
    AND Campaign_ID__LinkedIn IS NOT NULL
    AND Campaign__LinkedIn IS NOT NULL
    AND Creative_ID__LinkedIn IS NOT NULL

  UNION ALL

  -- =========================================================================
  -- REDDIT
  -- =========================================================================
  SELECT
    CAST(Date AS DATE) AS date,
    'reddit' AS platform_id,
    Campaign_ID__Reddit AS campaign_id,
    Campaign_Name__Reddit AS campaign_name,
    Ad_Group_ID__Reddit AS ad_set_id,
    Ad_Group_Name__Reddit AS ad_set_name,
    Ad_ID__Reddit AS ad_id,
    Ad_Name__Reddit AS ad_name,
    Account_ID__Reddit AS account_id,
    Account_Name__Reddit AS account_name,
    CAST(Cost__Reddit AS NUMERIC) AS spend,
    CAST(Impressions__Reddit AS INT64) AS impressions,
    CAST(Clicks__Reddit AS INT64) AS clicks,
    CAST(NULL AS INT64) AS reach,
    CAST(NULL AS FLOAT64) AS frequency,
    CAST(Video_Starts__Reddit AS INT64) AS video_views,
    CAST(Video_Watches_100__Reddit AS INT64) AS video_completions,
    CAST(Key_Conversion_Total_Count__Reddit AS NUMERIC) AS conversions,
    CAST(NULL AS INT64) AS engagements,
    -- Diagnostic signal columns (Reddit: limited availability)
    CAST(NULL AS INT64) AS video_views_3s,
    CAST(NULL AS INT64) AS thruplay,
    CAST(NULL AS INT64) AS video_q25,
    CAST(NULL AS INT64) AS video_q50,
    CAST(NULL AS INT64) AS video_q75,
    CAST(Video_Watches_100__Reddit AS INT64) AS video_q100,
    CAST(NULL AS INT64) AS post_engagement,
    CAST(NULL AS INT64) AS post_reactions,
    CAST(NULL AS INT64) AS post_comments,
    CAST(NULL AS INT64) AS outbound_clicks,
    CAST(NULL AS INT64) AS landing_page_views,
    CAST(0 AS NUMERIC) AS registrations,
    CAST(0 AS NUMERIC) AS leads,
    CAST(0 AS NUMERIC) AS on_platform_leads,
    CAST(0 AS NUMERIC) AS contacts,
    CAST(0 AS NUMERIC) AS donations,
    CAST(NULL AS STRING) AS campaign_objective,
    CAST(NULL AS INT64) AS viewability_measured,
    CAST(NULL AS INT64) AS viewability_viewed
  FROM
    `point-blank-ada.core_funnel_export.funnel_data`
  WHERE
    Date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
    AND Campaign_ID__Reddit IS NOT NULL
    AND Campaign_Name__Reddit IS NOT NULL
    AND Ad_ID__Reddit IS NOT NULL

  UNION ALL

  -- =========================================================================
  -- PINTEREST
  -- =========================================================================
  SELECT
    CAST(Date AS DATE) AS date,
    'pinterest' AS platform_id,
    Campaign_ID__Pinterest AS campaign_id,
    Campaign_Name__Pinterest AS campaign_name,
    Ad_Group_ID__Pinterest AS ad_set_id,
    Ad_Group_Name__Pinterest AS ad_set_name,
    Pin_ID__Pinterest AS ad_id,
    CAST(NULL AS STRING) AS ad_name,
    Advertiser_ID__Pinterest AS account_id,
    Advertiser_Name__Pinterest AS account_name,
    CAST(Spend__Pinterest AS NUMERIC) AS spend,
    CAST(Paid_impressions__Pinterest AS INT64) AS impressions,
    CAST(Paid_Outbound_Clicks__Pinterest AS INT64) AS clicks,
    CAST(NULL AS INT64) AS reach,
    CAST(NULL AS FLOAT64) AS frequency,
    CAST(Paid_video_views__Pinterest AS INT64) AS video_views,
    CAST(Paid_video_watched_at_100__Pinterest AS INT64) AS video_completions,
    CAST(Conversions__Pinterest AS NUMERIC) AS conversions,
    CAST(Paid_engagements__Pinterest AS INT64) AS engagements,
    -- Diagnostic signal columns (Pinterest: outbound clicks available natively)
    CAST(NULL AS INT64) AS video_views_3s,
    CAST(NULL AS INT64) AS thruplay,
    CAST(NULL AS INT64) AS video_q25,
    CAST(NULL AS INT64) AS video_q50,
    CAST(NULL AS INT64) AS video_q75,
    CAST(Paid_video_watched_at_100__Pinterest AS INT64) AS video_q100,
    CAST(Paid_engagements__Pinterest AS INT64) AS post_engagement,
    CAST(NULL AS INT64) AS post_reactions,
    CAST(NULL AS INT64) AS post_comments,
    CAST(Paid_Outbound_Clicks__Pinterest AS INT64) AS outbound_clicks,
    CAST(NULL AS INT64) AS landing_page_views,
    CAST(0 AS NUMERIC) AS registrations,
    CAST(0 AS NUMERIC) AS leads,
    CAST(0 AS NUMERIC) AS on_platform_leads,
    CAST(0 AS NUMERIC) AS contacts,
    CAST(0 AS NUMERIC) AS donations,
    CAST(NULL AS STRING) AS campaign_objective,
    CAST(NULL AS INT64) AS viewability_measured,
    CAST(NULL AS INT64) AS viewability_viewed
  FROM
    `point-blank-ada.core_funnel_export.funnel_data`
  WHERE
    Date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
    AND Campaign_ID__Pinterest IS NOT NULL
    AND Campaign_Name__Pinterest IS NOT NULL
    AND Pin_ID__Pinterest IS NOT NULL
),

enriched_data AS (
  SELECT
    pd.date,
    pd.platform_id,
    pd.campaign_id,
    pd.campaign_name,
    pd.ad_set_id,
    pd.ad_set_name,
    pd.ad_id,
    pd.ad_name,
    pd.account_id,
    pd.account_name,
    COALESCE(
      REGEXP_EXTRACT(pd.campaign_name, r'(?:^|_|\s|-)(2[0-9]\d{3})(?:_|\s|-|$)'),
      REGEXP_EXTRACT(pd.ad_set_name, r'(?:^|_|\s|-)(2[0-9]\d{3})(?:_|\s|-|$)'),
      cpm.project_code
    ) AS project_code,
    pd.spend,
    pd.impressions,
    pd.clicks,
    pd.reach,
    pd.frequency,
    pd.video_views,
    pd.video_completions,
    pd.conversions,
    pd.engagements,
    IF(pd.impressions > 0, SAFE_DIVIDE(pd.spend, pd.impressions) * 1000, NULL) AS cpm,
    IF(pd.clicks > 0, SAFE_DIVIDE(pd.spend, pd.clicks), NULL) AS cpc,
    IF(pd.impressions > 0, SAFE_DIVIDE(pd.clicks, pd.impressions), NULL) AS ctr,
    -- Diagnostic signal columns (pass through from platform_data)
    pd.video_views_3s,
    pd.thruplay,
    pd.video_q25,
    pd.video_q50,
    pd.video_q75,
    pd.video_q100,
    pd.post_engagement,
    pd.post_reactions,
    pd.post_comments,
    pd.outbound_clicks,
    pd.landing_page_views,
    pd.registrations,
    pd.leads,
    pd.on_platform_leads,
    pd.contacts,
    pd.donations,
    pd.campaign_objective,
    pd.viewability_measured,
    pd.viewability_viewed,
    'funnel_transform' AS ingestion_source,
    CURRENT_TIMESTAMP() AS loaded_at
  FROM
    platform_data pd
  LEFT JOIN `point-blank-ada.cip.campaign_project_mapping` cpm
    ON pd.platform_id = cpm.platform_id
    AND pd.campaign_name LIKE cpm.campaign_name
)

-- =============================================================================
-- MERGE INTO FACT_DIGITAL_DAILY
-- =============================================================================
-- Upsert with 7-field key: date, platform_id, campaign_id, ad_set_id, ad_id
-- (ad_set_id and ad_id handled as empty string if NULL for merge matching)
-- =============================================================================

MERGE INTO `point-blank-ada.cip.fact_digital_daily` AS target
USING (
  SELECT
    date,
    platform_id,
    campaign_id,
    COALESCE(ad_set_id, '') AS ad_set_id,
    COALESCE(ad_id, '') AS ad_id,
    campaign_name,
    ad_set_name,
    ad_name,
    account_id,
    account_name,
    project_code,
    spend,
    impressions,
    clicks,
    reach,
    frequency,
    video_views,
    video_completions,
    conversions,
    engagements,
    cpm,
    cpc,
    ctr,
    -- Diagnostic signal columns
    video_views_3s,
    thruplay,
    video_q25,
    video_q50,
    video_q75,
    video_q100,
    post_engagement,
    post_reactions,
    post_comments,
    outbound_clicks,
    landing_page_views,
    registrations,
    leads,
    on_platform_leads,
    contacts,
    donations,
    campaign_objective,
    viewability_measured,
    viewability_viewed,
    ingestion_source,
    loaded_at
  FROM
    enriched_data
) AS source
ON target.date = source.date
  AND target.platform_id = source.platform_id
  AND COALESCE(target.campaign_id, '') = COALESCE(source.campaign_id, '')
  AND COALESCE(target.ad_set_id, '') = COALESCE(source.ad_set_id, '')
  AND COALESCE(target.ad_id, '') = COALESCE(source.ad_id, '')

WHEN MATCHED THEN
  UPDATE SET
    campaign_name = source.campaign_name,
    ad_set_name = source.ad_set_name,
    ad_name = source.ad_name,
    account_id = source.account_id,
    account_name = source.account_name,
    project_code = source.project_code,
    spend = source.spend,
    impressions = source.impressions,
    clicks = source.clicks,
    reach = source.reach,
    frequency = source.frequency,
    video_views = source.video_views,
    video_completions = source.video_completions,
    conversions = source.conversions,
    engagements = source.engagements,
    cpm = source.cpm,
    cpc = source.cpc,
    ctr = source.ctr,
    -- Diagnostic signal columns
    video_views_3s = source.video_views_3s,
    thruplay = source.thruplay,
    video_q25 = source.video_q25,
    video_q50 = source.video_q50,
    video_q75 = source.video_q75,
    video_q100 = source.video_q100,
    post_engagement = source.post_engagement,
    post_reactions = source.post_reactions,
    post_comments = source.post_comments,
    outbound_clicks = source.outbound_clicks,
    landing_page_views = source.landing_page_views,
    registrations = source.registrations,
    leads = source.leads,
    on_platform_leads = source.on_platform_leads,
    contacts = source.contacts,
    donations = source.donations,
    campaign_objective = source.campaign_objective,
    viewability_measured = source.viewability_measured,
    viewability_viewed = source.viewability_viewed,
    ingestion_source = source.ingestion_source,
    loaded_at = source.loaded_at

WHEN NOT MATCHED THEN
  INSERT (
    date,
    platform_id,
    campaign_id,
    ad_set_id,
    ad_id,
    campaign_name,
    ad_set_name,
    ad_name,
    account_id,
    account_name,
    project_code,
    spend,
    impressions,
    clicks,
    reach,
    frequency,
    video_views,
    video_completions,
    conversions,
    engagements,
    cpm,
    cpc,
    ctr,
    -- Diagnostic signal columns
    video_views_3s,
    thruplay,
    video_q25,
    video_q50,
    video_q75,
    video_q100,
    post_engagement,
    post_reactions,
    post_comments,
    outbound_clicks,
    landing_page_views,
    registrations,
    leads,
    on_platform_leads,
    contacts,
    donations,
    campaign_objective,
    viewability_measured,
    viewability_viewed,
    ingestion_source,
    loaded_at
  )
  VALUES (
    source.date,
    source.platform_id,
    source.campaign_id,
    source.ad_set_id,
    source.ad_id,
    source.campaign_name,
    source.ad_set_name,
    source.ad_name,
    source.account_id,
    source.account_name,
    source.project_code,
    source.spend,
    source.impressions,
    source.clicks,
    source.reach,
    source.frequency,
    source.video_views,
    source.video_completions,
    source.conversions,
    source.engagements,
    source.cpm,
    source.cpc,
    source.ctr,
    -- Diagnostic signal columns
    source.video_views_3s,
    source.thruplay,
    source.video_q25,
    source.video_q50,
    source.video_q75,
    source.video_q100,
    source.post_engagement,
    source.post_reactions,
    source.post_comments,
    source.outbound_clicks,
    source.landing_page_views,
    source.registrations,
    source.leads,
    source.on_platform_leads,
    source.contacts,
    source.donations,
    source.campaign_objective,
    source.viewability_measured,
    source.viewability_viewed,
    source.ingestion_source,
    source.loaded_at
  );
