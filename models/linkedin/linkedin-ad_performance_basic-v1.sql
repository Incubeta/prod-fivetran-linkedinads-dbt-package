{{
  custom_config(
    alias=var('linkedin_ad_performance_basic_v1_alias','linkedin-ad_performance_basic-v1'),
    field="startDate")
}}

WITH
  account_history AS (
  SELECT
    DISTINCT id account_id,
    currency account_currency,
    name account_name,
    type account_type,
  FROM
    {{ source('linkedin', 'account_history') }} ),

  creative_history AS (
  SELECT
    DISTINCT id creative_id,
    account_id,
    campaign_id
  FROM
    {{ source('linkedin', 'creative_history') }} ),

  creative_account_history AS (
  SELECT
    *
  FROM
    creative_history
  LEFT JOIN
    account_history
  USING
    (account_id) ),

  campaign_history AS (
  SELECT
    id campaign_id,
    account_id,
    campaign_group_id,
    name campaign_name,
    objective_type campaign_objectiveType,
    status campaign_status,
  FROM
    {{ source('linkedin', 'campaign_history') }}
  QUALIFY
  ROW_NUMBER() OVER(PARTITION BY id, account_id, campaign_group_id, name ORDER BY last_modified_time DESC)=1),

  campaign_group_history AS (
  SELECT
    DISTINCT id campaign_group_id,
    account_id,
    status campaign_group_status,
  FROM
    {{ source('linkedin', 'campaign_group_history') }}
  QUALIFY
  ROW_NUMBER() OVER(PARTITION BY id, account_id ORDER BY last_modified_time DESC)=1),

  ad_analytics_by_creative AS (
  SELECT
    day,
    creative_id,
    clicks,
    cost_in_local_currency,
    follows,
    impressions,
    landing_page_clicks,
    one_click_leads,
    total_engagements,
    video_completions,
    video_first_quartile_completions,
    video_midpoint_completions,
    video_starts,
    video_third_quartile_completions,
    video_views,
    external_website_conversions,
    external_website_post_click_conversions,
    external_website_post_view_conversions,
    full_screen_plays,
    one_click_lead_form_opens,
  FROM
    {{ source('linkedin', 'ad_analytics_by_creative') }} )

  SELECT
    SAFE_CAST( ad_analytics_by_creative.day AS DATE ) startDate,
    SAFE_CAST( creative_account_history.account_id AS STRING ) account_id,
    SAFE_CAST( creative_account_history.account_name AS STRING ) account_name,
    SAFE_CAST( creative_account_history.account_type AS STRING ) account_type,
    SAFE_CAST( creative_account_history.account_currency AS STRING ) account_currency,
    SAFE_CAST( campaign_history.campaign_id AS STRING ) campaign_id,
    SAFE_CAST( campaign_history.campaign_name AS STRING ) campaign_name,
    SAFE_CAST( campaign_history.campaign_status AS STRING ) campaign_status,
    SAFE_CAST( ad_analytics_by_creative.creative_id AS STRING ) creative_id,
    SAFE_CAST( NULL AS STRING ) creative_name,
    SAFE_CAST( ad_analytics_by_creative.clicks AS STRING ) clicks,
    SAFE_CAST( ad_analytics_by_creative.cost_in_local_currency AS STRING ) costInLocalCurrency,
    SAFE_CAST( ad_analytics_by_creative.follows AS STRING ) follows,
    SAFE_CAST( ad_analytics_by_creative.impressions AS STRING ) impressions,
    SAFE_CAST( ad_analytics_by_creative.landing_page_clicks AS STRING ) landingPageClicks,
    SAFE_CAST( ad_analytics_by_creative.one_click_leads AS STRING ) oneClickLeads,
    SAFE_CAST( ad_analytics_by_creative.total_engagements AS STRING ) totalEngagements,
    SAFE_CAST( ad_analytics_by_creative.video_completions AS STRING ) videoCompletions,
    SAFE_CAST( ad_analytics_by_creative.video_first_quartile_completions AS STRING ) videoFirstQuartileCompletions,
    SAFE_CAST( ad_analytics_by_creative.video_midpoint_completions AS STRING ) videoMidpointCompletions,
    SAFE_CAST( ad_analytics_by_creative.video_starts AS STRING ) videoStarts,
    SAFE_CAST( ad_analytics_by_creative.video_third_quartile_completions AS STRING ) videoThirdQuartileCompletions,
    SAFE_CAST( ad_analytics_by_creative.video_views AS STRING ) videoViews,
    SAFE_CAST( ad_analytics_by_creative.external_website_conversions AS STRING ) externalWebsiteConversions,
    SAFE_CAST( ad_analytics_by_creative.external_website_post_click_conversions AS STRING ) externalWebsitePostClickConversions,
    SAFE_CAST( ad_analytics_by_creative.external_website_post_view_conversions AS STRING ) externalWebsitePostViewConversions,
    SAFE_CAST( ad_analytics_by_creative.full_screen_plays AS STRING ) fullScreenPlays,
  FROM
    ad_analytics_by_creative
  LEFT JOIN
    creative_account_history
  USING
    (creative_id)
  LEFT JOIN
    campaign_history
  USING
    (campaign_id,
      account_id)
  LEFT JOIN
    campaign_group_history
  USING
    (account_id,
      campaign_group_id)
