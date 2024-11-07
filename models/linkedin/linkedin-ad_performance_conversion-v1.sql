{{
  custom_config(
    alias=var('linkedin_ad_performance_conversion_v1_alias','linkedin-ad_performance_conversion-v1'),
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

  campaign_history AS (
  SELECT
    id campaign_id,
    account_id,
    name campaign_name,
    status campaign_status,
  FROM
    {{ source('linkedin', 'campaign_history') }}
  QUALIFY
    ROW_NUMBER() OVER(PARTITION BY id, account_id, name ORDER BY last_modified_time DESC)=1 ),

  campaign_account_history AS (
  SELECT
    DISTINCT campaign_history.campaign_id,
    campaign_history.account_id,
    campaign_history.campaign_name,
    campaign_history.campaign_status,
    account_history.account_currency,
    account_history.account_name,
    account_history.account_type,
  FROM
    campaign_history
  LEFT JOIN
    account_history
  USING
    (account_id) ),

  conversion_history AS (
  SELECT
    DISTINCT id conversion_id,
    account_id,
    name conversion_name,
    type conversion_type,
  FROM
    {{ source('linkedin', 'conversion_history') }} ),

  conversion_associated_campaign AS (
  SELECT
    DISTINCT campaign_id,
    conversion_id
  FROM
    {{ source('linkedin', 'conversion_associated_campaign') }} ),

  conversion_history_with_associated_campaign AS (
  SELECT
    DISTINCT conversion_associated_campaign.campaign_id,
    conversion_history.account_id,
    conversion_history.conversion_name,
    conversion_history.conversion_type,
  FROM
    conversion_associated_campaign
  LEFT JOIN
    conversion_history
  USING
    (conversion_id)),

  ad_analytics_by_campaign AS (
  SELECT
    DISTINCT day,
    campaign_id,
    conversion_value_in_local_currency,
    external_website_conversions,
    external_website_post_click_conversions,
    external_website_post_view_conversions,
  FROM
    {{ source('linkedin', 'ad_analytics_by_campaign') }} )

SELECT
  DISTINCT SAFE_CAST( ad_analytics_by_campaign.day AS DATE ) startDate,
  SAFE_CAST( campaign_account_history.account_currency AS STRING ) account_currency,
  SAFE_CAST( campaign_account_history.account_id AS STRING ) account_id,
  SAFE_CAST( campaign_account_history.account_name AS STRING ) account_name,
  SAFE_CAST( campaign_account_history.account_type AS STRING ) account_type,
  SAFE_CAST( ad_analytics_by_campaign.campaign_id AS STRING ) campaign_id,
  SAFE_CAST( campaign_account_history.campaign_name AS STRING ) campaign_name,
  SAFE_CAST( campaign_account_history.campaign_status AS STRING ) campaign_status,
  SAFE_CAST( conversion_history_with_associated_campaign.conversion_name AS STRING ) conversion_name,
  SAFE_CAST( conversion_history_with_associated_campaign.conversion_type AS STRING ) conversion_type,
  SAFE_CAST( conversion_value_in_local_currency AS STRING ) conversionValueInLocalCurrency,
  SAFE_CAST( ad_analytics_by_campaign.external_website_conversions AS STRING ) externalWebsiteConversions,
  SAFE_CAST( ad_analytics_by_campaign.external_website_post_click_conversions AS STRING ) externalWebsitePostClickConversions,
  SAFE_CAST( ad_analytics_by_campaign.external_website_post_view_conversions AS STRING ) externalWebsitePostViewConversions,
FROM
  ad_analytics_by_campaign
LEFT JOIN
  campaign_account_history
  USING (campaign_id)
LEFT JOIN
  conversion_history_with_associated_campaign
  USING (campaign_id, account_id)
