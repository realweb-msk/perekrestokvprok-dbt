{% if target.name == 'prod' %}

{{
  config(
    materialized='incremental',
    unique_key='unique_key',
    partition_by={
      "field": "date",
      "data_type": "date",
      "granularity": "day"
    },
    cluster_by = ["campaign_type"]
  )
}}

{% endif %}

WITH source AS (
  SELECT
      date_start,
      lower(campaign_name) as campaign_name,
      IF(REGEXP_CONTAINS(campaign_name, r'\[old\]'),'retargeting','UA') AS campaign_type,
      adset_name,
      ad_name,
      impressions,
      clicks,
      spend,
      actions,
      action_values,
      conversions,
      conversion_values
  FROM  {{ source('test2', 'facebook_ads_ad_stat_minimal_134923481805102') }}
),

unnests AS (
  SELECT 
    ARRAY_TO_STRING([
      CAST(date_start AS STRING),
      campaign_name,
      adset_name,
      ad_name
      ],'') AS unique_key,
    date_start AS date,
    campaign_name,
    campaign_type,
    adset_name,
    ad_name,
    SUM(impressions) AS impressions,
    SUM(clicks) AS clicks,
    SUM((
        SELECT value
        FROM UNNEST(actions)
        WHERE action_type = 'mobile_app_install'
        --'omni_app_install' не трекается
    )) AS installs,
    sum(spend) AS spend,
    SUM((
        SELECT value
        FROM UNNEST(actions)
        where action_type = 'app_custom_event.fb_mobile_purchase'
    )) AS purchase,
    SUM((
        SELECT value
        FROM UNNEST(action_values)
        where action_type = 'app_custom_event.fb_mobile_purchase'
    )) AS revenue,
    SUM((
        SELECT value
        FROM UNNEST(conversions)
        WHERE action_type = 'start_trial_mobile_app' 
        --'start_trial_total не трекается
    )) AS first_purchase,
    SUM((
        SELECT value
        FROM UNNEST(conversion_values)
        WHERE action_type = 'start_trial_total'
    )) AS first_purchase_revenue,
    SUM((
        SELECT value
        FROM UNNEST(actions)
        where action_type = 'app_custom_event.fb_mobile_add_to_cart'
    )) AS add_to_cart,
  FROM source
  GROUP BY 1,2,3,4,5,6
)

SELECT
    unique_key,
    date,
    campaign_name,
    campaign_type,
    adset_name,
    ad_name,
    impressions,
    clicks,
    installs,
    spend,
    purchase,
    revenue,
    first_purchase,
    first_purchase_revenue,
    add_to_cart
FROM unnests

{% if not is_incremental() %}

-- первый раз --
UNION ALL
SELECT DISTINCT
    ARRAY_TO_STRING([
      CAST(date AS STRING),
      lower(campaign_name),
      adset_name,
      ad_name
      ],'') AS unique_key,
    date,
    lower(campaign_name) campaign_name,
    IF(REGEXP_CONTAINS(campaign_name, r'\[old\]'),'retargeting','UA') AS campaign_type,
    adset_name,
    ad_name,
    show AS impressions,
    clicks,
    installs,
    spend,
    cnt_af_purchase AS purchase,
    revenue,
    cnt_first_purchase AS first_purchase,
    first_purchase_revenue,
    add_to_card
FROM {{ source('sheets_data', 'FBNEW_data') }}
WHERE date < (
  SELECT MIN(date)
  FROM unnests
)

--на будущее --
-- UNION DISTINCT
-- SELECT
--     unique_key,
--     date,
--     campaign_name,
--     adset_name,
--     ad_name,
--     impressions,
--     clicks,
--     installs,
--     spend,
--     purchase,
--     revenue,
--     first_purchase,
--     first_purchase_revenue,
--     add_to_cart
-- FROM {{ this }}

{% endif %}



