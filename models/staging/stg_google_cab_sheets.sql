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
    SELECT DISTINCT
        date,
        LOWER(campaign_name) campaign_name,
        adset_name,
        costs,
        installs,
        clicks,
        impressions
    FROM {{ source('sheets_data', 'google_ads_costs_and_installs') }}
    WHERE date IS NOT NULL
),

final AS (
    SELECT
        ARRAY_TO_STRING([
            CAST(date AS STRING),
            campaign_name,
            adset_name
        ],'') AS unique_key,
        date,
        campaign_name,
        IF(REGEXP_CONTAINS(campaign_name, r'\[old\]'),'retargeting','UA') AS campaign_type,
        adset_name,
        costs AS spend,
        installs,
        clicks,
        impressions
    FROM source
)

SELECT
    unique_key,
    date,
    {{ normalize('campaign_name') }} AS campaign_name,
    campaign_type,
    adset_name,
    spend,
    installs,
    clicks,
    impressions
FROM final

{% if not is_incremental() %}

-- первый раз --
UNION ALL
SELECT DISTINCT
    ARRAY_TO_STRING([
            CAST(date AS STRING),
            LOWER(campaign_name),
            adset_name
    ],'') AS unique_key,
    date,
    LOWER(campaign_name) campaign_name,
    IF(REGEXP_CONTAINS(campaign_name, r'\[old\]'),'retargeting','UA') AS campaign_type,
    adset_name,
    costs AS spend,
    installs,
    clicks,
    impressions
FROM {{ source('agg_data', 'google_ads_costs_and_installs_sum') }}
WHERE date < (
  SELECT MIN(date)
  FROM final
)
AND date IS NOT NULL

{% endif %}