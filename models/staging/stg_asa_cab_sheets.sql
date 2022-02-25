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
        campaign,
        Clikcs AS clicks,
        spend,
        installs,
        ROW_NUMBER() OVER(PARTITION BY date, campaign) AS counter
    FROM {{ source('sheets_data', 'Asa_cost') }}
    WHERE date IS NOT NULL
),

final AS (
    SELECT DISTINCT
        ARRAY_TO_STRING([
            CAST(date AS string),
            LOWER(campaign)
        ],'') AS unique_key,
        date,
        LOWER(campaign) AS campaign_name,
        clicks,
        spend,
        installs
    FROM source
    WHERE counter = 1
)

SELECT DISTINCT
    unique_key,
    date,
    {{ normalize('campaign_name') }} AS campaign_name,
    IF(REGEXP_CONTAINS(campaign_name, r'\(R\)'),'retargeting','UA') AS campaign_type,
    '-' adset_name,
    clicks,
    spend,
    installs,
    0 impressions
FROM final

{% if not is_incremental() %}

-- первый раз --
UNION ALL
SELECT DISTINCT
    ARRAY_TO_STRING([
      CAST(date AS STRING),
      LOWER(campaign_name)
      ],'') AS unique_key,
    date,
    campaign_name,
    campaign_type,
    adset_name,
    clicks,
    spend,
    0 installs,
    impressions
FROM {{ ref ('int_asa_cab_meta') }}
WHERE date < (
  SELECT MIN(date)
  FROM final
)
AND date IS NOT NULL

{% endif %}