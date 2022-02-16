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
        string_field_0,
        string_field_2,
        string_field_9,
        string_field_10,
        ROW_NUMBER() OVER(PARTITION BY string_field_0, string_field_2) AS counter
    FROM {{ source('sheets_data', 'twitter_sheets') }}
    WHERE string_field_0 IS NOT NULL
),

final AS (
    SELECT DISTINCT
        ARRAY_TO_STRING([
            CAST(DATE(REPLACE(string_field_0,'.','-')) AS STRING),
            LOWER(string_field_2)
        ],'') AS unique_key,
        DATE(REPLACE(string_field_0,'.','-')) AS date,
        LOWER(string_field_2) AS campaign_name,
        SAFE_CAST(REPLACE(string_field_9,',','.') AS FLOAT64) AS impressions,
        SAFE_CAST(REPLACE(string_field_10,',','.') AS FLOAT64) AS spend
    FROM source
    WHERE counter = 1
)

SELECT DISTINCT
    unique_key,
    date,
    {{ normalize('campaign_name') }} AS campaign_name,
    IF(REGEXP_CONTAINS(campaign_name, r'_old_'),'retargeting','UA') AS campaign_type,
    impressions,
    spend
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
    LOWER(campaign_name) campaign_name,
    IF(REGEXP_CONTAINS(campaign_name, r'_old_'),'retargeting','UA') AS campaign_type,
    impressions,
    spend,
FROM {{ source('sheets_data', 'twitter_data') }}
WHERE date < (
  SELECT MIN(date)
  FROM final
)
AND date IS NOT NULL

{% endif %}