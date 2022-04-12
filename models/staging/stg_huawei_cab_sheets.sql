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
        {{ normalize('campaign_name')}} campaign_name,
        status,
        cost,
        clicks,
        impressions,
        activations,
        campaign_type AS type,
        exchange_rate
    FROM {{ source('sheets_data', 'huawei_data') }}
    WHERE date IS NOT NULL
),

final AS (
    SELECT
        ARRAY_TO_STRING([
            CAST(date AS STRING),
            campaign_name
        ],'') AS unique_key,
        date,
        campaign_name,
        IF(
          REGEXP_CONTAINS(campaign_name, r'[_\[]new[\]_]'),'UA','retargeting'
          ) AS campaign_type,
        type,
        status,
        activations,
        cost,
        cost * exchange_rate AS spend,
        clicks,
        impressions,
        exchange_rate
    FROM source
)

SELECT
    unique_key,
    date,
    campaign_name,
    campaign_type,
    type,
    status,
    activations,
    cost,
    spend,
    clicks,
    impressions,
    exchange_rate
FROM final
