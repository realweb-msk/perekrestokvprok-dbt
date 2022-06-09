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
        PARSE_DATE('%d.%m.%Y', date) AS date,
        {{ normalize('campaign_name')}} campaign_name,
        cost
    FROM {{ source('sheets_data', 'zen_sheets') }}
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
        COALESCE(SAFE_CAST(REGEXP_REPLACE(cost, r',', '.') AS FLOAT64), 0) AS cost
    FROM source
)

SELECT
    unique_key,
    date,
    campaign_name,
    campaign_type,
    cost
FROM final
