{% if target.name == 'prod' %}

{{
  config(
    materialized='table',
  )
}}

{% endif %}

WITH source AS (
    SELECT
        PARSE_DATE('%d.%m.%Y', date) AS date,
        {{ normalize('campaign_name')}} campaign_name,
        SUM(COALESCE(SAFE_CAST(REGEXP_REPLACE(cost, r',', '.') AS FLOAT64), 0)) as cost
    FROM {{ source('sheets_data', 'vk_beta_sheet') }}
    WHERE date IS NOT NULL
    GROUP BY date, campaign_name
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
          REGEXP_CONTAINS(campaign_name, r'[_\[]new[\]_]|_new_install_'),'UA','retargeting'
          ) AS campaign_type,
        cost
    FROM source
)

SELECT
    unique_key,
    date,
    campaign_name,
    campaign_type,
    cost
FROM final
