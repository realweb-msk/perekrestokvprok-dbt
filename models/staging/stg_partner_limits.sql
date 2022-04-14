{% if target.name == 'prod' %}

{{
  config(
    materialized='table',
  )
}}

{% endif %}


SELECT
    start_date,
    end_date,
    partner,
    `limit` as limits,
    type,
    source
FROM {{ source('sheets_data','limits_sheet') }}
WHERE start_date IS NOT NULL