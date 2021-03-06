{% if target.name == 'prod' %}

{{
  config(
    materialized='table',
  )
}}

{% endif %}


SELECT
    date_start,
    date_end,
    promocod AS promocode,
    type,
    Channel AS channel
FROM {{ source('sheets_data', 'promo_sheets') }}
WHERE date_start IS NOT NULL