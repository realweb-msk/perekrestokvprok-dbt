{% if target.name == 'prod' %}

{{
  config(
    materialized='table',
  )
}}

{% endif %}


SELECT DISTINCT
    LOWER(promo) AS promo,
    name
FROM {{ source('sheets_data', 'promo_dict_sheets') }}
WHERE promo IS NOT NULL