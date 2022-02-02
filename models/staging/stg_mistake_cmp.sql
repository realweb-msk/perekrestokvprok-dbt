{% if target.name == 'prod' %}

{{
  config(
    materialized='table',
  )
}}

{% endif %}


SELECT
    mistake,
    correct
FROM {{ source('sheets_data', 'mistake_cmp') }}
WHERE mistake IS NOT NULL