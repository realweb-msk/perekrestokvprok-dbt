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
    plan_budget,
    plan_type,
    plan_order
FROM {{ source('sheets_data','budget_and_plan') }}
WHERE start_date IS NOT NULL