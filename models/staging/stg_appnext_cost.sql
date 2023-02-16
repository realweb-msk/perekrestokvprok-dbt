{% if target.name == 'prod' %}

{{
  config(
    materialized='table',
  )
}}

{% endif %}

SELECT
DISTINCT
    date,
    campaign_name,
    type,
    cost
FROM `perekrestokvprok-bq`.`sheets_data`.`appnext_cost`
  