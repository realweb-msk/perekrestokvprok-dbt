{% if target.name == 'prod' %}

{{
  config(
    materialized='table',
  )
}}

{% endif %}


SELECT
DISTINCT
    start_date,
    end_date,
    partner,
    platform,
    rate_for_parthner AS rate_for_partner,
    plan_f_p,
    rate_for_us,
    type,
    source,
    CASE
      WHEN REGEXP_CONTAINS(campaign_name, r'deep_outflow') THEN 'deep_outflow'
      WHEN type = 'RTG' and start_date >= '2022-10-01' and end_date <= '2022-10-04' THEN 'deep_outflow'
      WHEN REGEXP_CONTAINS(campaign_name, r'first_open_not_buy_rtg') THEN 'first_open_not_buy_rtg'
      WHEN REGEXP_CONTAINS(campaign_name, r'installed_the_app_but_not_buy_rtg') THEN 'installed_the_app_but_not_buy_rtg'
      WHEN REGEXP_CONTAINS(campaign_name, r'registered_but_not_buy_rtg') THEN 'registered_but_not_buy_rtg'
      ELSE 'Other'
    END as base
FROM {{ source('sheets_data', '_rates') }}
WHERE start_date IS NOT NULL