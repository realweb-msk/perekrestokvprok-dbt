{% if target.name == 'prod' %}

{{
  config(
    materialized='table',
  )
}}

{% endif %}

SELECT
  date,
  REGEXP_REPLACE(REGEXP_REPLACE(campaign, r'.*\/ ', ''), r'\W', '') as campaign,
  COUNT(distinct af_order_id) as orders
 --FROM {{ source('sheets_data', 'crm_redeem_first_orders') }}
 FROM {{ source('sheets_data', 'crm_redeem_first_orders') }}
 WHERE af_transaction_type = 'first_transaction' AND orderNumber is not null
 GROUP BY 1,2