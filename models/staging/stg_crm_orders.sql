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
 WHERE 
  orderNumber = 1
  AND bo_order_id IS NOT NULL
  AND site = 'Новое мобильное приложение "Впрок"' 
  AND date_difference = 'Даты совпадают'
  --AND REGEXP_CONTAINS(campaign, r'realweb_inapp')
 GROUP BY 1,2

UNION ALL

 SELECT
  date,
  REGEXP_REPLACE(REGEXP_REPLACE(campaign, r'.*\/ ', ''), r'\W', '') as campaign,
  COUNT(distinct af_order_id) as orders
 --FROM {{ source('sheets_data', 'crm_redeem_first_orders') }}
 FROM {{ source('sheets_data', 'crm_redeem_first_orders_2023') }}
 WHERE 
  orderNumber = 1
  AND bo_order_id IS NOT NULL
  AND site = 'Новое мобильное приложение "Впрок"' 
  AND is_valid_order = 'Валидный заказ'
  --AND REGEXP_CONTAINS(campaign, r'realweb_inapp')
 GROUP BY 1,2