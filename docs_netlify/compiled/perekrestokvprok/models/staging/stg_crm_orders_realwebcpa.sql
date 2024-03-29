





SELECT
  date,
  REGEXP_REPLACE(REGEXP_REPLACE(campaign, r'.*\/ ', ''), r'\W', '') as campaign,
  COUNT(distinct af_order_id) as orders
 --FROM `perekrestokvprok-bq`.`sheets_data`.`crm_redeem_first_orders`
 FROM `perekrestokvprok-bq`.`sheets_data`.`crm_redeem_first_orders_realwebcpa`
 WHERE 
  orderNumber = 1
  AND bo_order_id IS NOT NULL
  AND site = 'Новое мобильное приложение "Впрок"' 
  AND date_difference = 'Даты совпадают'
 GROUP BY 1,2

UNION ALL

 SELECT
  date,
  REGEXP_REPLACE(REGEXP_REPLACE(campaign, r'.*\/ ', ''), r'\W', '') as campaign,
  COUNT(distinct af_order_id) as orders
 --FROM `perekrestokvprok-bq`.`sheets_data`.`crm_redeem_first_orders`
 FROM `perekrestokvprok-bq`.`sheets_data`.`crm_redeem_first_orders_realwebcpa_2023`
 WHERE 
  orderNumber = 1
  AND bo_order_id IS NOT NULL
  AND site = 'Новое мобильное приложение "Впрок"' 
  AND is_valid_order = 'Валидный заказ'
 GROUP BY 1,2