

  create or replace table `perekrestokvprok-bq`.`dbt_production`.`stg_crm_orders`
  
  
  OPTIONS()
  as (
    





SELECT
  date,
  REGEXP_REPLACE(REGEXP_REPLACE(campaign, r'.*\/ ', ''), r'\W', '') as campaign,
  COUNT(distinct af_order_id) as orders
 --FROM `perekrestokvprok-bq`.`sheets_data`.`crm_redeem_first_orders`
 FROM `perekrestokvprok-bq`.`sheets_data`.`crm_redeem_first_orders`
 WHERE af_transaction_type = 'first_transaction' AND orderNumber is not null
 GROUP BY 1,2
  );
  