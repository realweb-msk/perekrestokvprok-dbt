

  create or replace view `perekrestokvprok-bq`.`dbt_production`.`stg_promocode_client_data`
  OPTIONS()
  as SELECT
    DATE(orderDate) AS order_date,
    region,
    promoname AS promo_name,
    promonamefull AS promo_name_full,
    promocode AS promo_code,
    orderid AS order_id,
    platform,
    revenue,
    cnt_order AS order_count
FROM `perekrestokvprok-bq`.`agg_data`.`promocod_client_data`;

