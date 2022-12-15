

  create or replace view `perekrestokvprok-bq`.`dbt_krepin`.`dim_client_promo`
  OPTIONS()
  as ---костов здесь нет, добавь в PBI---
WITH promo_client_data AS (
    SELECT
        order_date,
        region,
        promo_name,
        promo_name_full,
        LOWER(promo_code) AS promo_code,
        order_id,
        platform,
        revenue,
        order_count
    FROM `perekrestokvprok-bq`.`dbt_krepin`.`stg_promocode_client_data`
),

promo_dict AS (
    SELECT
        LOWER(promocode) AS promo_code,
        type,
        channel
    FROM `perekrestokvprok-bq`.`dbt_krepin`.`stg_promo_sheets`
)

SELECT
    DATE(order_date) AS date,
    channel,
    type,
    UPPER(promo_code) AS promo_code,
    promo_name,
    region,
    revenue,
    order_count
FROM promo_client_data
LEFT JOIN promo_dict
USING(promo_code)
WHERE type IS NOT NULL;

