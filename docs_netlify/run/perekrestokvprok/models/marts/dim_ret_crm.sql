

  create or replace view `perekrestokvprok-bq`.`dbt_lazuta`.`dim_ret_crm`
  OPTIONS()
  as WITH af_orders AS (
    SELECT
        REGEXP_EXTRACT(REGEXP_REPLACE(event_value,'"',''), 'af_order_id:(.*?),') AS order_id
    FROM  `perekrestokvprok-bq`.`dbt_lazuta`.`stg_af_client_data`
    WHERE event_name = "af_purchase"
),

client_data AS (
    SELECT
        order_date,
        promo_name,
        promo_code,
        order_id,
        platform,
        revenue,
        order_count
    FROM `perekrestokvprok-bq`.`dbt_lazuta`.`stg_promocode_client_data`
),

promo_data AS (
    SELECT
        date_start,
        date_end,
        promocode,
        type,
        channel
    FROM `perekrestokvprok-bq`.`dbt_lazuta`.`stg_promo_sheets`
),

final AS (
    SELECT
        order_date AS date,
        promo_name AS campaign_name,
        platform,
        CASE
            WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([promo_name],'')), r'promo.*regular') THEN 'promo regular'
            WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([promo_name],'')), r'promo.*global') THEN 'promo global'
            WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([promo_name],'')), r'promo.*feed') THEN 'promo feed'
        ELSE '-' END as promo_type,
        0 AS re_engagement,
        SUM(revenue) AS revenue,
        SUM(order_count) AS purchase,
        0 AS spend,
        channel AS source,
        'CRM' AS conversion_source_type
    FROM client_data
    LEFT JOIN promo_data
    ON LOWER(client_data.promo_code) = LOWER(promo_data.promocode)
    AND DATE(client_data.order_date) BETWEEN promo_data.date_start AND promo_data.date_end
    WHERE type is not null
    AND order_id NOT IN (
        SELECT DISTINCT order_id
        FROM af_orders
    )
    GROUP BY 1,2,3,4,9
)

SELECT
    date,
    campaign_name,
    platform,
    promo_type,
    re_engagement,
    revenue,
    purchase,
    spend,
    source,
    conversion_source_type
FROM final;

