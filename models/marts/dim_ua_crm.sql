WITH af_orders AS (
    SELECT
        REGEXP_EXTRACT(REGEXP_REPLACE(event_value,'"',''), 'af_order_id:(.*?),') AS order_id
    FROM  {{ ref('stg_af_client_data') }}
    WHERE event_name = "af_purchase"
),

client_data AS (
    SELECT
        order_date,
        promo_name,
        '-' adset_name,
        promo_code,
        order_id,
        platform,
        revenue,
        order_count
    FROM {{ ref('stg_promocode_client_data') }}
),

promo_data AS (
    SELECT
        date_start,
        date_end,
        promocode,
        type,
        channel
    FROM {{ ref('stg_promo_sheets') }}
),

final AS (
    SELECT
        order_date AS date,
        promo_name AS campaign_name,
        platform,
        {{ promo_type('promo_name', 'adset_name') }} as promo_type,
        {{ geo('promo_name', 'adset_name') }} AS geo,
        'UA' AS campaign_type,
        0 AS impressions,
        0 AS clicks,
        0 AS installs,
        SUM(revenue) AS revenue,
        SUM(order_count) AS purchase,
        0 AS uniq_purchase,
        0 AS first_purchase_revenue,
        0 AS first_purchase,
        0 AS uniq_first_purchase,
        0 AS spend,
        channel AS source,
        'CRM' AS conversion_source_type,
        '-' AS adv_type
    FROM client_data
    LEFT JOIN promo_data
    ON LOWER(client_data.promo_code) = LOWER(promo_data.promocode)
    AND DATE(client_data.order_date) BETWEEN promo_data.date_start AND promo_data.date_end
    WHERE type = 'uac'
    AND order_id NOT IN (
        SELECT DISTINCT order_id
        FROM af_orders
    )
    GROUP BY 1,2,3,4,5,17
)

SELECT 
    date,
    campaign_name,
    platform,
    promo_type,
    geo,
    campaign_type,
    impressions,
    clicks,
    installs,
    revenue,
    purchase,
    uniq_purchase,
    first_purchase_revenue,
    first_purchase,
    uniq_first_purchase,
    spend,
    source,
    conversion_source_type,
    adv_type
FROM final