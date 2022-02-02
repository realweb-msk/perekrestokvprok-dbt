WITH old_source AS (
    SELECT
        date,
        insertionOrder,
        insertionOrderID,
        impressions,
        clicks,
        revenueAdvCurrency,
        ROW_NUMBER() OVER (PARTITION BY InsertionOrderID, LineItemID, Date ORDER BY DATE(_PARTITIONTIME) DESC) 
            AS pd_rw
    FROM {{ source('DCM', 'google_dbm_existed_dbm_report_293044') }}
    WHERE date < '2021-12-01'
),

old_modified AS (
    SELECT
        date,
        insertionOrder AS insertion_order,
        insertionOrderID AS insertion_order_id,
        SUM(SAFE_CAST(impressions AS INT64)) AS impressions,
        SUM(SAFE_CAST(clicks AS INT64)) AS clicks,
        SUM(SAFE_CAST(revenueAdvCurrency AS FLOAT64)) AS revenue_adv_currency
    FROM old_source
    WHERE pd_rw = 1
    GROUP BY 1, 2, 3
),

new_source AS (
    SELECT
        date,
        insertion_order,
        insertion_order_id,
        impressions,
        clicks,
        revenue_adv_currency,
    FROM {{ ref('stg_google_dbm') }}
    WHERE date >= '2021-12-01'
),

new_modified AS (
    SELECT
        date,
        insertion_order,
        insertion_order_id,
        SUM(SAFE_CAST(impressions AS INT64)) AS impressions,
        SUM(SAFE_CAST(clicks AS INT64)) AS clicks,
        SUM(SAFE_CAST(revenue_adv_currency AS FLOAT64)) AS revenue_adv_currency
    FROM new_source
    GROUP BY 1, 2, 3
),

final AS (
    SELECT *
    FROM old_modified
    UNION DISTINCT
    SELECT *
    FROM new_modified
)

SELECT
    date,
    insertion_order,
    insertion_order_id,
    impressions,
    clicks,
    revenue_adv_currency
FROM final