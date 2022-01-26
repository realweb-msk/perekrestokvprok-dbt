WITH 
tiktok_storage AS (
    SELECT DISTINCT
        date,
        campaign_name,
        ad_group_name,
        cost,
        total_purchases,
        total_achieve_level
    FROM {{ source('sheets_data', 'TIKTOK_table') }}
    WHERE date < DATE_ADD(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL -3 DAY)
),

tiktok_sheet AS (
    SELECT DISTINCT
        date,
        campaign_name,
        ad_group_name,
        cost,
        total_purchases,
        total_achieve_level
    FROM {{ source('sheets_data', 'tiktok_sheets_campaign_cost_purchases') }}
    WHERE date >= DATE_ADD(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL -3 DAY)
),

final AS (
    SELECT *
    FROM tiktok_storage
    UNION DISTINCT 
    SELECT *
    FROM tiktok_sheet
)

SELECT 
    date,
    campaign_name,
    ad_group_name AS adset_name,
    cost,
    total_purchases AS purchase,
    total_achieve_level AS first_purchase
FROM final