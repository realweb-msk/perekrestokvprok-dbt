WITH source AS (
    SELECT 
        date,
        campaign,
        --- есть, но в запросе зануляется: ---
        -- impressions,
        -- clicks,
        RevenueAdvCurrency
    FROM {{ source('MetaCustom','google_dbm_google_dv360_cost_5807131') }}
),

final AS (
    SELECT
        date,
        LOWER(campaign) AS campaign_name,
        '-' AS adset_name,
        0 AS impressions,
        0 AS clicks,
        SUM(RevenueAdvCurrency) AS spend
    FROM source
    GROUP BY 1, 2, 3, 4, 5
)

SELECT
    date,
    campaign_name,
    adset_name,
    impressions,
    clicks,
    spend
FROM final