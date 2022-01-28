WITH source AS (
    SELECT
        Date,
        metadata_campaignName,
        impressions,
        taps,
        localSpend_amount
    FROM {{ source('MetaCustom', 'apple_search_ads_ASA_cost') }}
),

final AS (
    SELECT
        Date AS date,
        LOWER(metadata_campaignName) AS campaign_name,
        '-' adset_name,
        SUM(impressions) AS impressions,
        SUM(taps) AS clicks,
        SUM(SAFE_CAST(localSpend_amount AS FLOAT64)) AS spend
    FROM source
    GROUP BY 1,2,3
)

SELECT
    date,
    campaign_name,
    adset_name,
    impressions,
    clicks,
    spend
FROM final