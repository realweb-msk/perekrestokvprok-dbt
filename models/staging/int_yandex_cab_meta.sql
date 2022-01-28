WITH source AS (
    SELECT
        Date,
        CampaignName,
        AdGroupName,
        Impressions,
        Clicks,
        Cost
    FROM {{ source('MetaCustom', 'yandex_direct_ad_keyword_stat_x5perek_direct') }}
),

final AS (
    SELECT
        date,
        LOWER(REPLACE(REPLACE(CampaignName,'+','_'),'-','_')) AS campaign_name,
        AdGroupName AS adset_name,
        SUM(Impressions) AS impressions,
        SUM(Clicks) AS clicks,
        SUM(SAFE_CAST(SAFE_DIVIDE(Cost, 1.2) AS FLOAT64)) AS spend
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