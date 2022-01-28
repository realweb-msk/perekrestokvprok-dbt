WITH source AS (
    SELECT
        day,
        campaign.name,
        impressions,
        clicks,
        spent
    FROM {{ source('MetaCustom', 'vk_campaign_stat_1900013586_1605495720') }}
),

final AS (
    SELECT
        day AS date,
        LOWER(REPLACE(REPLACE(name,'+','_'),'-','_')) AS campaign_name,
        '-' adset_name,
        SUM(impressions) AS impressions,
        SUM(clicks) AS clicks,
        SUM(spent) AS spend
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