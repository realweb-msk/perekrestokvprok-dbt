WITH source AS (
    SELECT
        date,
        campaign.name,
        base.shows,
        base.clicks,
        base.spent
    FROM {{ source('MetaCustom', 'mytarget_banner_stat_d0927adb05_agency_client') }}
),

final AS (
    SELECT
        date,
        LOWER(name) AS campaign_name,
        '-' adset_name,
        SUM(shows) AS impressions,
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