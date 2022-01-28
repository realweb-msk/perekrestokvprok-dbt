WITH 
google_sheet AS (
    SELECT DISTINCT
        date,
        LOWER(campaign_name) campaign_name,
        adset_name,
        costs,
        installs,
        clicks,
        impressions
    FROM {{ source('sheets_data', 'google_ads_costs_and_installs') }}
    WHERE date IS NOT NULL
),

google_storage AS (
    SELECT DISTINCT
        date,
        campaign_name,
        adset_name,
        spend,
        installs,
        clicks,
        impressions
    FROM {{ ref('stg_google_cab_sheets') }}
    WHERE date < (
        SELECT MIN(date)
        FROM google_sheet
    )
),

final AS (
    SELECT *
    FROM google_storage
    UNION DISTINCT 
    SELECT *
    FROM google_sheet
)

SELECT 
    date,
    campaign_name,
    adset_name,
    spend,
    installs,
    clicks,
    impressions
FROM final