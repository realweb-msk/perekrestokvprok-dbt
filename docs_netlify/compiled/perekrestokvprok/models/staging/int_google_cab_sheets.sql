WITH 
google_sheet AS (
    SELECT DISTINCT
        date,
        LOWER(campaign_name) campaign_name,
        IF(REGEXP_CONTAINS(campaign_name, r'\[old\]'),'retargeting','UA') AS campaign_type,
        adset_name,
        costs,
        installs,
        clicks,
        impressions
    FROM `perekrestokvprok-bq`.`sheets_data`.`google_ads_costs_and_installs`
    WHERE date IS NOT NULL
),

google_storage AS (
    SELECT DISTINCT
        date,
        campaign_name,
        campaign_type,
        adset_name,
        spend,
        installs,
        clicks,
        impressions
    FROM `perekrestokvprok-bq`.`dbt_production`.`stg_google_cab_sheets`
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
    REGEXP_REPLACE(campaign_name, r'\+|-', '_') AS campaign_name,
    campaign_type,
    adset_name,
    spend,
    installs,
    clicks,
    impressions
FROM final