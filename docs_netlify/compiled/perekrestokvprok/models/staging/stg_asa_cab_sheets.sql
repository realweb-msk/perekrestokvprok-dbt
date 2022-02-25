





WITH source AS (
    SELECT DISTINCT
        date,
        campaign,
        Clikcs AS clicks,
        spend,
        installs,
        ROW_NUMBER() OVER(PARTITION BY date, campaign) AS counter
    FROM `perekrestokvprok-bq`.`sheets_data`.`Asa_cost`
    WHERE date IS NOT NULL
),

final AS (
    SELECT DISTINCT
        ARRAY_TO_STRING([
            CAST(date AS string),
            LOWER(campaign)
        ],'') AS unique_key,
        date,
        LOWER(campaign) AS campaign_name,
        clicks,
        spend,
        installs
    FROM source
    WHERE counter = 1
)

SELECT DISTINCT
    unique_key,
    date,
    
    TRIM(
        REGEXP_REPLACE(
            LOWER(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(campaign_name,  r'(_install_week.*)', '_install_weekend'), 
                    r'(_install_promo_gl.*)', '_install_promo_global'), 
                r'(_install_promo_re.*)', '_install_promo_regular')
            ), r'\+|-', '_')
        )
 AS campaign_name,
    IF(REGEXP_CONTAINS(campaign_name, r'\(R\)'),'retargeting','UA') AS campaign_type,
    '-' adset_name,
    clicks,
    spend,
    installs,
    0 impressions
FROM final

