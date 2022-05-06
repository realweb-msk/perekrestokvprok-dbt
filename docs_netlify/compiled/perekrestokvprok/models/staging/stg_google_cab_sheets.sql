

WITH source AS (
    SELECT DISTINCT
        date,
        LOWER(campaign_name) campaign_name,
        adset_name,
        costs,
        installs,
        clicks,
        impressions
    FROM `perekrestokvprok-bq`.`sheets_data`.`google_ads_costs_and_installs`
    WHERE date IS NOT NULL
),

final AS (
    SELECT
        ARRAY_TO_STRING([
            CAST(date AS STRING),
            campaign_name,
            adset_name
        ],'') AS unique_key,
        date,
        campaign_name,
        IF(
          REGEXP_CONTAINS(campaign_name, r'[_\[]old[\]_]'),
          'retargeting','UA') AS campaign_type,
        adset_name,
        costs AS spend,
        installs,
        clicks,
        impressions
    FROM source
)

SELECT
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
    campaign_type,
    adset_name,
    spend,
    installs,
    clicks,
    impressions
FROM final

-- 

-- -- первый раз --
-- UNION ALL
-- SELECT DISTINCT
--     ARRAY_TO_STRING([
--             CAST(date AS STRING),
--             LOWER(campaign_name),
--             adset_name
--     ],'') AS unique_key,
--     date,
--     LOWER(campaign_name) campaign_name,
--     IF(REGEXP_CONTAINS(campaign_name, r'\[old\]'),'retargeting','UA') AS campaign_type,
--     adset_name,
--     costs AS spend,
--     installs,
--     clicks,
--     impressions
-- FROM `perekrestokvprok-bq`.`agg_data`.`google_ads_costs_and_installs_sum`
-- WHERE date NOT IN  < (
--   SELECT DISTINCT date
--   FROM final
-- )
-- AND date IS NOT NULL

-- 