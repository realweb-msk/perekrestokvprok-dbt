

  create or replace view `perekrestokvprok-bq`.`dbt_krepin`.`stg_zen_data_sheets`
  OPTIONS()
  as 

WITH source AS (
    SELECT DISTINCT
        PARSE_DATE('%d.%m.%Y', date) AS date,
        
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
 campaign_name,
        cost
    FROM `perekrestokvprok-bq`.`sheets_data`.`zen_sheets`
    WHERE date IS NOT NULL
),

final AS (
    SELECT
        ARRAY_TO_STRING([
            CAST(date AS STRING),
            campaign_name
        ],'') AS unique_key,
        date,
        campaign_name,
        IF(
          REGEXP_CONTAINS(campaign_name, r'[_\[]new[\]_]'),'UA','retargeting'
          ) AS campaign_type,
        COALESCE(SAFE_CAST(REGEXP_REPLACE(cost, r',', '.') AS FLOAT64), 0) AS cost
    FROM source
)

SELECT
    unique_key,
    date,
    campaign_name,
    campaign_type,
    cost
FROM final;

