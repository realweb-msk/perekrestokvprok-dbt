

  create or replace view `perekrestokvprok-bq`.`dbt_krepin`.`stg_twitter_cab_sheets`
  OPTIONS()
  as 

WITH source AS (
    SELECT DISTINCT
        string_field_0,
        string_field_2,
        string_field_9,
        string_field_10,
        ROW_NUMBER() OVER(PARTITION BY string_field_0, string_field_2) AS counter
    FROM `perekrestokvprok-bq`.`sheets_data`.`twitter_sheets`
    WHERE string_field_0 IS NOT NULL
),

final AS (
    SELECT DISTINCT
        ARRAY_TO_STRING([
            CAST(DATE(REPLACE(string_field_0,'.','-')) AS STRING),
            LOWER(string_field_2)
        ],'') AS unique_key,
        DATE(REPLACE(string_field_0,'.','-')) AS date,
        LOWER(string_field_2) AS campaign_name,
        SAFE_CAST(REPLACE(string_field_9,',','.') AS FLOAT64) AS impressions,
        SAFE_CAST(REPLACE(string_field_10,',','.') AS FLOAT64) AS spend
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
    IF(REGEXP_CONTAINS(campaign_name, r'_old_'),'retargeting','UA') AS campaign_type,
    impressions,
    spend
FROM final



-- первый раз --
UNION ALL
SELECT DISTINCT
    ARRAY_TO_STRING([
      CAST(date AS STRING),
      LOWER(campaign_name)
      ],'') AS unique_key,
    date,
    LOWER(campaign_name) campaign_name,
    IF(REGEXP_CONTAINS(campaign_name, r'_old_'),'retargeting','UA') AS campaign_type,
    impressions,
    spend,
FROM `perekrestokvprok-bq`.`sheets_data`.`twitter_data`
WHERE date < (
  SELECT MIN(date)
  FROM final
)
AND date IS NOT NULL

;

