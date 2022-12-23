

  create or replace view `perekrestokvprok-bq`.`dbt_krepin`.`int_twitter_cab`
  OPTIONS()
  as WITH 
twitter_sheet AS (
    SELECT DISTINCT
        DATE(REPLACE(string_field_0,'.','-')) AS date,
        LOWER(string_field_2) AS campaign_name,
        IF(REGEXP_CONTAINS(LOWER(string_field_2), r'_old_'),'retargeting','UA') AS campaign_type,
        SAFE_CAST(REPLACE(string_field_9,',','.') AS FLOAT64) AS impressions,
        SAFE_CAST(REPLACE(string_field_10,',','.') AS FLOAT64) AS spend
    FROM `perekrestokvprok-bq`.`sheets_data`.`twitter_sheets`
),

twitter_storage AS (
    SELECT DISTINCT
        date,
        campaign_name,
        campaign_type,
        impressions,
        spend
    FROM `perekrestokvprok-bq`.`dbt_krepin`.`stg_twitter_cab_sheets`
    WHERE date < (
        SELECT MIN(date)
        FROM twitter_sheet
    )
),

final AS (
    SELECT *
    FROM twitter_storage
    UNION DISTINCT 
    SELECT *
    FROM twitter_sheet
)

SELECT 
    date,
    REGEXP_REPLACE(campaign_name, r'\+|-', '_') AS campaign_name,
    '-' adset_name,
    campaign_type,
    impressions,
    spend
FROM final;

