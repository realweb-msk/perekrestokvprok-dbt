

  create or replace table `perekrestokvprok-bq`.`dbt_production`.`stg_huawei_cab_sheets`
  partition by date
  cluster by campaign_type
  OPTIONS()
  as (
    





WITH source AS (
    SELECT DISTINCT
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
 campaign_name,
        status,
        cost,
        clicks,
        impressions,
        activations,
        campaign_type AS type,
        exchange_rate
    FROM `perekrestokvprok-bq`.`sheets_data`.`huawei_data`
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
        type,
        status,
        activations,
        cost,
        cost * exchange_rate AS spend,
        clicks,
        impressions,
        exchange_rate
    FROM source
)

SELECT
    unique_key,
    date,
    campaign_name,
    campaign_type,
    type,
    status,
    activations,
    cost,
    spend,
    clicks,
    impressions,
    exchange_rate
FROM final
  );
  