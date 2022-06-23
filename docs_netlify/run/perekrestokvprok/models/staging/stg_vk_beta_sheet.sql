

  create or replace table `perekrestokvprok-bq`.`dbt_production`.`stg_vk_beta_sheet`
  partition by date
  cluster by campaign_type
  OPTIONS()
  as (
    





WITH source AS (
    SELECT
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
        SUM(COALESCE(SAFE_CAST(REGEXP_REPLACE(REGEXP_REPLACE(cost, r',', '.'), r' ', '') AS FLOAT64), 0)) as cost
    FROM perekrestokvprok-bq.sheets_data.vk_beta_sheet
    WHERE date IS NOT NULL
    GROUP BY date, campaign_name
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
          REGEXP_CONTAINS(campaign_name, r'[_\[]new[\]_]|_new_install_'),'UA','retargeting'
          ) AS campaign_type,
        cost
    FROM source
)

SELECT
    unique_key,
    date,
    campaign_name,
    campaign_type,
    cost
FROM final
  );
  