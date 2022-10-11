
        
        
    

    

    merge into `perekrestokvprok-bq`.`dbt_production`.`stg_google_cab_sheets` as DBT_INTERNAL_DEST
        using (
          





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
        ) as DBT_INTERNAL_SOURCE
        on 
            DBT_INTERNAL_SOURCE.unique_key = DBT_INTERNAL_DEST.unique_key
        

    
    when matched then update set
        `unique_key` = DBT_INTERNAL_SOURCE.`unique_key`,`date` = DBT_INTERNAL_SOURCE.`date`,`campaign_name` = DBT_INTERNAL_SOURCE.`campaign_name`,`campaign_type` = DBT_INTERNAL_SOURCE.`campaign_type`,`adset_name` = DBT_INTERNAL_SOURCE.`adset_name`,`spend` = DBT_INTERNAL_SOURCE.`spend`,`installs` = DBT_INTERNAL_SOURCE.`installs`,`clicks` = DBT_INTERNAL_SOURCE.`clicks`,`impressions` = DBT_INTERNAL_SOURCE.`impressions`
    

    when not matched then insert
        (`unique_key`, `date`, `campaign_name`, `campaign_type`, `adset_name`, `spend`, `installs`, `clicks`, `impressions`)
    values
        (`unique_key`, `date`, `campaign_name`, `campaign_type`, `adset_name`, `spend`, `installs`, `clicks`, `impressions`)


  