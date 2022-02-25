
        
        
    

    

    merge into `perekrestokvprok-bq`.`dbt_production`.`stg_asa_cab_sheets` as DBT_INTERNAL_DEST
        using (
          





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


        ) as DBT_INTERNAL_SOURCE
        on 
            DBT_INTERNAL_SOURCE.unique_key = DBT_INTERNAL_DEST.unique_key
        

    
    when matched then update set
        `unique_key` = DBT_INTERNAL_SOURCE.`unique_key`,`date` = DBT_INTERNAL_SOURCE.`date`,`campaign_name` = DBT_INTERNAL_SOURCE.`campaign_name`,`campaign_type` = DBT_INTERNAL_SOURCE.`campaign_type`,`adset_name` = DBT_INTERNAL_SOURCE.`adset_name`,`clicks` = DBT_INTERNAL_SOURCE.`clicks`,`spend` = DBT_INTERNAL_SOURCE.`spend`,`installs` = DBT_INTERNAL_SOURCE.`installs`,`impressions` = DBT_INTERNAL_SOURCE.`impressions`
    

    when not matched then insert
        (`unique_key`, `date`, `campaign_name`, `campaign_type`, `adset_name`, `clicks`, `spend`, `installs`, `impressions`)
    values
        (`unique_key`, `date`, `campaign_name`, `campaign_type`, `adset_name`, `clicks`, `spend`, `installs`, `impressions`)


  