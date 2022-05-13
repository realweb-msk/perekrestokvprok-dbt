
        
        
    

    

    merge into `perekrestokvprok-bq`.`dbt_production`.`stg_huawei_cab_sheets` as DBT_INTERNAL_DEST
        using (
          





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
        ) as DBT_INTERNAL_SOURCE
        on 
            DBT_INTERNAL_SOURCE.unique_key = DBT_INTERNAL_DEST.unique_key
        

    
    when matched then update set
        `unique_key` = DBT_INTERNAL_SOURCE.`unique_key`,`date` = DBT_INTERNAL_SOURCE.`date`,`campaign_name` = DBT_INTERNAL_SOURCE.`campaign_name`,`campaign_type` = DBT_INTERNAL_SOURCE.`campaign_type`,`type` = DBT_INTERNAL_SOURCE.`type`,`status` = DBT_INTERNAL_SOURCE.`status`,`activations` = DBT_INTERNAL_SOURCE.`activations`,`cost` = DBT_INTERNAL_SOURCE.`cost`,`spend` = DBT_INTERNAL_SOURCE.`spend`,`clicks` = DBT_INTERNAL_SOURCE.`clicks`,`impressions` = DBT_INTERNAL_SOURCE.`impressions`,`exchange_rate` = DBT_INTERNAL_SOURCE.`exchange_rate`
    

    when not matched then insert
        (`unique_key`, `date`, `campaign_name`, `campaign_type`, `type`, `status`, `activations`, `cost`, `spend`, `clicks`, `impressions`, `exchange_rate`)
    values
        (`unique_key`, `date`, `campaign_name`, `campaign_type`, `type`, `status`, `activations`, `cost`, `spend`, `clicks`, `impressions`, `exchange_rate`)


  
