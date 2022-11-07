
        
        
    

    

    merge into `perekrestokvprok-bq`.`dbt_production`.`stg_zen_data_sheets` as DBT_INTERNAL_DEST
        using (
          





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
FROM final
        ) as DBT_INTERNAL_SOURCE
        on 
            DBT_INTERNAL_SOURCE.unique_key = DBT_INTERNAL_DEST.unique_key
        

    
    when matched then update set
        `unique_key` = DBT_INTERNAL_SOURCE.`unique_key`,`date` = DBT_INTERNAL_SOURCE.`date`,`campaign_name` = DBT_INTERNAL_SOURCE.`campaign_name`,`campaign_type` = DBT_INTERNAL_SOURCE.`campaign_type`,`cost` = DBT_INTERNAL_SOURCE.`cost`
    

    when not matched then insert
        (`unique_key`, `date`, `campaign_name`, `campaign_type`, `cost`)
    values
        (`unique_key`, `date`, `campaign_name`, `campaign_type`, `cost`)


  