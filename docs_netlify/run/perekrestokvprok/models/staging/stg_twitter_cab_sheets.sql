
        
            
            
        
    

    

    merge into `perekrestokvprok-bq`.`dbt_production`.`stg_twitter_cab_sheets` as DBT_INTERNAL_DEST
        using (
          





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


        ) as DBT_INTERNAL_SOURCE
        on 
                DBT_INTERNAL_SOURCE.unique_key = DBT_INTERNAL_DEST.unique_key
            

    
    when matched then update set
        `unique_key` = DBT_INTERNAL_SOURCE.`unique_key`,`date` = DBT_INTERNAL_SOURCE.`date`,`campaign_name` = DBT_INTERNAL_SOURCE.`campaign_name`,`campaign_type` = DBT_INTERNAL_SOURCE.`campaign_type`,`impressions` = DBT_INTERNAL_SOURCE.`impressions`,`spend` = DBT_INTERNAL_SOURCE.`spend`
    

    when not matched then insert
        (`unique_key`, `date`, `campaign_name`, `campaign_type`, `impressions`, `spend`)
    values
        (`unique_key`, `date`, `campaign_name`, `campaign_type`, `impressions`, `spend`)


    