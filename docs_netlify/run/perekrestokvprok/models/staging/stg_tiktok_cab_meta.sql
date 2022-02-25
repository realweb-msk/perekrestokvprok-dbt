
        
        
    

    

    merge into `perekrestokvprok-bq`.`dbt_production`.`stg_tiktok_cab_meta` as DBT_INTERNAL_DEST
        using (
          





WITH source AS (
    SELECT
        stat_time_day,
        LOWER(campaign_name) AS campaign_name,
        adgroup_name,
        impressions,
        reach,
        spend,
        clicks,
        total_purchase,
        total_achieve_level
    FROM `perekrestokvprok-bq`.`test2`.`tiktok_perek_settings_6952834783391023106`
),

final AS (
    SELECT
        ARRAY_TO_STRING([
            CAST(DATE(stat_time_day) AS STRING),
            campaign_name,
            adgroup_name
        ],'') AS unique_key,
        DATE(stat_time_day) AS date,
        campaign_name,
        IF(REGEXP_CONTAINS(campaign_name, r'_ret_'),'retargeting','UA') AS campaign_type,
        adgroup_name AS adset_name,
        impressions,
        reach,
        spend,
        clicks,
        total_purchase AS purchase,
        total_achieve_level AS first_purchase
    FROM source
)

SELECT
    unique_key,
    date,
    campaign_name,
    campaign_type,
    adset_name,
    impressions,
    reach,
    spend,
    clicks,
    purchase,
    first_purchase
FROM final


        ) as DBT_INTERNAL_SOURCE
        on 
            DBT_INTERNAL_SOURCE.unique_key = DBT_INTERNAL_DEST.unique_key
        

    
    when matched then update set
        `unique_key` = DBT_INTERNAL_SOURCE.`unique_key`,`date` = DBT_INTERNAL_SOURCE.`date`,`campaign_name` = DBT_INTERNAL_SOURCE.`campaign_name`,`campaign_type` = DBT_INTERNAL_SOURCE.`campaign_type`,`adset_name` = DBT_INTERNAL_SOURCE.`adset_name`,`impressions` = DBT_INTERNAL_SOURCE.`impressions`,`reach` = DBT_INTERNAL_SOURCE.`reach`,`spend` = DBT_INTERNAL_SOURCE.`spend`,`clicks` = DBT_INTERNAL_SOURCE.`clicks`,`purchase` = DBT_INTERNAL_SOURCE.`purchase`,`first_purchase` = DBT_INTERNAL_SOURCE.`first_purchase`
    

    when not matched then insert
        (`unique_key`, `date`, `campaign_name`, `campaign_type`, `adset_name`, `impressions`, `reach`, `spend`, `clicks`, `purchase`, `first_purchase`)
    values
        (`unique_key`, `date`, `campaign_name`, `campaign_type`, `adset_name`, `impressions`, `reach`, `spend`, `clicks`, `purchase`, `first_purchase`)


  