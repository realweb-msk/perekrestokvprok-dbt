
        
            
            
        
    

    

    merge into `perekrestokvprok-bq`.`dbt_production`.`stg_tiktok_cab_meta` as DBT_INTERNAL_DEST
        using (
          select * from `perekrestokvprok-bq`.`dbt_production`.`stg_tiktok_cab_meta__dbt_tmp`
        ) as DBT_INTERNAL_SOURCE
        on 
                DBT_INTERNAL_SOURCE.unique_key = DBT_INTERNAL_DEST.unique_key
            

    
    when matched then update set
        `unique_key` = DBT_INTERNAL_SOURCE.`unique_key`,`date` = DBT_INTERNAL_SOURCE.`date`,`campaign_name` = DBT_INTERNAL_SOURCE.`campaign_name`,`campaign_type` = DBT_INTERNAL_SOURCE.`campaign_type`,`adset_name` = DBT_INTERNAL_SOURCE.`adset_name`,`impressions` = DBT_INTERNAL_SOURCE.`impressions`,`reach` = DBT_INTERNAL_SOURCE.`reach`,`spend` = DBT_INTERNAL_SOURCE.`spend`,`clicks` = DBT_INTERNAL_SOURCE.`clicks`,`purchase` = DBT_INTERNAL_SOURCE.`purchase`,`first_purchase` = DBT_INTERNAL_SOURCE.`first_purchase`,`app_install` = DBT_INTERNAL_SOURCE.`app_install`
    

    when not matched then insert
        (`unique_key`, `date`, `campaign_name`, `campaign_type`, `adset_name`, `impressions`, `reach`, `spend`, `clicks`, `purchase`, `first_purchase`, `app_install`)
    values
        (`unique_key`, `date`, `campaign_name`, `campaign_type`, `adset_name`, `impressions`, `reach`, `spend`, `clicks`, `purchase`, `first_purchase`, `app_install`)


    