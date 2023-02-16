
  
    

    create or replace table `perekrestokvprok-bq`.`dbt_production`.`stg_appnext_cost`
    
    
    OPTIONS()
    as (
      





SELECT
DISTINCT
    date,
    campaign_name,
    type,
    cost
FROM `perekrestokvprok-bq`.`sheets_data`.`appnext_cost`
    );
  