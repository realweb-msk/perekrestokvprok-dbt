
  
    

    create or replace table `perekrestokvprok-bq`.`dbt_production`.`stg_promo_sheets`
    
    
    OPTIONS()
    as (
      






SELECT
    date_start,
    date_end,
    promocod AS promocode,
    type,
    Channel AS channel
FROM `perekrestokvprok-bq`.`sheets_data`.`promo_sheets`
WHERE date_start IS NOT NULL
    );
  