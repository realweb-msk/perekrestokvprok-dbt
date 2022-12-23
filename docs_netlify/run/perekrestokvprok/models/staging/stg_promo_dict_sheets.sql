

  create or replace view `perekrestokvprok-bq`.`dbt_krepin`.`stg_promo_dict_sheets`
  OPTIONS()
  as 


SELECT DISTINCT
    LOWER(promo) AS promo,
    name
FROM `perekrestokvprok-bq`.`sheets_data`.`promo_dict_sheets`
WHERE promo IS NOT NULL;

