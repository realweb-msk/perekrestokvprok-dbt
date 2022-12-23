

  create or replace view `perekrestokvprok-bq`.`dbt_krepin`.`stg_campaign_limits`
  OPTIONS()
  as 


SELECT
    start_date,
    CAST(end_date as DATE) as end_date,
    campaign,
    `limit` as limits,
FROM `perekrestokvprok-bq`.`sheets_data`.`campaign_limits`
WHERE start_date IS NOT NULL;

