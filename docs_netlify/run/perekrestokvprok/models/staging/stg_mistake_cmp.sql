

  create or replace view `perekrestokvprok-bq`.`dbt_lazuta`.`stg_mistake_cmp`
  OPTIONS()
  as 


SELECT
    mistake,
    correct
FROM `perekrestokvprok-bq`.`sheets_data`.`mistake_cmp`
WHERE mistake IS NOT NULL;

