

  create or replace table `perekrestokvprok-bq`.`dbt_production`.`stg_budget_and_plan`
  
  
  OPTIONS()
  as (
    






SELECT
    start_date,
    end_date,
    plan_budget,
    plan_type,
    plan_order
FROM `perekrestokvprok-bq`.`sheets_data`.`budget_and_plan`
WHERE start_date IS NOT NULL
  );
  