

  create or replace view `perekrestokvprok-bq`.`dbt_production`.`my_second_dbt_model`
  OPTIONS()
  as -- Use the `ref` function to select from other models

select *
from `perekrestokvprok-bq`.`dbt_production`.`my_first_dbt_model`
where id = 1;

