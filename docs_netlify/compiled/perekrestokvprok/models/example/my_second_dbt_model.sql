-- Use the `ref` function to select from other models

select *
from `perekrestokvprok-bq`.`dbt_lazuta`.`my_first_dbt_model`
where id = 1