

  create or replace view `perekrestokvprok-bq`.`dbt_production`.`int_partner_limits`
  OPTIONS()
  as WITH source AS (
    SELECT
        start_date,
        end_date,
        partner,
        limits,
        type
    FROM `perekrestokvprok-bq`.`dbt_production`.`stg_partner_limits`
),

limits_array AS (
    SELECT 
        GENERATE_DATE_ARRAY(start_date,end_date) AS period, 
        partner,
        limits,
        type
    FROM source
)

SELECT 
    period,
    partner,
    limits,
    type
FROM limits_array, UNNEST(period) AS period;

