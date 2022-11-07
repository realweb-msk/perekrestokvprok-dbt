

  create or replace view `perekrestokvprok-bq`.`dbt_production`.`int_rate_for_inside`
  OPTIONS()
  as WITH source_t AS (
    SELECT
        start_date,
        end_date,
        partner,
        platform,
        rate_for_partner,
        rate_for_us,
        plan_f_p,
        type,
        source,
        base
    FROM `perekrestokvprok-bq`.`dbt_production`.`stg_rate_info`
),

rate_array AS (
    SELECT 
        GENERATE_DATE_ARRAY(start_date,end_date) AS period, 
        partner,
        platform,
        rate_for_partner,
        rate_for_us,
        plan_f_p,
        type,
        source,
        base
    FROM source_t
),

rate AS (
    SELECT
        period,
        partner,
        platform,
        rate_for_partner,
        rate_for_us,
        plan_f_p,
        type,
        source,
        base,
        rate_for_us * plan_f_p AS prt_budget,
        SUM((rate_for_us - rate_for_partner) * plan_f_p) OVER(PARTITION BY period) AS plan_mrg,
        SUM((rate_for_us - rate_for_partner) * plan_f_p) OVER(PARTITION BY period, partner) AS prt_plan_mrg,
    FROM rate_array, UNNEST(period) AS period
),

final AS (
    SELECT
        period,
        partner,
        platform,
        rate_for_partner,
        rate_for_us,
        plan_f_p,
        type,
        source,
        base,
        prt_budget,
        plan_mrg,
        prt_plan_mrg,
    FROM rate
)

SELECT 
    period,
    partner,
    platform,
    rate_for_partner,
    rate_for_us,
    plan_f_p,
    type,
    source,
    base,
    prt_budget,
    plan_mrg,
    prt_plan_mrg,
FROM final;

