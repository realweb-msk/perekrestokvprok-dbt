WITH source AS (
    SELECT
        start_date,
        end_date,
        plan_budget,
        plan_type,
        plan_order
    FROM `perekrestokvprok-bq`.`dbt_lazuta`.`stg_budget_and_plan`
),

array_table AS (
    SELECT 
        GENERATE_DATE_ARRAY(start_date,end_date) AS date, 
        plan_budget, 
        plan_type, 
        plan_order 
    FROM source
),

plans AS (
    SELECT
        date,
        plan_budget,
        plan_order,
        CASE
            WHEN plan_type = "UA" THEN "uac"
            WHEN plan_type = "PROMO" THEN "promo"
            WHEN plan_type = "RTG" THEN "rtg"
            ELSE '-' END AS plan_type
    FROM array_table, UNNEST(date) AS date
)

SELECT
    date,
    plan_budget,
    plan_order,
    plan_type
FROM plans