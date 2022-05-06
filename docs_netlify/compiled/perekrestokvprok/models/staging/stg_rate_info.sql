


SELECT
    start_date,
    end_date,
    partner,
    platform,
    rate_for_parthner AS rate_for_partner,
    plan_f_p,
    rate_for_us,
    type,
    source
FROM `perekrestokvprok-bq`.`sheets_data`.`rate_info`
WHERE start_date IS NOT NULL