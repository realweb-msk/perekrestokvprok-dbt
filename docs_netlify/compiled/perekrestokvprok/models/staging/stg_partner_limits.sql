






SELECT
    start_date,
    end_date,
    partner,
    `limit` as limits,
    type,
    source
FROM `perekrestokvprok-bq`.`sheets_data`.`limits_sheet`
WHERE start_date IS NOT NULL