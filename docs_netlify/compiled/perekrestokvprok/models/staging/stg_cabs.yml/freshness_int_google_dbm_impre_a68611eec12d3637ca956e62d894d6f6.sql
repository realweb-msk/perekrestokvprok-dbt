


    -- фейлится если в рабочие дни данные за вчера не обновлены
    WITH source AS (
    SELECT
        DATE(MAX(date)) AS max_date
    FROM `perekrestokvprok-bq`.`dbt_lazuta`.`int_google_dbm_impressions_clicks_revenue_meta`
    ),

    mistakes AS (
        SELECT max_date
        FROM source
        WHERE max_date < DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
    )

    SELECT *    
    FROM mistakes



