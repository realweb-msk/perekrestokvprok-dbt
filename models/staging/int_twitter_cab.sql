WITH 
twitter_sheet AS (
    SELECT DISTINCT
        DATE(REPLACE(string_field_0,'.','-')) AS date,
        LOWER(string_field_2) AS campaign_name,
        IF(REGEXP_CONTAINS(LOWER(string_field_2), r'_old_'),'retargeting','UA') AS campaign_type,
        SAFE_CAST(REPLACE(string_field_9,',','.') AS FLOAT64) AS impressions,
        SAFE_CAST(REPLACE(string_field_10,',','.') AS FLOAT64) AS spend
    FROM {{ source('sheets_data', 'twitter_sheets') }}
),

twitter_storage AS (
    SELECT DISTINCT
        date,
        campaign_name,
        campaign_type,
        impressions,
        spend
    FROM {{ ref('stg_twitter_cab_sheets') }}
    WHERE date < (
        SELECT MIN(date)
        FROM twitter_sheet
    )
),

final AS (
    SELECT *
    FROM twitter_storage
    UNION DISTINCT 
    SELECT *
    FROM twitter_sheet
)

SELECT 
    date,
    campaign_name,
    campaign_type,
    impressions,
    spend
FROM final