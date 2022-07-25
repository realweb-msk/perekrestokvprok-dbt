





WITH source AS (
    SELECT
        stat_time_day,
        LOWER(campaign_name) AS campaign_name,
        adgroup_name,
        impressions,
        reach,
        spend,
        clicks,
        total_purchase,
        total_achieve_level,
        app_install
    FROM `perekrestokvprok-bq`.`test2`.`tiktok_perek_settings_6952834783391023106`
),

final AS (
    SELECT
        ARRAY_TO_STRING([
            CAST(DATE(stat_time_day) AS STRING),
            campaign_name,
            adgroup_name
        ],'') AS unique_key,
        DATE(stat_time_day) AS date,
        campaign_name,
        IF(REGEXP_CONTAINS(campaign_name, r'_ret_'),'retargeting','UA') AS campaign_type,
        adgroup_name AS adset_name,
        impressions,
        reach,
        spend,
        clicks,
        total_purchase AS purchase,
        total_achieve_level AS first_purchase,
        app_install
    FROM source
)

SELECT
    unique_key,
    date,
    campaign_name,
    campaign_type,
    adset_name,
    impressions,
    reach,
    spend,
    clicks,
    purchase,
    first_purchase,
    app_install
FROM final

