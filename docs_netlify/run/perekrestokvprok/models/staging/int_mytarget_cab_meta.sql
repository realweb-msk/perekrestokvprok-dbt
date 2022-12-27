

  create or replace view `perekrestokvprok-bq`.`dbt_production`.`int_mytarget_cab_meta`
  OPTIONS()
  as WITH source AS (
    SELECT
        date,
        campaign.name,
        base.shows,
        base.clicks,
        base.spent
    FROM `perekrestokvprok-bq`.`MetaCustom`.`mytarget_banner_stat_d0927adb05_agency_client`
),

final AS (
    SELECT
        date,
        LOWER(name) AS campaign_name,
        IF(REGEXP_CONTAINS(name, r'new'),'UA','retargeting') AS campaign_type,
        '-' adset_name,
        SUM(shows) AS impressions,
        SUM(clicks) AS clicks,
        SUM(spent) AS spend
    FROM source
    GROUP BY 1,2,3,4
)

SELECT
    date,
    REGEXP_REPLACE(campaign_name, r'\+|-', '_') AS campaign_name,
    campaign_type,
    adset_name,
    impressions,
    clicks,
    spend
FROM final;

