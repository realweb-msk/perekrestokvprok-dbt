

  create or replace view `perekrestokvprok-bq`.`dbt_production`.`int_yandex_cab_meta`
  OPTIONS()
  as WITH source_1 AS (
    SELECT
        Date,
        CampaignName,
        AdGroupName,
        Impressions,
        Clicks,
        Cost
    FROM `perekrestokvprok-bq`.`MetaCustom`.`yandex_direct_ad_keyword_stat_x5perek_direct`
    WHERE Date > '2021-02-01'
),

source_2 AS (
    SELECT
        Date,
        CampaignName,
        AdGroupName,
        Impressions,
        Clicks,
        Cost
    FROM `perekrestokvprok-bq`.`MetaCustom`.`yandex_direct_ad_keyword_stat_perek_vprok_sf`
    WHERE Date >= '2022-04-28' and Date <= '2022-05-05'
),

source AS (
    SELECT * FROM source_1
    UNION ALL
    SELECT * FROM source_2
),

final AS (
    SELECT
        date,
        LOWER(REPLACE(REPLACE(CampaignName,'+','_'),'-','_')) AS campaign_name,
        IF(REGEXP_CONTAINS(CampaignName, r'ret'),'retargeting','UA') AS campaign_type,
        AdGroupName AS adset_name,
        SUM(Impressions) AS impressions,
        SUM(Clicks) AS clicks,
        SUM(SAFE_CAST(SAFE_DIVIDE(Cost, 1.2) AS FLOAT64)) AS spend
    FROM source
    GROUP BY 1,2,3,4
)

SELECT
    date,
    campaign_name,
    campaign_type,
    adset_name,
    impressions,
    clicks,
    spend
FROM final;

