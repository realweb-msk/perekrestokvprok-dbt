

  create or replace view `perekrestokvprok-bq`.`dbt_krepin`.`int_vk_cab_meta`
  OPTIONS()
  as WITH source_1 AS (
    SELECT
        day,
        campaign.name,
        impressions,
        clicks,
        spent
    FROM `perekrestokvprok-bq`.`MetaCustom`.`vk_campaign_stat_1900013586_1605495720`
),

source_2 AS (
    SELECT
        day,
        campaign.name,
        impressions,
        clicks,
        spent
    FROM `perekrestokvprok-bq`.`MetaCustom`.`vk_campaign_stat_1900013586_1607141417`
),

unions AS (
    SELECT * FROM source_1
    UNION DISTINCT
    SELECT * FROM source_2
),

final AS (
    SELECT
        day AS date,
        LOWER(REPLACE(REPLACE(name,'+','_'),'-','_')) AS campaign_name,
        IF(REGEXP_CONTAINS(name, r'old'),'retargeting','UA') AS campaign_type,
        '-' adset_name,
        SUM(impressions) AS impressions,
        SUM(clicks) AS clicks,
        SUM(spent) AS spend
    FROM unions
    WHERE REGEXP_CONTAINS(name, 'realweb')
    GROUP BY 1,2,3
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

