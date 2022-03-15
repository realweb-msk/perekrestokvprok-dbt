

  create or replace view `perekrestokvprok-bq`.`dbt_production`.`int_asa_cab_meta`
  OPTIONS()
  as WITH source AS (
    SELECT
        Date,
        metadata_campaignName,
        impressions,
        taps,
        localSpend_amount
    FROM `perekrestokvprok-bq`.`MetaCustom`.`apple_search_ads_ASA_cost`
),

final AS (
    SELECT
        Date AS date,
        LOWER(metadata_campaignName) AS campaign_name,
        IF(REGEXP_CONTAINS(metadata_campaignName, r'\(R\)'),'retargeting','UA') AS campaign_type,
        '-' adset_name,
        SUM(impressions) AS impressions,
        SUM(taps) AS clicks,
        SUM(SAFE_CAST(localSpend_amount AS FLOAT64)) AS spend
    FROM source
    GROUP BY 1,2,3
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

