





WITH source AS (
  SELECT DISTINCT
        ARRAY_TO_STRING([
        CAST(date AS STRING),
            lower(campaign),
            adset,
            ad
            ],'') AS unique_key,
        date,
        lower(campaign) campaign_name,
        IF(REGEXP_CONTAINS(lower(campaign), r'\[old\]'),'retargeting','UA') AS campaign_type,
        adset adset_name,
        ad ad_name,
        show AS impressions,
        clicks,
        installs,
        spend,
        purchase,
        purchase_revenue revenue,
        first_purchase,
        first_purchase_revenue,
        ad_to_cars add_to_cart,
        ROW_NUMBER() OVER(PARTITION BY date, campaign, adset, ad) AS counter
    FROM `perekrestokvprok-bq`.`sheets_data`.`fb_spreadsheets_data`
)

SELECT
    unique_key,
    date,
    
    TRIM(
        REGEXP_REPLACE(
            LOWER(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(campaign_name,  r'(_install_week.*)', '_install_weekend'), 
                    r'(_install_promo_gl.*)', '_install_promo_global'), 
                r'(_install_promo_re.*)', '_install_promo_regular')
            ), r'\+|-', '_')
        )
 AS campaign_name,
    campaign_type,
    adset_name,
    ad_name,
    impressions,
    clicks,
    installs,
    spend,
    purchase,
    revenue,
    first_purchase,
    first_purchase_revenue,
    add_to_cart
FROM source
WHERE counter = 1

