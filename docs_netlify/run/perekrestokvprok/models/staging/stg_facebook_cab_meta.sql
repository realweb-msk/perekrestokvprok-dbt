
        
            
            
        
    

    

    merge into `perekrestokvprok-bq`.`dbt_production`.`stg_facebook_cab_meta` as DBT_INTERNAL_DEST
        using (
          





WITH source AS (
  SELECT
      date_start,
      lower(campaign_name) as campaign_name,
      IF(REGEXP_CONTAINS(campaign_name, r'\[old\]'),'retargeting','UA') AS campaign_type,
      adset_name,
      ad_name,
      impressions,
      clicks,
      spend,
      actions,
      action_values,
      conversions,
      conversion_values
  FROM  `perekrestokvprok-bq`.`test2`.`facebook_ads_ad_stat_minimal_134923481805102`
  -- добавить после всех проверок
  -- 
  -- WHERE date_start = DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY)
  -- 
),

unnests AS (
  SELECT 
    ARRAY_TO_STRING([
      CAST(date_start AS STRING),
      campaign_name,
      adset_name,
      ad_name
      ],'') AS unique_key,
    date_start AS date,
    campaign_name,
    campaign_type,
    adset_name,
    ad_name,
    SUM(impressions) AS impressions,
    SUM(clicks) AS clicks,
    SUM((
        SELECT value
        FROM UNNEST(actions)
        WHERE action_type = 'mobile_app_install'
        --'omni_app_install' не трекается
    )) AS installs,
    sum(spend) AS spend,
    SUM((
        SELECT value
        FROM UNNEST(actions)
        where action_type = 'app_custom_event.fb_mobile_purchase'
    )) AS purchase,
    SUM((
        SELECT value
        FROM UNNEST(action_values)
        where action_type = 'app_custom_event.fb_mobile_purchase'
    )) AS revenue,
    SUM((
        SELECT value
        FROM UNNEST(conversions)
        WHERE action_type = 'start_trial_mobile_app' 
        --'start_trial_total не трекается
    )) AS first_purchase,
    SUM((
        SELECT value
        FROM UNNEST(conversion_values)
        WHERE action_type = 'start_trial_total'
    )) AS first_purchase_revenue,
    SUM((
        SELECT value
        FROM UNNEST(actions)
        where action_type = 'app_custom_event.fb_mobile_add_to_cart'
    )) AS add_to_cart,
  FROM source
  GROUP BY 1,2,3,4,5,6
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
FROM unnests


        ) as DBT_INTERNAL_SOURCE
        on 
                DBT_INTERNAL_SOURCE.unique_key = DBT_INTERNAL_DEST.unique_key
            

    
    when matched then update set
        `unique_key` = DBT_INTERNAL_SOURCE.`unique_key`,`date` = DBT_INTERNAL_SOURCE.`date`,`campaign_name` = DBT_INTERNAL_SOURCE.`campaign_name`,`campaign_type` = DBT_INTERNAL_SOURCE.`campaign_type`,`adset_name` = DBT_INTERNAL_SOURCE.`adset_name`,`ad_name` = DBT_INTERNAL_SOURCE.`ad_name`,`impressions` = DBT_INTERNAL_SOURCE.`impressions`,`clicks` = DBT_INTERNAL_SOURCE.`clicks`,`installs` = DBT_INTERNAL_SOURCE.`installs`,`spend` = DBT_INTERNAL_SOURCE.`spend`,`purchase` = DBT_INTERNAL_SOURCE.`purchase`,`revenue` = DBT_INTERNAL_SOURCE.`revenue`,`first_purchase` = DBT_INTERNAL_SOURCE.`first_purchase`,`first_purchase_revenue` = DBT_INTERNAL_SOURCE.`first_purchase_revenue`,`add_to_cart` = DBT_INTERNAL_SOURCE.`add_to_cart`
    

    when not matched then insert
        (`unique_key`, `date`, `campaign_name`, `campaign_type`, `adset_name`, `ad_name`, `impressions`, `clicks`, `installs`, `spend`, `purchase`, `revenue`, `first_purchase`, `first_purchase_revenue`, `add_to_cart`)
    values
        (`unique_key`, `date`, `campaign_name`, `campaign_type`, `adset_name`, `ad_name`, `impressions`, `clicks`, `installs`, `spend`, `purchase`, `revenue`, `first_purchase`, `first_purchase_revenue`, `add_to_cart`)


    