

  create or replace view `perekrestokvprok-bq`.`dbt_lazuta`.`dim_rtg_agg`
  OPTIONS()
  as /* 
для лучшего понимания лучше заглянуть сюда: https://github.com/realweb-msk/perekrestokvprok-dbt
или сюда: https://brave-hermann-395dc3.netlify.app/#!/model/model.perekrestokvprok.dim_rtg_agg
*/

WITH af_conversions AS (
    SELECT
        date,
        media_source AS mediasource,
        campaign_name,
        report_type AS campaign_type,
        platform,
        
    CASE
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-"],'')), r'promo.*regular') THEN 'promo regular'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-"],'')), r'promo.*global') THEN 'promo global'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-"],'')), r'promo.*feed') THEN 'promo feed'
    ELSE '-' END
 as promo_type,
        
    CASE
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-"], ' ')), 'minusfrequency2')
        THEN 'All buyers minus frequency 2'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-"], ' ')), 'aud.cart')
        THEN 'Add to cart not buy'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-"], ' ')), 'core_promo_5')
        THEN 'Ядро_Промо+5_fix'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-"], ' ')), 'reg_not_buy')
        THEN 'Registered but not buy'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-"], ' ')), 'purchase15_22')
        THEN 'Предотток'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-"], ' ')), 'purchase23_73')
        THEN 'Отток'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-"], ' ')), 'purchase74_130')
        OR REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-"], ' ')), 'purchase_aft130')
        THEN 'Глубокий отток'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-"], ' ')), 'notused_30d')
        THEN 'Не использовали приложение более 30 дней'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-"], ' ')), 'purch_less_2')
        THEN 'Покупают реже, чем 2 раза в месяц'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-"], ' ')), 'purch_from_2')
        THEN 'Покупают от 2 раз в месяц'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-"], ' ')), 'deep_outflow_rtg')
        THEN 'Deep_outflow_rtg'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-"], ' ')), 'cpo_outflow_rtg')
        THEN 'outflow'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-"], ' ')), 'cpo_preflow_rtg')
        THEN 'preflow'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-"], ' ')), 'cpo_installed_the_app_but_not_buy_')
        THEN 'Installed_the_app_but_not_buy'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-"], ' ')), 'first_open_.ot_buy_rtg')
        THEN 'First_open_not_buy_rtg'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-"], ' ')), 'installed_the_app_but_not_buy_rtg')
        THEN 'Installed_the_app_but_not_buy_rtg'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-"], ' ')), 'registered_but_not_buy')
        THEN 'Registered_but_not_buy'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-"], ' ')), 'firebase')
        THEN  "Предиктивная аудитория" 
        WHEN REGEXP_CONTAINS(LOWER(campaign_name), 'inapp')
        THEN 'Аудитории площадки (inapp)'
        ELSE 'Нет аудитории' END
 AS auditory,
        purchase,
        revenue,
        conversions AS re_engagement,
    FROM  `perekrestokvprok-bq`.`dbt_lazuta`.`stg_af_rtg_partners_by_date`
),

----------------------- facebook -------------------------

facebook_cost AS (
    SELECT
        date,
        campaign_name,
        
    CASE
        WHEN REGEXP_CONTAINS(LOWER(campaign_name), r'\[p:ios\]|_ios_|p02') THEN 'ios'
        WHEN REGEXP_CONTAINS(LOWER(campaign_name), r'\[p:and\]|_and_|android|p01|:and_') THEN 'android'
    ELSE 'no_platform' END
 as platform,
        
    CASE
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name],'')), r'promo.*regular') THEN 'promo regular'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name],'')), r'promo.*global') THEN 'promo global'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name],'')), r'promo.*feed') THEN 'promo feed'
    ELSE '-' END
 as promo_type,
        
    CASE
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'minusfrequency2')
        THEN 'All buyers minus frequency 2'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'aud.cart')
        THEN 'Add to cart not buy'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'core_promo_5')
        THEN 'Ядро_Промо+5_fix'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'reg_not_buy')
        THEN 'Registered but not buy'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'purchase15_22')
        THEN 'Предотток'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'purchase23_73')
        THEN 'Отток'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'purchase74_130')
        OR REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'purchase_aft130')
        THEN 'Глубокий отток'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'notused_30d')
        THEN 'Не использовали приложение более 30 дней'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'purch_less_2')
        THEN 'Покупают реже, чем 2 раза в месяц'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'purch_from_2')
        THEN 'Покупают от 2 раз в месяц'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'deep_outflow_rtg')
        THEN 'Deep_outflow_rtg'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'cpo_outflow_rtg')
        THEN 'outflow'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'cpo_preflow_rtg')
        THEN 'preflow'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'cpo_installed_the_app_but_not_buy_')
        THEN 'Installed_the_app_but_not_buy'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'first_open_.ot_buy_rtg')
        THEN 'First_open_not_buy_rtg'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'installed_the_app_but_not_buy_rtg')
        THEN 'Installed_the_app_but_not_buy_rtg'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'registered_but_not_buy')
        THEN 'Registered_but_not_buy'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'firebase')
        THEN  "Предиктивная аудитория" 
        WHEN REGEXP_CONTAINS(LOWER(campaign_name), 'inapp')
        THEN 'Аудитории площадки (inapp)'
        ELSE 'Нет аудитории' END
 AS auditory,
        campaign_type,
        SUM(impressions) AS impressions,
        SUM(clicks) AS clicks,
        SUM(spend) AS spend
    FROM `perekrestokvprok-bq`.`dbt_lazuta`.`stg_facebook_cab_sheets`
    --`perekrestokvprok-bq`.`dbt_lazuta`.`stg_facebook_cab_meta`
    WHERE campaign_type = 'retargeting'
    GROUP BY 1,2,3,4,5,6
),

facebook_convs AS (
    SELECT
        date,
        campaign_name,
        platform,
        promo_type,
        campaign_type,
        auditory,
        revenue,
        purchase,
        re_engagement
    FROM af_conversions
    WHERE REGEXP_CONTAINS(campaign_name, r'realweb_fb')
),

facebook AS (
    SELECT
        COALESCE(facebook_convs.date, facebook_cost.date) AS date,
        COALESCE(facebook_convs.campaign_name, facebook_cost.campaign_name) AS campaign_name,
        COALESCE(facebook_convs.platform, facebook_cost.platform) AS platform,
        COALESCE(facebook_convs.promo_type, facebook_cost.promo_type) AS promo_type,
        COALESCE(facebook_convs.auditory, facebook_cost.auditory) AS auditory,
        COALESCE(facebook_convs.campaign_type, facebook_cost.campaign_type) AS campaign_type,
        COALESCE(impressions,0) AS impressions,
        COALESCE(clicks,0) AS clicks,
        COALESCE(re_engagement,0) AS re_engagement,
        COALESCE(revenue,0) AS revenue,
        COALESCE(purchase,0) AS purchase,
        COALESCE(spend,0) AS spend,
        'Facebook' AS source,
    FROM facebook_convs
    FULL OUTER JOIN facebook_cost
    ON facebook_convs.date = facebook_cost.date 
    AND facebook_convs.campaign_name = facebook_cost.campaign_name
    AND facebook_convs.promo_type = facebook_cost.promo_type
    AND facebook_convs.auditory = facebook_cost.auditory
    WHERE 
        COALESCE(re_engagement,0) + 
        COALESCE(revenue,0) + 
        COALESCE(purchase,0) + 
        COALESCE(spend,0) > 0
    AND COALESCE(facebook_convs.campaign_name, facebook_cost.campaign_name) != 'None'
),

----------------------- yandex -------------------------

yandex_cost AS (
    SELECT
        date,
        campaign_name,
        
    CASE
        WHEN REGEXP_CONTAINS(LOWER(campaign_name), r'\[p:ios\]|_ios_|p02') THEN 'ios'
        WHEN REGEXP_CONTAINS(LOWER(campaign_name), r'\[p:and\]|_and_|android|p01|:and_') THEN 'android'
    ELSE 'no_platform' END
 as platform,
        
    CASE
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name],'')), r'promo.*regular') THEN 'promo regular'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name],'')), r'promo.*global') THEN 'promo global'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name],'')), r'promo.*feed') THEN 'promo feed'
    ELSE '-' END
 as promo_type,
        
    CASE
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'minusfrequency2')
        THEN 'All buyers minus frequency 2'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'aud.cart')
        THEN 'Add to cart not buy'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'core_promo_5')
        THEN 'Ядро_Промо+5_fix'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'reg_not_buy')
        THEN 'Registered but not buy'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'purchase15_22')
        THEN 'Предотток'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'purchase23_73')
        THEN 'Отток'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'purchase74_130')
        OR REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'purchase_aft130')
        THEN 'Глубокий отток'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'notused_30d')
        THEN 'Не использовали приложение более 30 дней'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'purch_less_2')
        THEN 'Покупают реже, чем 2 раза в месяц'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'purch_from_2')
        THEN 'Покупают от 2 раз в месяц'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'deep_outflow_rtg')
        THEN 'Deep_outflow_rtg'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'cpo_outflow_rtg')
        THEN 'outflow'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'cpo_preflow_rtg')
        THEN 'preflow'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'cpo_installed_the_app_but_not_buy_')
        THEN 'Installed_the_app_but_not_buy'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'first_open_.ot_buy_rtg')
        THEN 'First_open_not_buy_rtg'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'installed_the_app_but_not_buy_rtg')
        THEN 'Installed_the_app_but_not_buy_rtg'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'registered_but_not_buy')
        THEN 'Registered_but_not_buy'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'firebase')
        THEN  "Предиктивная аудитория" 
        WHEN REGEXP_CONTAINS(LOWER(campaign_name), 'inapp')
        THEN 'Аудитории площадки (inapp)'
        ELSE 'Нет аудитории' END
 AS auditory,
        campaign_type,
        SUM(impressions) AS impressions,
        SUM(clicks) AS clicks,
        SUM(spend) AS spend
    FROM `perekrestokvprok-bq`.`dbt_lazuta`.`int_yandex_cab_meta`
    WHERE campaign_type = 'retargeting'
    AND REGEXP_CONTAINS(campaign_name, r'realweb')
    GROUP BY 1,2,3,4,5,6
),

yandex_convs AS (
    SELECT
        date,
        campaign_name,
        platform,
        promo_type,
        campaign_type,
        auditory,
        revenue,
        purchase,
        re_engagement
    FROM af_conversions
    WHERE REGEXP_CONTAINS(campaign_name, r'realweb_ya')
),

yandex AS (
    SELECT
        COALESCE(yandex_convs.date, yandex_cost.date) AS date,
        COALESCE(yandex_convs.campaign_name, yandex_cost.campaign_name) AS campaign_name,
        COALESCE(yandex_convs.platform, yandex_cost.platform) AS platform,
        COALESCE(yandex_convs.promo_type, yandex_cost.promo_type) AS promo_type,
        COALESCE(yandex_convs.auditory, yandex_cost.auditory) AS auditory,
        COALESCE(yandex_convs.campaign_type, yandex_cost.campaign_type) AS campaign_type,
        COALESCE(impressions,0) AS impressions,
        COALESCE(clicks,0) AS clicks,
        COALESCE(re_engagement,0) AS re_engagement,
        COALESCE(revenue,0) AS revenue,
        COALESCE(purchase,0) AS purchase,
        COALESCE(spend,0) AS spend,
        'Яндекс.Директ' AS source,
    FROM yandex_convs
    FULL OUTER JOIN yandex_cost
    ON yandex_convs.date = yandex_cost.date 
    AND yandex_convs.campaign_name = yandex_cost.campaign_name
    AND yandex_convs.promo_type = yandex_cost.promo_type
    AND yandex_convs.auditory = yandex_cost.auditory
    WHERE 
        COALESCE(re_engagement,0) + 
        COALESCE(revenue,0) + 
        COALESCE(purchase,0) + 
        COALESCE(spend,0) > 0
    AND COALESCE(yandex_convs.campaign_name, yandex_cost.campaign_name) != 'None'
),

----------------------- vk -------------------------

vk_cost AS (
    SELECT
        date,
        campaign_name,
        
    CASE
        WHEN REGEXP_CONTAINS(LOWER(campaign_name), r'\[p:ios\]|_ios_|p02') THEN 'ios'
        WHEN REGEXP_CONTAINS(LOWER(campaign_name), r'\[p:and\]|_and_|android|p01|:and_') THEN 'android'
    ELSE 'no_platform' END
 as platform,
        
    CASE
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name],'')), r'promo.*regular') THEN 'promo regular'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name],'')), r'promo.*global') THEN 'promo global'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name],'')), r'promo.*feed') THEN 'promo feed'
    ELSE '-' END
 as promo_type,
        
    CASE
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'minusfrequency2')
        THEN 'All buyers minus frequency 2'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'aud.cart')
        THEN 'Add to cart not buy'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'core_promo_5')
        THEN 'Ядро_Промо+5_fix'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'reg_not_buy')
        THEN 'Registered but not buy'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'purchase15_22')
        THEN 'Предотток'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'purchase23_73')
        THEN 'Отток'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'purchase74_130')
        OR REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'purchase_aft130')
        THEN 'Глубокий отток'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'notused_30d')
        THEN 'Не использовали приложение более 30 дней'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'purch_less_2')
        THEN 'Покупают реже, чем 2 раза в месяц'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'purch_from_2')
        THEN 'Покупают от 2 раз в месяц'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'deep_outflow_rtg')
        THEN 'Deep_outflow_rtg'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'cpo_outflow_rtg')
        THEN 'outflow'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'cpo_preflow_rtg')
        THEN 'preflow'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'cpo_installed_the_app_but_not_buy_')
        THEN 'Installed_the_app_but_not_buy'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'first_open_.ot_buy_rtg')
        THEN 'First_open_not_buy_rtg'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'installed_the_app_but_not_buy_rtg')
        THEN 'Installed_the_app_but_not_buy_rtg'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'registered_but_not_buy')
        THEN 'Registered_but_not_buy'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'firebase')
        THEN  "Предиктивная аудитория" 
        WHEN REGEXP_CONTAINS(LOWER(campaign_name), 'inapp')
        THEN 'Аудитории площадки (inapp)'
        ELSE 'Нет аудитории' END
 AS auditory,
        campaign_type,
        SUM(impressions) AS impressions,
        SUM(clicks) AS clicks,
        SUM(spend) AS spend
    FROM `perekrestokvprok-bq`.`dbt_lazuta`.`int_vk_cab_meta`
    WHERE campaign_type = 'retargeting'
    AND REGEXP_CONTAINS(campaign_name, r'realweb')
    GROUP BY 1,2,3,4,5,6
),

vk_convs AS (
    SELECT
        date,
        campaign_name,
        platform,
        promo_type,
        campaign_type,
        auditory,
        revenue,
        purchase,
        re_engagement
    FROM af_conversions
    WHERE REGEXP_CONTAINS(campaign_name, r'realweb_vk')
),

vk AS (
    SELECT
        COALESCE(vk_convs.date, vk_cost.date) AS date,
        COALESCE(vk_convs.campaign_name, vk_cost.campaign_name) AS campaign_name,
        COALESCE(vk_convs.platform, vk_cost.platform) AS platform,
        COALESCE(vk_convs.promo_type, vk_cost.promo_type) AS promo_type,
        COALESCE(vk_convs.auditory, vk_cost.auditory) AS auditory,
        COALESCE(vk_convs.campaign_type, vk_cost.campaign_type) AS campaign_type,
        COALESCE(impressions,0) AS impressions,
        COALESCE(clicks,0) AS clicks,
        COALESCE(re_engagement,0) AS re_engagement,
        COALESCE(revenue,0) AS revenue,
        COALESCE(purchase,0) AS purchase,
        COALESCE(spend,0) AS spend,
        'ВК' AS source,
    FROM vk_convs
    FULL OUTER JOIN vk_cost
    ON vk_convs.date = vk_cost.date 
    AND vk_convs.campaign_name = vk_cost.campaign_name
    AND vk_convs.promo_type = vk_cost.promo_type
    AND vk_convs.auditory = vk_cost.auditory
    WHERE 
        COALESCE(re_engagement,0) + 
        COALESCE(revenue,0) + 
        COALESCE(purchase,0) + 
        COALESCE(spend,0) > 0
    AND COALESCE(vk_convs.campaign_name, vk_cost.campaign_name) != 'None'
),

----------------------- mytarget -------------------------

mt_cost AS (
    SELECT
        date,
        campaign_name,
        
    CASE
        WHEN REGEXP_CONTAINS(LOWER(campaign_name), r'\[p:ios\]|_ios_|p02') THEN 'ios'
        WHEN REGEXP_CONTAINS(LOWER(campaign_name), r'\[p:and\]|_and_|android|p01|:and_') THEN 'android'
    ELSE 'no_platform' END
 as platform,
        
    CASE
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name],'')), r'promo.*regular') THEN 'promo regular'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name],'')), r'promo.*global') THEN 'promo global'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name],'')), r'promo.*feed') THEN 'promo feed'
    ELSE '-' END
 as promo_type,
        
    CASE
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'minusfrequency2')
        THEN 'All buyers minus frequency 2'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'aud.cart')
        THEN 'Add to cart not buy'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'core_promo_5')
        THEN 'Ядро_Промо+5_fix'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'reg_not_buy')
        THEN 'Registered but not buy'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'purchase15_22')
        THEN 'Предотток'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'purchase23_73')
        THEN 'Отток'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'purchase74_130')
        OR REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'purchase_aft130')
        THEN 'Глубокий отток'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'notused_30d')
        THEN 'Не использовали приложение более 30 дней'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'purch_less_2')
        THEN 'Покупают реже, чем 2 раза в месяц'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'purch_from_2')
        THEN 'Покупают от 2 раз в месяц'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'deep_outflow_rtg')
        THEN 'Deep_outflow_rtg'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'cpo_outflow_rtg')
        THEN 'outflow'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'cpo_preflow_rtg')
        THEN 'preflow'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'cpo_installed_the_app_but_not_buy_')
        THEN 'Installed_the_app_but_not_buy'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'first_open_.ot_buy_rtg')
        THEN 'First_open_not_buy_rtg'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'installed_the_app_but_not_buy_rtg')
        THEN 'Installed_the_app_but_not_buy_rtg'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'registered_but_not_buy')
        THEN 'Registered_but_not_buy'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'firebase')
        THEN  "Предиктивная аудитория" 
        WHEN REGEXP_CONTAINS(LOWER(campaign_name), 'inapp')
        THEN 'Аудитории площадки (inapp)'
        ELSE 'Нет аудитории' END
 AS auditory,
        campaign_type,
        SUM(impressions) AS impressions,
        SUM(clicks) AS clicks,
        SUM(spend) AS spend
    FROM `perekrestokvprok-bq`.`dbt_lazuta`.`int_mytarget_cab_meta`
    WHERE campaign_type = 'retargeting'
    AND REGEXP_CONTAINS(campaign_name, r'realweb')
    GROUP BY 1,2,3,4,5,6
),

mt_convs AS (
    SELECT
        date,
        campaign_name,
        platform,
        promo_type,
        campaign_type,
        auditory,
        revenue,
        purchase,
        re_engagement
    FROM af_conversions
    WHERE REGEXP_CONTAINS(campaign_name, r'realweb_mt')
),

mt AS (
    SELECT
        COALESCE(mt_convs.date, mt_cost.date) AS date,
        COALESCE(mt_convs.campaign_name, mt_cost.campaign_name) AS campaign_name,
        COALESCE(mt_convs.platform, mt_cost.platform) AS platform,
        COALESCE(mt_convs.promo_type, mt_cost.promo_type) AS promo_type,
        COALESCE(mt_convs.auditory, mt_cost.auditory) AS auditory,
        COALESCE(mt_convs.campaign_type, mt_cost.campaign_type) AS campaign_type,
        COALESCE(impressions,0) AS impressions,
        COALESCE(clicks,0) AS clicks,
        COALESCE(re_engagement,0) AS re_engagement,
        COALESCE(revenue,0) AS revenue,
        COALESCE(purchase,0) AS purchase,
        COALESCE(spend,0) AS spend,
        'MyTarget' AS source,
    FROM mt_convs
    FULL OUTER JOIN mt_cost
    ON mt_convs.date = mt_cost.date 
    AND mt_convs.campaign_name = mt_cost.campaign_name
    AND mt_convs.promo_type = mt_cost.promo_type
    AND mt_convs.auditory = mt_cost.auditory
    WHERE 
        COALESCE(re_engagement,0) + 
        COALESCE(revenue,0) + 
        COALESCE(purchase,0) + 
        COALESCE(spend,0) > 0
    AND COALESCE(mt_convs.campaign_name, mt_cost.campaign_name) != 'None'
),

----------------------- tiktok -------------------------

tiktok_cost AS (
    SELECT
        date,
        campaign_name,
        
    CASE
        WHEN REGEXP_CONTAINS(LOWER(campaign_name), r'\[p:ios\]|_ios_|p02') THEN 'ios'
        WHEN REGEXP_CONTAINS(LOWER(campaign_name), r'\[p:and\]|_and_|android|p01|:and_') THEN 'android'
    ELSE 'no_platform' END
 as platform,
        
    CASE
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name],'')), r'promo.*regular') THEN 'promo regular'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name],'')), r'promo.*global') THEN 'promo global'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name],'')), r'promo.*feed') THEN 'promo feed'
    ELSE '-' END
 as promo_type,
        
    CASE
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'minusfrequency2')
        THEN 'All buyers minus frequency 2'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'aud.cart')
        THEN 'Add to cart not buy'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'core_promo_5')
        THEN 'Ядро_Промо+5_fix'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'reg_not_buy')
        THEN 'Registered but not buy'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'purchase15_22')
        THEN 'Предотток'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'purchase23_73')
        THEN 'Отток'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'purchase74_130')
        OR REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'purchase_aft130')
        THEN 'Глубокий отток'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'notused_30d')
        THEN 'Не использовали приложение более 30 дней'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'purch_less_2')
        THEN 'Покупают реже, чем 2 раза в месяц'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'purch_from_2')
        THEN 'Покупают от 2 раз в месяц'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'deep_outflow_rtg')
        THEN 'Deep_outflow_rtg'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'cpo_outflow_rtg')
        THEN 'outflow'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'cpo_preflow_rtg')
        THEN 'preflow'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'cpo_installed_the_app_but_not_buy_')
        THEN 'Installed_the_app_but_not_buy'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'first_open_.ot_buy_rtg')
        THEN 'First_open_not_buy_rtg'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'installed_the_app_but_not_buy_rtg')
        THEN 'Installed_the_app_but_not_buy_rtg'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'registered_but_not_buy')
        THEN 'Registered_but_not_buy'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'firebase')
        THEN  "Предиктивная аудитория" 
        WHEN REGEXP_CONTAINS(LOWER(campaign_name), 'inapp')
        THEN 'Аудитории площадки (inapp)'
        ELSE 'Нет аудитории' END
 AS auditory,
        campaign_type,
        SUM(impressions) AS impressions,
        SUM(clicks) AS clicks,
        SUM(spend) AS spend
    FROM `perekrestokvprok-bq`.`dbt_lazuta`.`stg_tiktok_cab_meta`
    WHERE REGEXP_CONTAINS(campaign_name, r'realweb')
    AND campaign_type = 'retargeting'
    GROUP BY 1,2,3,4,5,6
),

tiktok_convs AS (
    SELECT
        date,
        campaign_name,
        platform,
        promo_type,
        campaign_type,
        auditory,
        revenue,
        purchase,
        re_engagement
    FROM af_conversions
    WHERE REGEXP_CONTAINS(campaign_name, r'realweb_tiktok')
),

tiktok AS (
    SELECT
        COALESCE(tiktok_convs.date, tiktok_cost.date) AS date,
        COALESCE(tiktok_convs.campaign_name, tiktok_cost.campaign_name) AS campaign_name,
        COALESCE(tiktok_convs.platform, tiktok_cost.platform) AS platform,
        COALESCE(tiktok_convs.promo_type, tiktok_cost.promo_type) AS promo_type,
        COALESCE(tiktok_convs.auditory, tiktok_cost.auditory) AS auditory,
        COALESCE(tiktok_convs.campaign_type, tiktok_cost.campaign_type) AS campaign_type,
        COALESCE(impressions,0) AS impressions,
        COALESCE(clicks,0) AS clicks,
        COALESCE(re_engagement,0) AS re_engagement,
        COALESCE(revenue,0) AS revenue,
        COALESCE(purchase,0) AS purchase,
        COALESCE(spend,0) AS spend,
        'TikTok' AS source,
    FROM tiktok_convs
    FULL OUTER JOIN tiktok_cost
    ON tiktok_convs.date = tiktok_cost.date 
    AND tiktok_convs.campaign_name = tiktok_cost.campaign_name
    AND tiktok_convs.promo_type = tiktok_cost.promo_type
    AND tiktok_convs.auditory = tiktok_cost.auditory
    WHERE 
        COALESCE(re_engagement,0) + 
        COALESCE(revenue,0) + 
        COALESCE(purchase,0) + 
        COALESCE(spend,0) > 0
    AND COALESCE(tiktok_convs.campaign_name, tiktok_cost.campaign_name) != 'None'
),

----------------------- apple search ads -------------------------

asa_cost AS (
    SELECT
        date,
        campaign_name,
        'ios' as platform,
        
    CASE
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name],'')), r'promo.*regular') THEN 'promo regular'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name],'')), r'promo.*global') THEN 'promo global'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name],'')), r'promo.*feed') THEN 'promo feed'
    ELSE '-' END
 as promo_type,
        
    CASE
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'minusfrequency2')
        THEN 'All buyers minus frequency 2'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'aud.cart')
        THEN 'Add to cart not buy'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'core_promo_5')
        THEN 'Ядро_Промо+5_fix'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'reg_not_buy')
        THEN 'Registered but not buy'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'purchase15_22')
        THEN 'Предотток'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'purchase23_73')
        THEN 'Отток'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'purchase74_130')
        OR REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'purchase_aft130')
        THEN 'Глубокий отток'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'notused_30d')
        THEN 'Не использовали приложение более 30 дней'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'purch_less_2')
        THEN 'Покупают реже, чем 2 раза в месяц'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'purch_from_2')
        THEN 'Покупают от 2 раз в месяц'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'deep_outflow_rtg')
        THEN 'Deep_outflow_rtg'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'cpo_outflow_rtg')
        THEN 'outflow'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'cpo_preflow_rtg')
        THEN 'preflow'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'cpo_installed_the_app_but_not_buy_')
        THEN 'Installed_the_app_but_not_buy'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'first_open_.ot_buy_rtg')
        THEN 'First_open_not_buy_rtg'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'installed_the_app_but_not_buy_rtg')
        THEN 'Installed_the_app_but_not_buy_rtg'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'registered_but_not_buy')
        THEN 'Registered_but_not_buy'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'firebase')
        THEN  "Предиктивная аудитория" 
        WHEN REGEXP_CONTAINS(LOWER(campaign_name), 'inapp')
        THEN 'Аудитории площадки (inapp)'
        ELSE 'Нет аудитории' END
 AS auditory,
        campaign_type,
        SUM(meta.impressions) AS impressions,
        SUM(sheet.clicks) AS clicks,
        SUM(sheet.spend) AS spend
    FROM `perekrestokvprok-bq`.`dbt_lazuta`.`stg_asa_cab_sheets` sheet
    LEFT JOIN `perekrestokvprok-bq`.`dbt_lazuta`.`int_asa_cab_meta` meta
    USING(date, campaign_name, campaign_type, adset_name)
    WHERE campaign_type = 'retargeting'
    GROUP BY 1,2,3,4,5,6
),

asa_convs AS (
    SELECT
        date,
        campaign_name,
        platform,
        promo_type,
        campaign_type,
        auditory,
        revenue,
        purchase,
        re_engagement
    FROM af_conversions
    WHERE 
    --REGEXP_CONTAINS(campaign_name, r'\(r\)') AND
    (
        REGEXP_CONTAINS(campaign_name, r'\(exact\)|зоо') OR
        mediasource = 'Apple Search Ads'
    )
),

asa AS (
    SELECT
        COALESCE(asa_convs.date, asa_cost.date) AS date,
        COALESCE(asa_convs.campaign_name, asa_cost.campaign_name) AS campaign_name,
        COALESCE(asa_convs.platform, asa_cost.platform) AS platform,
        COALESCE(asa_convs.promo_type, asa_cost.promo_type) AS promo_type,
        COALESCE(asa_convs.auditory, asa_cost.auditory) AS auditory,
        COALESCE(asa_convs.campaign_type, asa_cost.campaign_type) AS campaign_type,
        COALESCE(impressions,0) AS impressions,
        COALESCE(clicks,0) AS clicks,
        COALESCE(re_engagement,0) AS re_engagement,
        COALESCE(revenue,0) AS revenue,
        COALESCE(purchase,0) AS purchase,
        COALESCE(spend,0) AS spend,
        'Apple Search Ads' AS source,
    FROM asa_convs
    FULL OUTER JOIN asa_cost
    ON asa_convs.date = asa_cost.date 
    AND asa_convs.campaign_name = asa_cost.campaign_name
    AND asa_convs.promo_type = asa_cost.promo_type
    AND asa_convs.auditory = asa_cost.auditory
    WHERE 
        COALESCE(re_engagement,0) + 
        COALESCE(revenue,0) + 
        COALESCE(purchase,0) + 
        COALESCE(spend,0) > 0
    AND COALESCE(asa_convs.campaign_name, asa_cost.campaign_name) != 'None'
),

----------------------- google -------------------------

google_cost AS (
    SELECT
        date,
        campaign_name,
        
    CASE
        WHEN REGEXP_CONTAINS(LOWER(campaign_name), r'\[p:ios\]|_ios_|p02') THEN 'ios'
        WHEN REGEXP_CONTAINS(LOWER(campaign_name), r'\[p:and\]|_and_|android|p01|:and_') THEN 'android'
    ELSE 'no_platform' END
 as platform,
        
    CASE
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name],'')), r'promo.*regular') THEN 'promo regular'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name],'')), r'promo.*global') THEN 'promo global'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name],'')), r'promo.*feed') THEN 'promo feed'
    ELSE '-' END
 as promo_type,
        
    CASE
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'minusfrequency2')
        THEN 'All buyers minus frequency 2'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'aud.cart')
        THEN 'Add to cart not buy'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'core_promo_5')
        THEN 'Ядро_Промо+5_fix'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'reg_not_buy')
        THEN 'Registered but not buy'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'purchase15_22')
        THEN 'Предотток'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'purchase23_73')
        THEN 'Отток'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'purchase74_130')
        OR REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'purchase_aft130')
        THEN 'Глубокий отток'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'notused_30d')
        THEN 'Не использовали приложение более 30 дней'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'purch_less_2')
        THEN 'Покупают реже, чем 2 раза в месяц'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'purch_from_2')
        THEN 'Покупают от 2 раз в месяц'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'deep_outflow_rtg')
        THEN 'Deep_outflow_rtg'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'cpo_outflow_rtg')
        THEN 'outflow'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'cpo_preflow_rtg')
        THEN 'preflow'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'cpo_installed_the_app_but_not_buy_')
        THEN 'Installed_the_app_but_not_buy'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'first_open_.ot_buy_rtg')
        THEN 'First_open_not_buy_rtg'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'installed_the_app_but_not_buy_rtg')
        THEN 'Installed_the_app_but_not_buy_rtg'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'registered_but_not_buy')
        THEN 'Registered_but_not_buy'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'firebase')
        THEN  "Предиктивная аудитория" 
        WHEN REGEXP_CONTAINS(LOWER(campaign_name), 'inapp')
        THEN 'Аудитории площадки (inapp)'
        ELSE 'Нет аудитории' END
 AS auditory,
        campaign_type,
        SUM(impressions) AS impressions,
        SUM(clicks) AS clicks,
        SUM(spend) AS spend
    FROM `perekrestokvprok-bq`.`dbt_lazuta`.`stg_google_cab_sheets`
    WHERE (campaign_type = 'retargeting'
    OR campaign_name IN (
            'realweb_uac_2022 [p:and] [cpi] [mskspb] [new] [general] [darkstore] [purchase] [firebase]',
            'realweb_uac_2022 [p:and] [cpi] [reg1] [new] [general] [darkstore] [purchase] [firebase]'
            ))
    AND REGEXP_CONTAINS(campaign_name, r'realweb')
    GROUP BY 1,2,3,4,5,6
),

google_convs AS (
    SELECT
        date,
        campaign_name,
        platform,
        promo_type,
        campaign_type,
        auditory,
        revenue,
        purchase,
        re_engagement
    FROM af_conversions
    WHERE REGEXP_CONTAINS(campaign_name, r'realweb_uac')
),

google AS (
    SELECT
        COALESCE(google_convs.date, google_cost.date) AS date,
        COALESCE(google_convs.campaign_name, google_cost.campaign_name) AS campaign_name,
        COALESCE(google_convs.platform, google_cost.platform) AS platform,
        COALESCE(google_convs.promo_type, google_cost.promo_type) AS promo_type,
        COALESCE(google_convs.auditory, google_cost.auditory) AS auditory,
        COALESCE(google_convs.campaign_type, google_cost.campaign_type) AS campaign_type,
        COALESCE(impressions,0) AS impressions,
        COALESCE(clicks,0) AS clicks,
        COALESCE(re_engagement,0) AS re_engagement,
        COALESCE(revenue,0) AS revenue,
        COALESCE(purchase,0) AS purchase,
        COALESCE(spend,0) AS spend,
        'Google Ads' AS source,
    FROM google_convs
    FULL OUTER JOIN google_cost
    ON google_convs.date = google_cost.date 
    AND google_convs.campaign_name = google_cost.campaign_name
    AND google_convs.promo_type = google_cost.promo_type
    AND google_convs.auditory = google_cost.auditory
    WHERE 
        COALESCE(re_engagement,0) + 
        COALESCE(revenue,0) + 
        COALESCE(purchase,0) + 
        COALESCE(spend,0) > 0
    AND COALESCE(google_convs.campaign_name, google_cost.campaign_name) != 'None'
),

----------------------- Twitter -------------------------

tw_cost AS (
    SELECT
        date,
        campaign_name,
        
    CASE
        WHEN REGEXP_CONTAINS(LOWER(campaign_name), r'\[p:ios\]|_ios_|p02') THEN 'ios'
        WHEN REGEXP_CONTAINS(LOWER(campaign_name), r'\[p:and\]|_and_|android|p01|:and_') THEN 'android'
    ELSE 'no_platform' END
 as platform,
        
    CASE
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-"],'')), r'promo.*regular') THEN 'promo regular'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-"],'')), r'promo.*global') THEN 'promo global'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-"],'')), r'promo.*feed') THEN 'promo feed'
    ELSE '-' END
 as promo_type,
        
    CASE
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-"], ' ')), 'minusfrequency2')
        THEN 'All buyers minus frequency 2'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-"], ' ')), 'aud.cart')
        THEN 'Add to cart not buy'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-"], ' ')), 'core_promo_5')
        THEN 'Ядро_Промо+5_fix'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-"], ' ')), 'reg_not_buy')
        THEN 'Registered but not buy'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-"], ' ')), 'purchase15_22')
        THEN 'Предотток'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-"], ' ')), 'purchase23_73')
        THEN 'Отток'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-"], ' ')), 'purchase74_130')
        OR REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-"], ' ')), 'purchase_aft130')
        THEN 'Глубокий отток'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-"], ' ')), 'notused_30d')
        THEN 'Не использовали приложение более 30 дней'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-"], ' ')), 'purch_less_2')
        THEN 'Покупают реже, чем 2 раза в месяц'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-"], ' ')), 'purch_from_2')
        THEN 'Покупают от 2 раз в месяц'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-"], ' ')), 'deep_outflow_rtg')
        THEN 'Deep_outflow_rtg'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-"], ' ')), 'cpo_outflow_rtg')
        THEN 'outflow'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-"], ' ')), 'cpo_preflow_rtg')
        THEN 'preflow'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-"], ' ')), 'cpo_installed_the_app_but_not_buy_')
        THEN 'Installed_the_app_but_not_buy'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-"], ' ')), 'first_open_.ot_buy_rtg')
        THEN 'First_open_not_buy_rtg'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-"], ' ')), 'installed_the_app_but_not_buy_rtg')
        THEN 'Installed_the_app_but_not_buy_rtg'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-"], ' ')), 'registered_but_not_buy')
        THEN 'Registered_but_not_buy'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-"], ' ')), 'firebase')
        THEN  "Предиктивная аудитория" 
        WHEN REGEXP_CONTAINS(LOWER(campaign_name), 'inapp')
        THEN 'Аудитории площадки (inapp)'
        ELSE 'Нет аудитории' END
 AS auditory,
        campaign_type,
        SUM(impressions) AS impressions,
        0 AS clicks,
        SUM(spend) AS spend
    FROM `perekrestokvprok-bq`.`dbt_lazuta`.`stg_twitter_cab_sheets`
    WHERE campaign_type = 'retargeting'
    AND REGEXP_CONTAINS(campaign_name, r'realweb_tw')
    GROUP BY 1,2,3,4,5,6
),

tw_convs AS (
    SELECT
        date,
        campaign_name,
        platform,
        promo_type,
        campaign_type,
        auditory,
        revenue,
        purchase,
        re_engagement
    FROM af_conversions
    WHERE REGEXP_CONTAINS(campaign_name, r'realweb_tw')
),

tw AS (
    SELECT
        COALESCE(tw_convs.date, tw_cost.date) AS date,
        COALESCE(tw_convs.campaign_name, tw_cost.campaign_name) AS campaign_name,
        COALESCE(tw_convs.platform, tw_cost.platform) AS platform,
        COALESCE(tw_convs.promo_type, tw_cost.promo_type) AS promo_type,
        COALESCE(tw_convs.auditory, tw_cost.auditory) AS auditory,
        COALESCE(tw_convs.campaign_type, tw_cost.campaign_type) AS campaign_type,
        COALESCE(impressions,0) AS impressions,
        COALESCE(clicks,0) AS clicks,
        COALESCE(re_engagement,0) AS re_engagement,
        COALESCE(revenue,0) AS revenue,
        COALESCE(purchase,0) AS purchase,
        COALESCE(spend,0) AS spend,
        'Twitter' AS source,
    FROM tw_convs
    FULL OUTER JOIN tw_cost
    ON tw_convs.date = tw_cost.date 
    AND tw_convs.campaign_name = tw_cost.campaign_name
    AND tw_convs.promo_type = tw_cost.promo_type
    AND tw_convs.auditory = tw_cost.auditory
    WHERE 
        COALESCE(re_engagement,0) + 
        COALESCE(revenue,0) + 
        COALESCE(purchase,0) + 
        COALESCE(spend,0) > 0
    AND COALESCE(tw_convs.campaign_name, tw_cost.campaign_name) != 'None'
),

----------------------inapp----------------------------

rate AS (
    SELECT
        start_date,
        end_date,
        partner,
        platform,
        rate_for_us
FROM `perekrestokvprok-bq`.`dbt_lazuta`.`stg_rate_info`
WHERE type = 'RTG'
),

limits_table AS (
    SELECT
        start_date,
        end_date,
        partner,
        limits
    FROM `perekrestokvprok-bq`.`dbt_lazuta`.`stg_partner_limits`
    WHERE type = 'RTG'
),

inapp_convs_without_cumulation AS (
    SELECT 
        date,
        campaign_name,
        platform,
        
    CASE 
        WHEN REGEXP_CONTAINS(campaign_name, r'_ms_') THEN 'Mobisharks'
        WHEN REGEXP_CONTAINS(campaign_name, r'_tl_') THEN '2leads'
        WHEN REGEXP_CONTAINS(campaign_name, r'_mx_') THEN 'MobX'
        WHEN REGEXP_CONTAINS(campaign_name, r'_sw_') THEN 'SW'
        WHEN REGEXP_CONTAINS(campaign_name, r'_tm_') THEN 'Think Mobile'
        WHEN REGEXP_CONTAINS(campaign_name, r'_abc[_\s]|_sf_') THEN 'Mediasurfer'
    ELSE '-' END
 AS partner,
        promo_type,
        campaign_type,
        auditory,
        revenue,
        purchase,
        re_engagement
    FROM af_conversions
    WHERE REGEXP_CONTAINS(campaign_name, r'realweb_inapp')
),

inapp_convs_with_cumulation AS (
    SELECT
        date,
        campaign_name,
        partner,
        platform,
        promo_type,
        campaign_type,
        auditory,
        revenue,
        purchase,
        re_engagement,
        SUM(purchase) 
            OVER(PARTITION BY DATE_TRUNC(date, MONTH) ORDER BY date, revenue)
            AS cum_event_count,
        SUM(purchase) 
            OVER(PARTITION BY DATE_TRUNC(date, MONTH), partner ORDER BY date, revenue)
            AS cum_event_count_by_prt
    FROM inapp_convs_without_cumulation
),

inapp_convs AS (
    SELECT
        date,
        campaign_name,
        inapp_convs_with_cumulation.partner,
        platform,
        promo_type,
        auditory,
        campaign_type,
        re_engagement,
        cum_event_count,
        cum_event_count_by_prt,
        limits,
        IF(cum_event_count_by_prt <= COALESCE(limits, 1000000), purchase, 0) AS purchase,
        IF(cum_event_count_by_prt <= COALESCE(limits, 1000000), revenue, 0) AS revenue,
    FROM inapp_convs_with_cumulation
    LEFT JOIN limits_table
    ON inapp_convs_with_cumulation.partner = limits_table.partner 
    AND inapp_convs_with_cumulation.date BETWEEN limits_table.start_date AND limits_table.end_date
),

inapp AS (
    SELECT
        date,
        campaign_name,
        inapp_convs.platform AS platform,
        promo_type,
        auditory,
        campaign_type,
        0 AS impressions,
        0 AS clicks,
        SUM(COALESCE(re_engagement,0)) AS re_engagement,
        SUM(COALESCE(revenue,0)) AS revenue,
        SUM(COALESCE(purchase,0)) AS purchase,
        SUM(
            CASE
                WHEN date BETWEEN '2021-10-01' AND '2021-10-31'
                    AND cum_event_count >= 3000 THEN purchase * 140
                WHEN date BETWEEN '2021-10-01' AND '2021-10-31'
                    AND cum_event_count < 3000 THEN purchase * rate_for_us
                WHEN date NOT BETWEEN '2021-10-01' AND '2021-10-31'
                    AND cum_event_count_by_prt <= COALESCE(limits, 1000000)
                    THEN purchase * rate_for_us
                ELSE 0 END
            ) AS spend,
        'inapp' AS source,
    FROM inapp_convs
    LEFT JOIN rate
    ON inapp_convs.partner = rate.partner 
    AND inapp_convs.date BETWEEN rate.start_date AND rate.end_date
    AND inapp_convs.platform = rate.platform 
    WHERE 
        COALESCE(re_engagement,0) + 
        COALESCE(revenue,0) + 
        COALESCE(purchase,0) > 0
    AND campaign_name != 'None'
    GROUP BY 1,2,3,4,5,6,7,8
),

----------------------final----------------------------

unions AS (
    SELECT * FROM yandex
    UNION ALL 
    SELECT * FROM vk
    UNION ALL 
    SELECT * FROM mt
    UNION ALL  
    SELECT * FROM tiktok
    UNION ALL
    SELECT * FROM asa
    WHERE date <= '2022-08-31'
    UNION ALL
    SELECT * FROM facebook
    UNION ALL
    SELECT * FROM google
    UNION ALL
    SELECT * FROM tw
    UNION ALL
    SELECT * FROM inapp
),

final AS (
    SELECT 
        date,
        campaign_name,
        platform,
        promo_type,
        auditory,
        impressions,
        clicks,
        re_engagement,
        revenue,
        purchase,
        spend,
        source,
        
    CASE
          WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-"], ' ')), r'msk+spb|mskspb|msk_spb') THEN 'МСК, СПб'
          WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-"], ' ')), r'spb') THEN 'СПб'
          WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-"], ' ')), r'msk') THEN 'МСК'
          WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-"], ' ')), r'_nn_|g:nn|\[nn\]') THEN 'НН'
          WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-"], ' ')), r'reg1') THEN 'Регионы с доставкой'
          WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-"], ' ')), r'reg2|rostov|kzn|g_all')THEN 'Регионы без доставки'
        ELSE 'Россия' END
 AS geo
    FROM unions
)

SELECT 
    date,
    campaign_name,
    platform,
    promo_type,
    auditory,
    impressions,
    clicks,
    re_engagement,
    revenue,
    purchase,
    spend,
    source,
    geo
FROM final;

