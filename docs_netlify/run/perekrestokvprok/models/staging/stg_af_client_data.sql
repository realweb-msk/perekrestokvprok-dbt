

  create or replace view `perekrestokvprok-bq`.`dbt_lazuta`.`stg_af_client_data`
  OPTIONS()
  as WITH no_campaign AS (
SELECT 
    date,
    is_retargeting,
    af_c_id AS af_cid,
    af_adset AS adset_name,
    mediasource,
    event_value,
    platform,
    eventname AS event_name,
    unig_event AS uniq_event_count,
    EventRevenue AS event_revenue,
    cnt_event AS event_count,
    
    TRIM(
        REGEXP_REPLACE(
            LOWER(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(campaign,  r'(_install_week.*)', '_install_weekend'), 
                    r'(_install_promo_gl.*)', '_install_promo_global'), 
                r'(_install_promo_re.*)', '_install_promo_regular')
            ), r'\+|-', '_')
        )
 AS campaign_name
FROM  `perekrestokvprok-bq`.`agg_data`.`AF_client_data`
WHERE campaign != 'campaign_name' --убираем все campaign_name
),
campaign_name as (
  SELECT
    date,
    is_retargeting,
    af_c_id AS af_cid,
    af_adset AS adset_name,
    mediasource,
    event_value,
    platform,
    eventname AS event_name,
    unig_event AS uniq_event_count,
    EventRevenue AS event_revenue,
    cnt_event AS event_count,
    
    TRIM(
        REGEXP_REPLACE(
            LOWER(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(campaign,  r'(_install_week.*)', '_install_weekend'), 
                    r'(_install_promo_gl.*)', '_install_promo_global'), 
                r'(_install_promo_re.*)', '_install_promo_regular')
            ), r'\+|-', '_')
        )
 AS campaign_name
  FROM (
    SELECT * FROM `perekrestokvprok-bq`.`agg_data`.`AF_client_data`
    WHERE campaign = 'campaign_name'
  ) av
  WHERE date <'2022-05-01' OR date >'2022-05-15' --возвращаем campaign_name за определенные даты
),
smartbanners AS (
  SELECT
    date,
    is_retargeting,
    af_c_id AS af_cid,
    af_adset AS adset_name,
    mediasource,
    event_value,
    platform,
    eventname AS event_name,
    unig_event AS uniq_event_count,
    EventRevenue AS event_revenue,
    cnt_event AS event_count,
    
    TRIM(
        REGEXP_REPLACE(
            LOWER(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(campaign,  r'(_install_week.*)', '_install_weekend'), 
                    r'(_install_promo_gl.*)', '_install_promo_global'), 
                r'(_install_promo_re.*)', '_install_promo_regular')
            ), r'\+|-', '_')
        )
 AS campaign_name
  FROM `perekrestokvprok-bq`.`agg_data`.`AF_smartbanners_may_2022` --вместо campaign_name вставляем смартбаннеры
),
cte as (
  SELECT * FROM no_campaign
  UNION ALL
  SELECT * FROM campaign_name
  UNION ALL
  SELECT * FROM smartbanners
)
SELECT
  date,
  is_retargeting,
    CASE
        WHEN af_cid = 'campaign_id' THEN '61809857'
        ELSE af_cid END AS af_cid,
  adset_name,
    CASE 
        WHEN mediasource = 'mail.ru_int' and campaign_name = 'campaign_name' THEN 'yandexdirect_int'
        ELSE mediasource END AS mediasource,
  event_value,
  platform,
  event_name,
  uniq_event_count,
  event_revenue,
  event_count,
    CASE WHEN campaign_name = 'campaign_name' THEN 'realweb_ya_2022_and_ret_reg2_smartbanner'
         ELSE campaign_name END AS campaign_name,
FROM cte;

