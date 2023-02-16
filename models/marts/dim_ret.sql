/* 
для лучшего понимания лучше заглянуть сюда: https://github.com/realweb-msk/perekrestokvprok-dbt
или сюда: https://brave-hermann-395dc3.netlify.app/#!/model/model.perekrestokvprok.dim_ret
*/

WITH af_conversions AS (
    SELECT
        date,
        is_retargeting,
        af_cid,
        --adset_name,
        {{ promo_type('campaign_name', 'adset_name') }} as promo_type,
        {{ promo_search('campaign_name', 'adset_name') }} as promo_search,
        {{ aud('campaign_name', 'adset_name') }} AS auditory,
        mediasource,
        platform,
        CASE
            WHEN event_name IN ('re-attribution','re-engagement')
            THEN 're-engagement' 
            WHEN event_name = "af_purchase"
            THEN "af_purchase"
            ELSE 'no' END AS event_name,
        uniq_event_count,
        event_revenue,
        event_count,
        campaign_name
    FROM  {{ ref('stg_af_client_data') }}
    WHERE event_name IN('re-attribution','re-engagement',"af_purchase")
    -- WHERE is_retargeting = TRUE
    -- AND REGEXP_CONTAINS(campaign_name, 'realweb')
),

----------------------- facebook -------------------------

facebook AS (
    SELECT
        date,
        campaign_name,
        {{ platform('campaign_name') }} as platform,
        {{ promo_type('campaign_name', 'adset_name') }} as promo_type, 
        {{ promo_search('campaign_name', 'adset_name', 'ad_name') }} as promo_search,
        {{ aud('campaign_name', 'adset_name') }} AS auditory,
        SUM(installs) AS re_engagement,
        SUM(revenue) AS revenue,
        SUM(purchase) AS purchase,
        null AS first_purchase,
        null AS first_purchase_revenue,
        SUM(spend) AS spend,
        'Other' as base,
        'Facebook' AS source
    FROM {{ ref('stg_facebook_cab_sheets') }}
    --{{ ref('stg_facebook_cab_meta') }}
    WHERE campaign_type = 'retargeting'
    GROUP BY 1,2,3,4,5,6
),

----------------------- yandex -------------------------

yandex_cost AS (
    SELECT
        date,
        campaign_name,
        {{ promo_type('campaign_name', 'adset_name') }} as promo_type, 
        {{ platform('campaign_name') }} as platform,
        {{ promo_search('campaign_name', 'adset_name') }} as promo_search,
        {{ aud('campaign_name', 'adset_name') }} AS auditory,
        -- SUM(impressions),
        -- SUM(clicks),
        SUM(spend) AS spend
    FROM {{ ref('int_yandex_cab_meta') }}
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
        promo_search,
        auditory,
        SUM(IF(event_name = "af_purchase", event_revenue, 0)) AS revenue,
        SUM(IF(event_name = "af_purchase", event_count, 0)) AS purchase,
        SUM(IF(event_name = 're-engagement', event_count, 0)) AS re_engagement
    FROM af_conversions
    WHERE mediasource ='yandexdirect_int'
    AND is_retargeting = TRUE
    AND REGEXP_CONTAINS(campaign_name, r'realweb')
    AND REGEXP_CONTAINS(campaign_name, r'_ret_|[_\[]old[_\]]')
    GROUP BY 1,2,3,4,5,6
),

yandex AS (
    SELECT
        COALESCE(yandex_convs.date, yandex_cost.date) AS date,
        COALESCE(yandex_convs.campaign_name, yandex_cost.campaign_name) AS campaign_name,
        COALESCE(yandex_convs.platform, yandex_cost.platform) AS platform,
        COALESCE(yandex_convs.promo_type, yandex_cost.promo_type) AS promo_type,
        COALESCE(yandex_convs.promo_search, yandex_cost.promo_search) AS promo_search,
        COALESCE(yandex_convs.auditory, yandex_cost.auditory) AS auditory,
        COALESCE(re_engagement,0) AS re_engagement,
        COALESCE(revenue,0) AS revenue,
        COALESCE(purchase,0) AS purchase,
        null AS first_purchase,
        null AS first_purchase_revenue,
        COALESCE(spend,0) AS spend,
        'Other' as base,
        'Яндекс.Директ' AS source,
    FROM yandex_convs
    FULL OUTER JOIN yandex_cost
    ON yandex_convs.date = yandex_cost.date 
    AND yandex_convs.campaign_name = yandex_cost.campaign_name
    AND yandex_convs.promo_type = yandex_cost.promo_type
    AND yandex_convs.promo_search = yandex_cost.promo_search
    AND yandex_convs.auditory = yandex_cost.auditory
    WHERE COALESCE(re_engagement,0) + COALESCE(revenue,0) + COALESCE(purchase,0) + COALESCE(spend,0) > 0
    AND COALESCE(yandex_convs.campaign_name, yandex_cost.campaign_name) != 'None'
),

----------------------- vk -------------------------

vk_cost AS (
    SELECT
        date,
        campaign_name,
        {{ promo_type('campaign_name', 'adset_name') }} as promo_type,
        {{ platform('campaign_name') }} as platform,
        {{ promo_search('campaign_name', 'adset_name') }} as promo_search,
        {{ aud('campaign_name', 'adset_name') }} AS auditory,
        -- SUM(impressions),
        -- SUM(clicks),
        SUM(spend) AS spend
    FROM {{ ref('int_vk_cab_meta') }}
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
        promo_search,
        auditory,
        SUM(IF(event_name = "af_purchase", event_revenue, 0)) AS revenue,
        SUM(IF(event_name = "af_purchase", event_count, 0)) AS purchase,
        SUM(IF(event_name = 're-engagement', event_count, 0)) AS re_engagement
    FROM af_conversions
    WHERE mediasource ='vk_int'
    AND is_retargeting = TRUE
    AND REGEXP_CONTAINS(campaign_name, r'realweb')
    AND REGEXP_CONTAINS(campaign_name, r'_ret_|[_\[]old[_\]]')
    GROUP BY 1,2,3,4,5,6
),

vk AS (
    SELECT
        COALESCE(vk_convs.date, vk_cost.date) AS date,
        COALESCE(vk_convs.campaign_name, vk_cost.campaign_name) AS campaign_name,
        COALESCE(vk_convs.platform, vk_cost.platform) AS platform,
        COALESCE(vk_convs.promo_type, vk_cost.promo_type) AS promo_type,
        COALESCE(vk_convs.promo_search, vk_cost.promo_search) AS promo_search,
        COALESCE(vk_convs.auditory, vk_cost.auditory) AS auditory,
        COALESCE(re_engagement,0) AS re_engagement,
        COALESCE(revenue,0) AS revenue,
        COALESCE(purchase,0) AS purchase,
        null AS first_purchase,
        null AS first_purchase_revenue,
        COALESCE(spend,0) AS spend,
        'Other' as base,
        'ВК' AS source
    FROM vk_convs
    FULL OUTER JOIN vk_cost
    ON vk_convs.date = vk_cost.date 
    AND vk_convs.campaign_name = vk_cost.campaign_name
    AND vk_convs.promo_type = vk_cost.promo_type
    AND vk_convs.promo_search = vk_cost.promo_search
    AND vk_convs.auditory = vk_cost.auditory
    WHERE COALESCE(re_engagement,0) + COALESCE(revenue,0) + COALESCE(purchase,0) + COALESCE(spend,0) > 0
    AND COALESCE(vk_convs.campaign_name, vk_cost.campaign_name) != 'None'
),

----------------------- mytarget -------------------------

mt_cost AS (
    SELECT
        date,
        campaign_name,
        {{ promo_type('campaign_name', 'adset_name') }} as promo_type,
        {{ platform('campaign_name') }} as platform,
        {{ promo_search('campaign_name', 'adset_name') }} as promo_search,
        {{ aud('campaign_name', 'adset_name') }} AS auditory,
        -- SUM(impressions),
        -- SUM(clicks),
        SUM(spend) AS spend
    FROM {{ ref('int_mytarget_cab_meta') }}
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
        promo_search,
        auditory,
        SUM(IF(event_name = "af_purchase", event_revenue, 0)) AS revenue,
        SUM(IF(event_name = "af_purchase", event_count, 0)) AS purchase,
        SUM(IF(event_name = 're-engagement', event_count, 0)) AS re_engagement
    FROM af_conversions
    WHERE mediasource = 'mail.ru_int'
    AND is_retargeting = TRUE
    AND REGEXP_CONTAINS(campaign_name, r'realweb')
    AND REGEXP_CONTAINS(campaign_name, r'_ret_|[_\[]old[_\]]')
    GROUP BY 1,2,3,4,5,6
),

mt AS (
    SELECT
        COALESCE(mt_convs.date, mt_cost.date) AS date,
        COALESCE(mt_convs.campaign_name, mt_cost.campaign_name) AS campaign_name,
        COALESCE(mt_convs.platform, mt_cost.platform) AS platform,
        COALESCE(mt_convs.promo_type, mt_cost.promo_type) AS promo_type,
        COALESCE(mt_convs.promo_search, mt_cost.promo_search) AS promo_search,
        COALESCE(mt_convs.auditory, mt_cost.auditory) AS auditory,
        COALESCE(re_engagement,0) AS re_engagement,
        COALESCE(revenue,0) AS revenue,
        COALESCE(purchase,0) AS purchase,
        null AS first_purchase,
        null AS first_purchase_revenue,
        COALESCE(spend,0) AS spend,
        'Other' as base,
        'MyTarget' AS source
    FROM mt_convs
    FULL OUTER JOIN mt_cost
    ON mt_convs.date = mt_cost.date 
    AND mt_convs.campaign_name = mt_cost.campaign_name
    AND mt_convs.promo_type = mt_cost.promo_type
    AND mt_convs.promo_search = mt_cost.promo_search
    AND mt_convs.auditory = mt_cost.auditory
    WHERE COALESCE(re_engagement,0) + COALESCE(revenue,0) + COALESCE(purchase,0) + COALESCE(spend,0) > 0
    AND COALESCE(mt_convs.campaign_name, mt_cost.campaign_name) != 'None'
),

----------------------- twitter -------------------------

tw_cost AS (
    SELECT
        date,
        campaign_name,
        {{ promo_type('campaign_name', '"-"') }} as promo_type,
        {{ platform('campaign_name') }} as platform,
        {{ promo_search('campaign_name') }} as promo_search,
        {{ aud('campaign_name') }} AS auditory,
        -- SUM(impressions),
        -- SUM(clicks),
        SUM(spend) AS spend
    FROM {{ ref('stg_twitter_cab_sheets') }}
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
        promo_search,
        auditory,
        SUM(IF(event_name = "af_purchase", event_revenue, 0)) AS revenue,
        SUM(IF(event_name = "af_purchase", event_count, 0)) AS purchase,
        SUM(IF(event_name = 're-engagement', event_count, 0)) AS re_engagement
    FROM af_conversions
    WHERE is_retargeting = TRUE
    AND REGEXP_CONTAINS(campaign_name, r'realweb_tw')
    AND REGEXP_CONTAINS(campaign_name, r'_ret_|[_\[]old[_\]]')
    GROUP BY 1,2,3,4,5,6
),

tw AS (
    SELECT
        COALESCE(tw_convs.date, tw_cost.date) AS date,
        COALESCE(tw_convs.campaign_name, tw_cost.campaign_name) AS campaign_name,
        COALESCE(tw_convs.platform, tw_cost.platform) AS platform,
        COALESCE(tw_convs.promo_type, tw_cost.promo_type) AS promo_type,
        COALESCE(tw_convs.promo_search, tw_cost.promo_search) AS promo_search,
        COALESCE(tw_convs.auditory, tw_cost.auditory) AS auditory,
        COALESCE(re_engagement,0) AS re_engagement,
        COALESCE(revenue,0) AS revenue,
        COALESCE(purchase,0) AS purchase,
        null AS first_purchase,
        null AS first_purchase_revenue,
        COALESCE(spend,0) AS spend,
        'Other' as base,
        'Twitter' AS source
    FROM tw_convs
    FULL OUTER JOIN tw_cost
    ON tw_convs.date = tw_cost.date 
    AND tw_convs.campaign_name = tw_cost.campaign_name
    AND tw_convs.promo_type = tw_cost.promo_type
    AND tw_convs.promo_search = tw_cost.promo_search
    AND tw_convs.auditory = tw_cost.auditory
    WHERE COALESCE(re_engagement,0) + COALESCE(revenue,0) + COALESCE(purchase,0) + COALESCE(spend,0) > 0
    AND COALESCE(tw_convs.campaign_name, tw_cost.campaign_name) != 'None'
),

----------------------- tiktok -------------------------

tiktok_cost AS (
    SELECT
        date,
        campaign_name,
        {{ promo_type('campaign_name', 'adset_name') }} as promo_type,
        {{ platform('campaign_name') }} as platform,
        {{ promo_search('campaign_name', 'adset_name') }} as promo_search,
        {{ aud('campaign_name', 'adset_name') }} AS auditory,
        -- SUM(impressions),
        -- SUM(clicks),
        SUM(spend) AS spend,
        -- для тиктока из кабинета:--
        SUM(purchase) AS purchase
    FROM {{ ref('stg_tiktok_cab_meta') }}
    WHERE campaign_type = 'retargeting'
    AND REGEXP_CONTAINS(campaign_name, r'realweb')
    GROUP BY 1,2,3,4,5,6
),

tiktok_convs AS (
    SELECT 
        date,
        campaign_name,
        platform,
        promo_type,
        promo_search,
        auditory,
        SUM(IF(event_name = "af_purchase", event_revenue, 0)) AS revenue,
        --SUM(IF(event_name = "af_purchase", event_count, 0)) AS purchase,
        SUM(IF(event_name = 're-engagement', event_count, 0)) AS re_engagement
    FROM af_conversions
    WHERE  is_retargeting = TRUE
    AND REGEXP_CONTAINS(campaign_name, r'realweb_tiktok')
    AND REGEXP_CONTAINS(campaign_name, r'_ret_|[_\[]old[_\]]')
    GROUP BY 1,2,3,4,5,6
),

tiktok AS (
    SELECT
        COALESCE(tiktok_convs.date, tiktok_cost.date) AS date,
        COALESCE(tiktok_convs.campaign_name, tiktok_cost.campaign_name) AS campaign_name,
        COALESCE(tiktok_convs.platform, tiktok_cost.platform) AS platform,
        COALESCE(tiktok_convs.promo_type, tiktok_cost.promo_type) AS promo_type,
        COALESCE(tiktok_convs.promo_search, tiktok_cost.promo_search) AS promo_search,
        COALESCE(tiktok_convs.auditory, tiktok_cost.auditory) AS auditory,
        COALESCE(re_engagement,0) AS re_engagement,
        COALESCE(revenue,0) AS revenue,
        COALESCE(purchase,0) AS purchase,
        null AS first_purchase,
        null AS first_purchase_revenue,
        COALESCE(spend,0) AS spend,
        'Other' as base,
        'TikTok' AS source
    FROM tiktok_convs
    FULL OUTER JOIN tiktok_cost
    ON tiktok_convs.date = tiktok_cost.date 
    AND tiktok_convs.campaign_name = tiktok_cost.campaign_name
    AND tiktok_convs.promo_type = tiktok_cost.promo_type
    AND tiktok_convs.promo_search = tiktok_cost.promo_search
    AND tiktok_convs.auditory = tiktok_cost.auditory
    WHERE COALESCE(re_engagement,0) + COALESCE(revenue,0) + COALESCE(purchase,0) + COALESCE(spend,0) > 0
    AND COALESCE(tiktok_convs.campaign_name, tiktok_cost.campaign_name) != 'None'
),

----------------------- apple search ads -------------------------

asa_cost AS (
    SELECT
        date,
        campaign_name,
        {{ promo_type('campaign_name', 'adset_name') }} as promo_type,
        {{ promo_search('campaign_name', 'adset_name') }} as promo_search,
        {{ aud('campaign_name', 'adset_name') }} AS auditory,
        'ios' as platform,
        -- SUM(impressions),
        -- SUM(clicks),
        SUM(spend) AS spend
    FROM {{ ref('stg_asa_cab_sheets') }}
    --{{ ref('int_asa_cab_meta') }}
    WHERE campaign_type = 'retargeting'
    GROUP BY 1,2,3,4,5,6
),

asa_convs AS (
    SELECT 
        date,
        campaign_name,
        platform,
        promo_type,
        promo_search,
        auditory,
        SUM(IF(event_name = "af_purchase", event_revenue, 0)) AS revenue,
        SUM(IF(event_name = "af_purchase", event_count, 0)) AS purchase,
        SUM(IF(event_name = 're-engagement', event_count, 0)) AS re_engagement
    FROM af_conversions
    WHERE REGEXP_CONTAINS(campaign_name, r'\(r\)')
    GROUP BY 1,2,3,4,5,6
),

asa AS (
    SELECT
        COALESCE(asa_convs.date, asa_cost.date) AS date,
        COALESCE(asa_convs.campaign_name, asa_cost.campaign_name) AS campaign_name,
        COALESCE(asa_convs.platform, asa_cost.platform) AS platform,
        COALESCE(asa_convs.promo_type, asa_cost.promo_type) AS promo_type,
        COALESCE(asa_convs.promo_search, asa_cost.promo_search) AS promo_search,
        COALESCE(asa_convs.auditory, asa_cost.auditory) AS auditory,
        COALESCE(re_engagement,0) AS re_engagement,
        COALESCE(revenue,0) AS revenue,
        COALESCE(purchase,0) AS purchase,
        null AS first_purchase,
        null AS first_purchase_revenue,
        COALESCE(spend,0) AS spend,
        'Other' as base,
        'Apple Search Ads' AS source
    FROM asa_convs
    FULL OUTER JOIN asa_cost
    ON asa_convs.date = asa_cost.date 
    AND asa_convs.campaign_name = asa_cost.campaign_name
    AND asa_convs.promo_type = asa_cost.promo_type
    AND asa_convs.promo_search = asa_cost.promo_search
    AND asa_convs.auditory = asa_cost.auditory
    WHERE COALESCE(re_engagement,0) + COALESCE(revenue,0) + COALESCE(purchase,0) + COALESCE(spend,0) > 0
    AND COALESCE(asa_convs.campaign_name, asa_cost.campaign_name) != 'None'
),

----------------------- Google Ads -------------------------

google_cost AS (
    SELECT
        date,
        campaign_name,
        {{ promo_type('campaign_name', 'adset_name') }} as promo_type,
        {{ platform('campaign_name') }} as platform,
        {{ promo_search('campaign_name', 'adset_name') }} as promo_search,
        {{ aud('campaign_name', 'adset_name') }} AS auditory,
        -- SUM(impressions),
        -- SUM(clicks),
        SUM(spend) AS spend,
        SUM(installs) AS re_engagement
    FROM {{ ref('stg_google_cab_sheets') }}
    WHERE (campaign_type = 'retargeting'
    --- костыль 10.02.2022 X5RGPEREK-272 ---
    OR campaign_name IN (
            'realweb_uac_2022 [p:and] [cpi] [mskspb] [new] [general] [darkstore] [purchase] [firebase]',
            'realweb_uac_2022 [p:and] [cpi] [reg1] [new] [general] [darkstore] [purchase] [firebase]'))
    AND REGEXP_CONTAINS(campaign_name, r'realweb_|ohm')
    GROUP BY 1,2,3,4,5,6
),

google_convs AS (
    SELECT 
        date,
        campaign_name,
        platform,
        promo_type,
        promo_search,
        auditory,
        SUM(IF(event_name = "af_purchase", event_revenue, 0)) AS revenue,
        SUM(IF(event_name = "af_purchase", event_count, 0)) AS purchase,
    FROM af_conversions
    WHERE mediasource ='googleadwords_int'
    AND is_retargeting = TRUE
    AND REGEXP_CONTAINS(campaign_name, r'realweb|ohm')
    AND (REGEXP_CONTAINS(campaign_name,  r'[_\[]old[\]_]')
    OR campaign_name IN (
            'realweb_uac_2022 [p:and] [cpi] [mskspb] [new] [general] [darkstore] [purchase] [firebase]',
            'realweb_uac_2022 [p:and] [cpi] [reg1] [new] [general] [darkstore] [purchase] [firebase]'))
    GROUP BY 1,2,3,4,5,6
),

google AS (
    SELECT
        COALESCE(google_convs.date, google_cost.date) AS date,
        COALESCE(google_convs.campaign_name, google_cost.campaign_name) AS campaign_name,
        COALESCE(google_convs.platform, google_cost.platform) AS platform,
        COALESCE(google_convs.promo_type, google_cost.promo_type) AS promo_type,
        COALESCE(google_convs.promo_search, google_cost.promo_search) AS promo_search,
        COALESCE(google_convs.auditory, google_cost.auditory) AS auditory,
        COALESCE(re_engagement,0) AS re_engagement,
        COALESCE(revenue,0) AS revenue,
        COALESCE(purchase,0) AS purchase,
        null AS first_purchase,
        null AS first_purchase_revenue,
        COALESCE(spend,0) AS spend,
        'Other' as base,
        'Google Ads' AS source
    FROM google_convs
    FULL OUTER JOIN google_cost
    ON google_convs.date = google_cost.date 
    AND google_convs.campaign_name = google_cost.campaign_name
    AND google_convs.promo_type = google_cost.promo_type
    AND google_convs.promo_search = google_cost.promo_search
    AND google_convs.auditory = google_cost.auditory
    WHERE COALESCE(re_engagement,0) + COALESCE(revenue,0) + COALESCE(purchase,0) + COALESCE(spend,0) > 0
    AND COALESCE(google_convs.campaign_name, google_cost.campaign_name) != 'None'
),

----------------------- inapp -------------------------
af_inapp AS (
    SELECT
        date,
        is_retargeting,
        af_cid,
        adset_name,
        {{ promo_type('campaign_name', 'adset_name') }} as promo_type,
        {{ geo('campaign_name', 'adset_name') }} AS geo,
        {{ promo_search('campaign_name', 'adset_name') }} as promo_search,
        {{ aud('campaign_name', 'adset_name') }} AS auditory,
        mediasource,
        platform,
        CASE
            WHEN event_name IN ('re-attribution','re-engagement')
            THEN 're-engagement'
            ELSE event_name END AS event_name,
        uniq_event_count,
        event_revenue,
        event_count,
        campaign_name
    FROM {{ ref('stg_af_client_data') }}
    WHERE REGEXP_CONTAINS(campaign_name, 'realweb_inapp') AND is_retargeting = TRUE
    -- NOT REGEXP_CONTAINS(campaign_name, r'first_open_not_buy_rtg|installed_the_app_but_not_buy_rtg|registered_but_not_buy_rtg')
    -- WHERE is_retargeting = FALSE
    -- AND REGEXP_CONTAINS(campaign_name, 'realweb')
),

rate AS (
    SELECT
        start_date,
        end_date,
        partner,
        platform,
        rate_for_us,
        base
FROM {{ ref('stg_rate_info') }}
WHERE type = 'RTG'
),

limits_table AS (
    SELECT
        start_date,
        end_date,
        partner,
        limits
    FROM {{ ref('stg_partner_limits') }}
    WHERE type = 'RTG'
),

inapp_convs_without_cumulation AS (
    SELECT 
        date,
        campaign_name,
        {{ partner('campaign_name') }} AS partner,
        auditory,
        platform,
        CASE
      WHEN REGEXP_CONTAINS(campaign_name, r'deep_outflow') and date > '2022-10-04' THEN 'deep_outflow'
      WHEN date >= '2022-10-01' and date <= '2022-10-04' THEN 'deep_outflow'
      WHEN REGEXP_CONTAINS(campaign_name, r'first_open_not_buy_rtg') and date > '2022-10-04' THEN 'first_open_not_buy_rtg'
      WHEN REGEXP_CONTAINS(campaign_name, r'installed_the_app_but_not_buy_rtg') and date > '2022-10-04' THEN 'installed_the_app_but_not_buy_rtg'
      WHEN REGEXP_CONTAINS(campaign_name, r'registered_but_not_buy_rtg') and date > '2022-10-04' THEN 'registered_but_not_buy_rtg'
      ELSE 'Other'
    END as base,
        promo_type,
        promo_search,
        SUM(IF(event_name = 'first_purchase', event_revenue,0)) AS first_purchase_revenue,
        SUM(IF(event_name = 'first_purchase',event_count, 0)) AS first_purchase,
        SUM(IF(event_name = "af_purchase", event_revenue, 0)) AS revenue,
        SUM(IF(event_name = "af_purchase", event_count, 0)) AS purchase,
        SUM(IF(event_name = 're-engagement', event_count, 0)) AS re_engagement
    FROM af_inapp
    WHERE REGEXP_CONTAINS(campaign_name, r'realweb_inapp')
    AND is_retargeting = TRUE
    GROUP BY 1,2,3,4,5,6,7,8
),

inapp_convs_with_cumulation AS (
    SELECT
        date,
        campaign_name,
        partner,
        platform,
        base,
        promo_type,
        auditory,
        promo_search,
        first_purchase_revenue,
        first_purchase,
        revenue,
        purchase,
        re_engagement,
        SUM(first_purchase) 
            OVER(PARTITION BY DATE_TRUNC(date, MONTH), partner ORDER BY date, first_purchase_revenue)
            AS cum_first_purchase_count_by_prt,
        SUM(first_purchase) 
            OVER(PARTITION BY DATE_TRUNC(date, MONTH) ORDER BY date, first_purchase_revenue)
            AS cum_first_purchase,
        SUM(purchase) 
            OVER(PARTITION BY DATE_TRUNC(date, MONTH), partner ORDER BY date, revenue)
            AS cum_purchase_count_by_prt,
        SUM(purchase) 
            OVER(PARTITION BY DATE_TRUNC(date, MONTH) ORDER BY date, revenue)
            AS cum_purchase
    FROM inapp_convs_without_cumulation
),

inapp_convs AS (
    SELECT
        i.date,
        i.campaign_name,
        i.partner,
        i.platform,
        base,
        auditory,
        cum_purchase,
        i.promo_type,
        i.promo_search,
        IF(cum_first_purchase_count_by_prt <= COALESCE(l.limits, 1000000), first_purchase_revenue, 0) AS first_purchase_revenue,
        IF(cum_first_purchase_count_by_prt <= COALESCE(l.limits, 1000000), first_purchase, 0) AS first_purchase, 
        IF(cum_purchase_count_by_prt <= COALESCE(l.limits, 1000000), revenue, 0) AS revenue,
        IF(cum_purchase_count_by_prt <= COALESCE(l.limits, 1000000), purchase, 0) AS purchase,
        re_engagement
    FROM inapp_convs_with_cumulation i
    LEFT JOIN limits_table l
    ON i.partner = l.partner 
    AND i.date BETWEEN l.start_date AND l.end_date
),

inapp AS (
    SELECT
        date,
        campaign_name,
        inapp_convs.platform AS platform,
        promo_type,
        promo_search,
        auditory,
        re_engagement,
        COALESCE(revenue,0) AS revenue,
        COALESCE(purchase,0) AS purchase,
        COALESCE(first_purchase,0) AS first_purchase,
        COALESCE(first_purchase_revenue,0) AS first_purchase_revenue,
        CASE
                WHEN date BETWEEN '2021-10-01' AND '2021-10-31'
                    AND cum_purchase >= 3000 THEN purchase * 140
                WHEN date BETWEEN '2021-10-01' AND '2021-10-31'
                    AND cum_purchase < 3000 THEN purchase * rate_for_us
                WHEN date NOT BETWEEN '2021-10-01' AND '2021-10-31' AND date <= '2022-10-04'
                    THEN purchase * rate_for_us
                WHEN date > '2022-10-04'
                    AND REGEXP_CONTAINS(inapp_convs.base, r'first_open_not_buy_rtg|installed_the_app_but_not_buy_rtg|registered_but_not_buy_rtg')
                    THEN first_purchase * rate_for_us
                WHEN date > '2022-10-04'
                    AND REGEXP_CONTAINS(inapp_convs.base, r'deep_outflow')
                    THEN purchase * rate_for_us
                ELSE 0 END AS spend,
        inapp_convs.base as base,
        'inapp' AS source
    FROM inapp_convs
    LEFT JOIN rate
    ON inapp_convs.partner = rate.partner 
    AND inapp_convs.date BETWEEN rate.start_date AND rate.end_date
    AND inapp_convs.platform = rate.platform
    AND inapp_convs.base = rate.base 
    WHERE campaign_name != 'None'
),

--------------------------------Appnext------------------------------------------

appnext_cost AS (
    SELECT
        date,
        campaign_name,
        type,
        cost
    FROM {{ ref('stg_appnext_cost') }}
    WHERE
        REGEXP_CONTAINS(campaign_name,r'_u2_')
        AND cost != 0
),

Appnext_convs AS (
    SELECT 
        date,
        CASE
          WHEN af_cid = '514529' THEN 'realweb_p01_a999_c211_m404_u2_vprok_test1'
          ELSE campaign_name
        END AS campaign_name,
        platform,
        promo_type,
        promo_search,
        auditory,
        SUM(IF(event_name = "af_purchase", event_revenue, 0)) AS revenue,
        --SUM(IF(event_name = "af_purchase", event_count, 0)) AS purchase,
        SUM(IF(event_name = 're-engagement', event_count, 0)) AS re_engagement
    FROM af_conversions
    WHERE  is_retargeting = TRUE
    AND (REGEXP_CONTAINS(campaign_name, r'_c211') OR REGEXP_CONTAINS(af_cid, r'514529'))
    GROUP BY 1,2,3,4,5,6
),

Appnext AS (
    SELECT
        COALESCE(Appnext_convs.date, Appnext_cost.date) AS date,
        COALESCE(Appnext_convs.campaign_name, Appnext_cost.campaign_name) AS campaign_name,
        Appnext_convs.platform AS platform,
        Appnext_convs.promo_type AS promo_type,
        Appnext_convs.promo_search AS promo_search,
        Appnext_convs.auditory AS auditory,
        COALESCE(re_engagement,0) AS re_engagement,
        COALESCE(revenue,0) AS revenue,
        null AS purchase,
        null AS first_purchase,
        null AS first_purchase_revenue,
        appnext_cost.cost AS spend,
        'Other' as base,
        'Appnext' AS source
    FROM Appnext_convs
    FULL OUTER JOIN Appnext_cost
    ON  Appnext_convs.date = Appnext_cost.date 
    AND Appnext_convs.campaign_name = Appnext_cost.campaign_name
    --WHERE COALESCE(re_engagement,0) + COALESCE(revenue,0) + COALESCE(purchase,0) + COALESCE(spend,0) > 0
    --AND COALESCE(Appnext_convs.campaign_name, Appnext_cost.campaign_name) != 'None'
),



final AS (
    SELECT * FROM yandex
    UNION ALL 
    SELECT * FROM vk
    UNION ALL 
    SELECT * FROM mt
    UNION ALL 
    SELECT * FROM tw
    UNION ALL 
    SELECT * FROM tiktok
    UNION ALL
    SELECT * FROM asa
    WHERE date <= '2022-08-31'
    UNION ALL
    SELECT * FROM facebook
    UNION ALL
    SELECT * FROM inapp
    UNION ALL
    SELECT * FROM google
    UNION ALL
    SELECT * FROM Appnext
)

SELECT 
    date,
    campaign_name,
    platform,
    promo_type,
    promo_search,
    auditory,
    re_engagement,
    revenue,
    purchase,
    first_purchase,
    first_purchase_revenue,
    spend,
    base,
    source,
    {{ geo('campaign_name') }} AS geo
FROM final