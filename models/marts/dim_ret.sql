WITH af_conversions AS (
    SELECT
        date,
        is_retargeting,
        --af_cid,
        --adset_name,
        {{ promo_type('campaign_name', 'adset_name') }} as promo_type,
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
        SUM(installs) AS re_engagement,
        SUM(revenue) AS revenue,
        SUM(purchase) AS purchase,
        SUM(spend) AS spend,
        'Facebook' AS source
    FROM {{ ref('stg_facebook_cab_meta') }}
    WHERE campaign_type = 'retargeting'
    GROUP BY 1,2,3,4
),

----------------------- yandex -------------------------

yandex_cost AS (
    SELECT
        date,
        campaign_name,
        {{ promo_type('campaign_name', 'adset_name') }} as promo_type, 
        {{ platform('campaign_name') }} as platform,
        -- SUM(impressions),
        -- SUM(clicks),
        SUM(spend) AS spend
    FROM {{ ref('int_yandex_cab_meta') }}
    WHERE campaign_type = 'retargeting'
    AND REGEXP_CONTAINS(campaign_name, r'realweb')
    GROUP BY 1,2,3,4
),

yandex_convs AS (
    SELECT 
        date,
        campaign_name,
        platform,
        promo_type,
        SUM(IF(event_name = "af_purchase", event_revenue, 0)) AS revenue,
        SUM(IF(event_name = "af_purchase", event_count, 0)) AS purchase,
        SUM(IF(event_name = 're-engagement', event_count, 0)) AS re_engagement
    FROM af_conversions
    WHERE mediasource ='yandexdirect_int'
    AND is_retargeting = TRUE
    AND REGEXP_CONTAINS(campaign_name, r'realweb')
    AND REGEXP_CONTAINS(campaign_name, r'_ret_|[_\[]old[_\]]')
    GROUP BY 1,2,3,4
),

yandex AS (
    SELECT
        COALESCE(yandex_convs.date, yandex_cost.date) AS date,
        COALESCE(yandex_convs.campaign_name, yandex_cost.campaign_name) AS campaign_name,
        COALESCE(yandex_convs.platform, yandex_cost.platform) AS platform,
        COALESCE(yandex_convs.promo_type, yandex_cost.promo_type) AS promo_type,
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
        -- SUM(impressions),
        -- SUM(clicks),
        SUM(spend) AS spend
    FROM {{ ref('int_vk_cab_meta') }}
    WHERE campaign_type = 'retargeting'
    AND REGEXP_CONTAINS(campaign_name, r'realweb')
    GROUP BY 1,2,3,4
),

vk_convs AS (
    SELECT 
        date,
        campaign_name,
        platform,
        promo_type,
        SUM(IF(event_name = "af_purchase", event_revenue, 0)) AS revenue,
        SUM(IF(event_name = "af_purchase", event_count, 0)) AS purchase,
        SUM(IF(event_name = 're-engagement', event_count, 0)) AS re_engagement
    FROM af_conversions
    WHERE mediasource ='vk_int'
    AND is_retargeting = TRUE
    AND REGEXP_CONTAINS(campaign_name, r'realweb')
    AND REGEXP_CONTAINS(campaign_name, r'_ret_|[_\[]old[_\]]')
    GROUP BY 1,2,3,4
),

vk AS (
    SELECT
        COALESCE(vk_convs.date, vk_cost.date) AS date,
        COALESCE(vk_convs.campaign_name, vk_cost.campaign_name) AS campaign_name,
        COALESCE(vk_convs.platform, vk_cost.platform) AS platform,
        COALESCE(vk_convs.promo_type, vk_cost.promo_type) AS promo_type,
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
        -- SUM(impressions),
        -- SUM(clicks),
        SUM(spend) AS spend
    FROM {{ ref('int_mytarget_cab_meta') }}
    WHERE campaign_type = 'retargeting'
    AND REGEXP_CONTAINS(campaign_name, r'realweb')
    GROUP BY 1,2,3,4
),

mt_convs AS (
    SELECT 
        date,
        campaign_name,
        platform,
        promo_type,
        SUM(IF(event_name = "af_purchase", event_revenue, 0)) AS revenue,
        SUM(IF(event_name = "af_purchase", event_count, 0)) AS purchase,
        SUM(IF(event_name = 're-engagement', event_count, 0)) AS re_engagement
    FROM af_conversions
    WHERE mediasource = 'mail.ru_int'
    AND is_retargeting = TRUE
    AND REGEXP_CONTAINS(campaign_name, r'realweb')
    AND REGEXP_CONTAINS(campaign_name, r'_ret_|[_\[]old[_\]]')
    GROUP BY 1,2,3,4
),

mt AS (
    SELECT
        COALESCE(mt_convs.date, mt_cost.date) AS date,
        COALESCE(mt_convs.campaign_name, mt_cost.campaign_name) AS campaign_name,
        COALESCE(mt_convs.platform, mt_cost.platform) AS platform,
        COALESCE(mt_convs.promo_type, mt_cost.promo_type) AS promo_type,
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
        -- SUM(impressions),
        -- SUM(clicks),
        SUM(spend) AS spend
    FROM {{ ref('stg_twitter_cab_sheets') }}
    WHERE campaign_type = 'retargeting'
    AND REGEXP_CONTAINS(campaign_name, r'realweb_tw')
    GROUP BY 1,2,3,4
),

tw_convs AS (
    SELECT 
        date,
        campaign_name,
        platform,
        promo_type,
        SUM(IF(event_name = "af_purchase", event_revenue, 0)) AS revenue,
        SUM(IF(event_name = "af_purchase", event_count, 0)) AS purchase,
        SUM(IF(event_name = 're-engagement', event_count, 0)) AS re_engagement
    FROM af_conversions
    WHERE is_retargeting = TRUE
    AND REGEXP_CONTAINS(campaign_name, r'realweb_tw')
    AND REGEXP_CONTAINS(campaign_name, r'_ret_|[_\[]old[_\]]')
    GROUP BY 1,2,3,4
),

tw AS (
    SELECT
        COALESCE(tw_convs.date, tw_cost.date) AS date,
        COALESCE(tw_convs.campaign_name, tw_cost.campaign_name) AS campaign_name,
        COALESCE(tw_convs.platform, tw_cost.platform) AS platform,
        COALESCE(tw_convs.promo_type, tw_cost.promo_type) AS promo_type,
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
        -- SUM(impressions),
        -- SUM(clicks),
        SUM(spend) AS spend,
        -- для тиктока из кабинета:--
        SUM(purchase) AS purchase
    FROM {{ ref('stg_tiktok_cab_meta') }}
    WHERE campaign_type = 'retargeting'
    AND REGEXP_CONTAINS(campaign_name, r'realweb')
    GROUP BY 1,2,3,4
),

tiktok_convs AS (
    SELECT 
        date,
        campaign_name,
        platform,
        promo_type,
        SUM(IF(event_name = "af_purchase", event_revenue, 0)) AS revenue,
        --SUM(IF(event_name = "af_purchase", event_count, 0)) AS purchase,
        SUM(IF(event_name = 're-engagement', event_count, 0)) AS re_engagement
    FROM af_conversions
    WHERE  is_retargeting = TRUE
    AND REGEXP_CONTAINS(campaign_name, r'realweb_tiktok')
    AND REGEXP_CONTAINS(campaign_name, r'_ret_|[_\[]old[_\]]')
    GROUP BY 1,2,3,4
),

tiktok AS (
    SELECT
        COALESCE(tiktok_convs.date, tiktok_cost.date) AS date,
        COALESCE(tiktok_convs.campaign_name, tiktok_cost.campaign_name) AS campaign_name,
        COALESCE(tiktok_convs.platform, tiktok_cost.platform) AS platform,
        COALESCE(tiktok_convs.promo_type, tiktok_cost.promo_type) AS promo_type,
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
    WHERE COALESCE(re_engagement,0) + COALESCE(revenue,0) + COALESCE(purchase,0) + COALESCE(spend,0) > 0
    AND COALESCE(tiktok_convs.campaign_name, tiktok_cost.campaign_name) != 'None'
),

----------------------- apple search ads -------------------------

asa_cost AS (
    SELECT
        date,
        campaign_name,
        {{ promo_type('campaign_name', 'adset_name') }} as promo_type,
        'ios' as platform,
        -- SUM(impressions),
        -- SUM(clicks),
        SUM(spend) AS spend
    FROM {{ ref('int_asa_cab_meta') }}
    WHERE campaign_type = 'retargeting'
    GROUP BY 1,2,3,4
),

asa_convs AS (
    SELECT 
        date,
        campaign_name,
        platform,
        promo_type,
        SUM(IF(event_name = "af_purchase", event_revenue, 0)) AS revenue,
        SUM(IF(event_name = "af_purchase", event_count, 0)) AS purchase,
        SUM(IF(event_name = 're-engagement', event_count, 0)) AS re_engagement
    FROM af_conversions
    WHERE REGEXP_CONTAINS(campaign_name, r'\(r\)')
    GROUP BY 1,2,3,4
),

asa AS (
    SELECT
        COALESCE(asa_convs.date, asa_cost.date) AS date,
        COALESCE(asa_convs.campaign_name, asa_cost.campaign_name) AS campaign_name,
        COALESCE(asa_convs.platform, asa_cost.platform) AS platform,
        COALESCE(asa_convs.promo_type, asa_cost.promo_type) AS promo_type,
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
    GROUP BY 1,2,3,4
),

google_convs AS (
    SELECT 
        date,
        campaign_name,
        platform,
        promo_type,
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
    GROUP BY 1,2,3,4
),

google AS (
    SELECT
        COALESCE(google_convs.date, google_cost.date) AS date,
        COALESCE(google_convs.campaign_name, google_cost.campaign_name) AS campaign_name,
        COALESCE(google_convs.platform, google_cost.platform) AS platform,
        COALESCE(google_convs.promo_type, google_cost.promo_type) AS promo_type,
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
    WHERE COALESCE(re_engagement,0) + COALESCE(revenue,0) + COALESCE(purchase,0) + COALESCE(spend,0) > 0
    AND COALESCE(google_convs.campaign_name, google_cost.campaign_name) != 'None'
),

----------------------- inapp -------------------------

rate AS (
    SELECT
        start_date,
        end_date,
        partner,
        platform,
        rate_for_us
FROM {{ ref('stg_rate_info') }}
WHERE type = 'RTG'
),

inapp_events AS (
    SELECT DISTINCT
        date,
        campaign_name,
        platform,
        {{ partner('campaign_name') }} AS partner,
        promo_type,
        event_name,
        event_revenue,
        event_count,
        SUM(event_count) 
            OVER(PARTITION BY DATE_TRUNC(date, MONTH), event_name ORDER BY date, event_revenue)
            AS cum_event_count
        FROM af_conversions
        WHERE REGEXP_CONTAINS(campaign_name, r'inapp')
        AND is_retargeting = TRUE
),

inapp AS (
    SELECT
        date,
        campaign_name,
        inapp_events.platform,
        promo_type,
        SUM(IF(event_name = 're-engagement', event_count, 0)) AS re_engagement,
        SUM(IF(event_name = "af_purchase", event_revenue, 0)) AS revenue,
        SUM(IF(event_name = "af_purchase", event_count, 0)) AS purchase,
        SUM(
            CASE
                WHEN event_name = 'af_purchase' 
                    AND date BETWEEN '2021-10-01' AND '2021-10-31'
                    AND cum_event_count >= 3000 THEN event_count * 140
                WHEN event_name = 'af_purchase' 
                    AND date BETWEEN '2021-10-01' AND '2021-10-31'
                    AND cum_event_count < 3000 THEN event_count * rate_for_us
                WHEN event_name = 'af_purchase' 
                    AND date NOT BETWEEN '2021-10-01' AND '2021-10-31'
                    THEN event_count * rate_for_us
                ELSE 0 END
            ) AS spend,
        'inapp' AS source
    FROM inapp_events
    LEFT JOIN rate
    ON inapp_events.partner = rate.partner 
    AND inapp_events.date BETWEEN rate.start_date AND rate.end_date
    AND inapp_events.platform = rate.platform 
    GROUP BY 1,2,3,4
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
    UNION ALL
    SELECT * FROM facebook
    UNION ALL
    SELECT * FROM inapp
    UNION ALL
    SELECT * FROM google
)

SELECT 
    date,
    campaign_name,
    platform,
    promo_type,
    re_engagement,
    revenue,
    purchase,
    spend,
    source,
    {{ aud('campaign_name', 'source', 'platform') }} AS auditory
FROM final