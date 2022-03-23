/* 
для лучшего понимания лучше заглянуть сюда: https://github.com/realweb-msk/perekrestokvprok-dbt
или сюда: https://brave-hermann-395dc3.netlify.app/#!/model/model.perekrestokvprok.dim_ua
*/


WITH af_conversions AS (
    SELECT
        date,
        is_retargeting,
        af_cid,
        --adset_name,
        {{ promo_type('campaign_name', 'adset_name') }} as promo_type,
        {{ geo('campaign_name', 'adset_name') }} AS geo,
        {{ promo_search('campaign_name', 'adset_name') }} as promo_search,
        mediasource,
        platform,
        event_name,
        uniq_event_count,
        event_revenue,
        event_count,
        campaign_name
    FROM  {{ ref('stg_af_client_data') }}
    -- WHERE is_retargeting = FALSE
    -- AND REGEXP_CONTAINS(campaign_name, 'realweb')
),

----------------------- facebook -------------------------

facebook AS (
    SELECT
        date,
        campaign_name,
        {{ platform('campaign_name') }} as platform,
        {{ promo_type('campaign_name', 'adset_name') }} as promo_type,
        {{ geo('campaign_name', 'adset_name') }} AS geo,
        campaign_type,
        {{ promo_search('campaign_name', 'adset_name', 'ad_name') }} as promo_search,
        SUM(impressions) AS impressions,
        SUM(clicks) AS clicks,
        SUM(IF(campaign_type = 'UA', installs, 0)) AS installs,
        SUM(revenue) AS revenue,
        SUM(purchase) AS purchase,
        SUM(purchase) AS uniq_purchase,
        SUM(first_purchase_revenue) AS first_purchase_revenue,
        SUM(first_purchase) AS first_purchase,
        SUM(first_purchase) AS uniq_first_purchase,
        SUM(IF(campaign_type = 'UA', spend, 0)) AS spend,
        'Facebook' AS source,
        "social" as adv_type
    FROM {{ ref('stg_facebook_cab_sheets') }}
    --{{ ref('stg_facebook_cab_meta') }}
    GROUP BY 1,2,3,4,5,6,7
),

----------------------- yandex -------------------------

yandex_cost AS (
    SELECT
        date,
        campaign_name,
        {{ platform('campaign_name') }} as platform,
        {{ promo_type('campaign_name', 'adset_name') }} as promo_type,
        {{ geo('campaign_name', 'adset_name') }} AS geo,
        campaign_type,
        {{ promo_search('campaign_name', 'adset_name') }} as promo_search,
        SUM(impressions) AS impressions,
        SUM(clicks) AS clicks,
        SUM(spend) AS spend
    FROM {{ ref('int_yandex_cab_meta') }}
    WHERE campaign_type = 'UA'
    AND REGEXP_CONTAINS(campaign_name, r'realweb')
    GROUP BY 1,2,3,4,5,6,7
),

yandex_convs_ua AS (
    SELECT 
        date,
        campaign_name,
        platform,
        promo_type,
        geo,
        'UA' as campaign_type,
        promo_search,
        SUM(IF(event_name = 'install', event_count,0)) AS installs,
        SUM(IF(event_name = 'first_purchase', event_revenue,0)) AS first_purchase_revenue,
        SUM(IF(event_name = 'first_purchase',event_count, 0)) AS first_purchase,
        SUM(IF(event_name = 'first_purchase', uniq_event_count, 0)) AS uniq_first_purchase,
        SUM(IF(event_name = "af_purchase", event_revenue, 0)) AS revenue,
        SUM(IF(event_name = "af_purchase", event_count, 0)) AS purchase,
        SUM(IF(event_name = "af_purchase", uniq_event_count, 0)) AS uniq_purchase,
    FROM af_conversions
    WHERE is_retargeting = FALSE
    AND REGEXP_CONTAINS(campaign_name, r'realweb_ya')
    AND NOT REGEXP_CONTAINS(campaign_name, r'_ret_|[_\[]old[_\]]')
    GROUP BY 1,2,3,4,5,6,7
),

yandex_convs_rtg AS (
    SELECT 
        date,
        campaign_name,
        platform,
        promo_type,
        geo,
        'retargeting' AS campaign_type,
        promo_search,
        -- информация по покупкам в рет кампаниях должна быть в дашборде UA
        SUM(IF(event_name = 'install', 0,0)) AS installs,
        SUM(IF(event_name = 'first_purchase', event_revenue,0)) AS first_purchase_revenue,
        SUM(IF(event_name = 'first_purchase',event_count, 0)) AS first_purchase,
        SUM(IF(event_name = 'first_purchase', uniq_event_count, 0)) AS uniq_first_purchase,
        SUM(IF(event_name = "af_purchase", event_revenue, 0)) AS revenue,
        SUM(IF(event_name = "af_purchase", event_count, 0)) AS purchase,
        SUM(IF(event_name = "af_purchase", uniq_event_count, 0)) AS uniq_purchase,
    FROM af_conversions
    WHERE is_retargeting = TRUE --? 
    AND REGEXP_CONTAINS(campaign_name, r'realweb_ya')
    AND REGEXP_CONTAINS(campaign_name, r'_ret_|[_\[]old[_\]]')
    GROUP BY 1,2,3,4,5,6,7
),

yandex_convs AS (
    SELECT * FROM yandex_convs_ua
    UNION ALL 
    SELECT * FROM yandex_convs_rtg
),

yandex AS (
    SELECT
        COALESCE(yandex_convs.date, yandex_cost.date) AS date,
        COALESCE(yandex_convs.campaign_name, yandex_cost.campaign_name) AS campaign_name,
        COALESCE(yandex_convs.platform, yandex_cost.platform) AS platform,
        COALESCE(yandex_convs.promo_type, yandex_cost.promo_type) AS promo_type,
        COALESCE(yandex_convs.geo, yandex_cost.geo) AS geo,
        COALESCE(yandex_convs.campaign_type, yandex_cost.campaign_type) AS campaign_type,
        COALESCE(yandex_convs.promo_search, yandex_cost.promo_search) AS promo_search,
        COALESCE(impressions,0) AS impressions,
        COALESCE(clicks,0) AS clicks,
        COALESCE(installs,0) AS installs,
        COALESCE(revenue,0) AS revenue,
        COALESCE(purchase,0) AS purchase,
        COALESCE(uniq_purchase,0) AS uniq_purchase,
        COALESCE(first_purchase_revenue,0) AS first_purchase_revenue,
        COALESCE(first_purchase,0) AS first_purchase,
        COALESCE(uniq_first_purchase,0) AS uniq_first_purchase,
        COALESCE(spend,0) AS spend,
        'Яндекс.Директ' AS source,
        'context' AS adv_type
    FROM yandex_convs
    FULL OUTER JOIN yandex_cost
    ON yandex_convs.date = yandex_cost.date 
    AND yandex_convs.campaign_name = yandex_cost.campaign_name
    AND yandex_convs.promo_type = yandex_cost.promo_type
    AND yandex_convs.geo = yandex_cost.geo
    AND yandex_convs.promo_search = yandex_cost.promo_search
    WHERE 
        COALESCE(installs,0) + 
        COALESCE(revenue,0) + 
        COALESCE(purchase,0) + 
        COALESCE(uniq_purchase,0) +
        COALESCE(first_purchase_revenue,0) +
        COALESCE(first_purchase,0) + 
        COALESCE(uniq_first_purchase,0) +
        COALESCE(spend,0) > 0
    AND COALESCE(yandex_convs.campaign_name, yandex_cost.campaign_name) != 'None'
),

----------------------- mytarget -------------------------

mt_cost AS (
    SELECT
        date,
        campaign_name,
        {{ platform('campaign_name') }} as platform,
        {{ promo_type('campaign_name', 'adset_name') }} as promo_type,
        {{ geo('campaign_name', 'adset_name') }} AS geo,
        campaign_type,
        {{ promo_search('campaign_name', 'adset_name') }} as promo_search,
        SUM(impressions) AS impressions,
        SUM(clicks) AS clicks,
        SUM(spend) AS spend
    FROM {{ ref('int_mytarget_cab_meta') }}
    WHERE campaign_type = 'UA'
    AND REGEXP_CONTAINS(campaign_name, r'realweb')
    GROUP BY 1,2,3,4,5,6,7
),

mt_convs AS (
    SELECT 
        date,
        campaign_name,
        platform,
        promo_type,
        geo,
        'UA' as campaign_type,
        promo_search,
        SUM(IF(event_name = 'install', event_count,0)) AS installs,
        SUM(IF(event_name = 'first_purchase', event_revenue,0)) AS first_purchase_revenue,
        SUM(IF(event_name = 'first_purchase',event_count, 0)) AS first_purchase,
        SUM(IF(event_name = 'first_purchase', uniq_event_count, 0)) AS uniq_first_purchase,
        SUM(IF(event_name = "af_purchase", event_revenue, 0)) AS revenue,
        SUM(IF(event_name = "af_purchase", event_count, 0)) AS purchase,
        SUM(IF(event_name = "af_purchase", uniq_event_count, 0)) AS uniq_purchase,
    FROM af_conversions
    WHERE is_retargeting = FALSE
    AND REGEXP_CONTAINS(campaign_name, r'realweb_mt')
    AND REGEXP_CONTAINS(campaign_name, r'new')
    GROUP BY 1,2,3,4,5,6,7
),

mt AS (
    SELECT
        COALESCE(mt_convs.date, mt_cost.date) AS date,
        COALESCE(mt_convs.campaign_name, mt_cost.campaign_name) AS campaign_name,
        COALESCE(mt_convs.platform, mt_cost.platform) AS platform,
        COALESCE(mt_convs.promo_type, mt_cost.promo_type) AS promo_type,
        COALESCE(mt_convs.geo, mt_cost.geo) AS geo,
        COALESCE(mt_convs.campaign_type, mt_cost.campaign_type) AS campaign_type,
        COALESCE(mt_convs.promo_search, mt_cost.promo_search) AS promo_search,
        COALESCE(impressions,0) AS impressions,
        COALESCE(clicks,0) AS clicks,
        COALESCE(installs,0) AS installs,
        COALESCE(revenue,0) AS revenue,
        COALESCE(purchase,0) AS purchase,
        COALESCE(uniq_purchase,0) AS uniq_purchase,
        COALESCE(first_purchase_revenue,0) AS first_purchase_revenue,
        COALESCE(first_purchase,0) AS first_purchase,
        COALESCE(uniq_first_purchase,0) AS uniq_first_purchase,
        COALESCE(spend,0) AS spend,
        'MyTarget' AS source,
        'social' AS adv_type
    FROM mt_convs
    FULL OUTER JOIN mt_cost
    ON mt_convs.date = mt_cost.date 
    AND mt_convs.campaign_name = mt_cost.campaign_name
    AND mt_convs.promo_type = mt_cost.promo_type
    AND mt_convs.geo = mt_cost.geo
    AND mt_convs.promo_search = mt_cost.promo_search
    WHERE 
        COALESCE(installs,0) + 
        COALESCE(revenue,0) + 
        COALESCE(purchase,0) + 
        COALESCE(uniq_purchase,0) +
        COALESCE(first_purchase_revenue,0) +
        COALESCE(first_purchase,0) + 
        COALESCE(uniq_first_purchase,0) +
        COALESCE(spend,0) > 0
    AND COALESCE(mt_convs.campaign_name, mt_cost.campaign_name) != 'None'
),

----------------------- tiktok -------------------------

tiktok_cost AS (
    SELECT
        date,
        campaign_name,
        {{ platform('campaign_name') }} as platform,
        {{ promo_type('campaign_name', 'adset_name') }} as promo_type,
        {{ geo('campaign_name', 'adset_name') }} AS geo,
        campaign_type,
        {{ promo_search('campaign_name', 'adset_name') }} as promo_search,
        -- информация по покупкам в рет кампаниях должна быть в дашборде UA
        SUM(IF(campaign_type = 'UA',impressions,0)) AS impressions,
        SUM(IF(campaign_type = 'UA',clicks,0)) AS clicks,
        SUM(IF(campaign_type = 'UA',spend,0)) AS spend,
        SUM(purchase) AS purchase,
        SUM(first_purchase) AS first_purchase,
        SUM(SAFE_CAST(app_install AS INT64)) AS app_install
    FROM {{ ref('stg_tiktok_cab_meta') }}
    WHERE REGEXP_CONTAINS(campaign_name, r'realweb')
    GROUP BY 1,2,3,4,5,6,7
),

tiktok_convs AS (
    SELECT  
        date,
        campaign_name,
        platform,
        promo_type,
        geo,
        'UA' as campaign_type,
        promo_search,
        SUM(IF(event_name = 'install', event_count,0)) AS installs,
        SUM(IF(event_name = 'first_purchase', event_revenue,0)) AS first_purchase_revenue,
        --SUM(IF(event_name = 'first_purchase',event_count, 0)) AS first_purchase,
        SUM(IF(event_name = 'first_purchase', uniq_event_count, 0)) AS uniq_first_purchase,
        SUM(IF(event_name = "af_purchase", event_revenue, 0)) AS revenue,
        --SUM(IF(event_name = "af_purchase", event_count, 0)) AS purchase,
        SUM(IF(event_name = "af_purchase", uniq_event_count, 0)) AS uniq_purchase,
    FROM af_conversions
    WHERE is_retargeting = FALSE
    AND REGEXP_CONTAINS(campaign_name, r'realweb_tiktok')
    AND NOT REGEXP_CONTAINS(campaign_name, r'_ret_|[_\[]old[_\]]')
    GROUP BY 1,2,3,4,5,6,7
),

tiktok AS (
    SELECT
        COALESCE(tiktok_convs.date, tiktok_cost.date) AS date,
        COALESCE(tiktok_convs.campaign_name, tiktok_cost.campaign_name) AS campaign_name,
        COALESCE(tiktok_convs.platform, tiktok_cost.platform) AS platform,
        COALESCE(tiktok_convs.promo_type, tiktok_cost.promo_type) AS promo_type,
        COALESCE(tiktok_convs.geo, tiktok_cost.geo) AS geo,
        COALESCE(tiktok_convs.campaign_type, tiktok_cost.campaign_type) AS campaign_type,
        COALESCE(tiktok_convs.promo_search, tiktok_cost.promo_search) AS promo_search,
        COALESCE(impressions,0) AS impressions,
        COALESCE(clicks,0) AS clicks,
        COALESCE(installs, app_install, 0) AS installs,
        COALESCE(revenue,0) AS revenue,
        COALESCE(purchase,0) AS purchase,
        COALESCE(uniq_purchase,0) AS uniq_purchase,
        COALESCE(first_purchase_revenue,0) AS first_purchase_revenue,
        COALESCE(first_purchase,0) AS first_purchase,
        COALESCE(uniq_first_purchase,0) AS uniq_first_purchase,
        COALESCE(spend,0) AS spend,
        'TikTok' AS source,
        'social' AS adv_type
    FROM tiktok_convs
    FULL OUTER JOIN tiktok_cost
    ON tiktok_convs.date = tiktok_cost.date 
    AND tiktok_convs.campaign_name = tiktok_cost.campaign_name
    AND tiktok_convs.promo_type = tiktok_cost.promo_type
    AND tiktok_convs.geo = tiktok_cost.geo
    AND tiktok_convs.promo_search = tiktok_cost.promo_search
    WHERE 
        COALESCE(installs,0) + 
        COALESCE(revenue,0) + 
        COALESCE(purchase,0) + 
        COALESCE(uniq_purchase,0) +
        COALESCE(first_purchase_revenue,0) +
        COALESCE(first_purchase,0) + 
        COALESCE(uniq_first_purchase,0) +
        COALESCE(spend,0) > 0
    AND COALESCE(tiktok_convs.campaign_name, tiktok_cost.campaign_name) != 'None'
),

----------------------- apple search ads -------------------------

asa_cost AS (
    SELECT
        date,
        campaign_name,
        'ios' as platform,
        {{ promo_type('campaign_name', 'adset_name') }} as promo_type,
        {{ geo('campaign_name', 'adset_name') }} AS geo,
        campaign_type,
        {{ promo_search('campaign_name', 'adset_name') }} as promo_search,
        SUM(meta.impressions) AS impressions,
        SUM(sheet.clicks) AS clicks,
        SUM(sheet.spend) AS spend
    FROM {{ ref('stg_asa_cab_sheets') }} sheet
    --{{ ref('int_asa_cab_meta') }}
    LEFT JOIN {{ ref('int_asa_cab_meta') }} meta
    USING(date, campaign_name, campaign_type, adset_name)
    WHERE campaign_type = 'UA'
    GROUP BY 1,2,3,4,5,6,7
),

asa_convs AS (
    SELECT 
        date,
        campaign_name,
        platform,
        promo_type,
        geo,
        'UA' as campaign_type,
        promo_search,
        SUM(IF(event_name = 'install', event_count,0)) AS installs,
        SUM(IF(event_name = 'first_purchase', event_revenue,0)) AS first_purchase_revenue,
        SUM(IF(event_name = 'first_purchase',event_count, 0)) AS first_purchase,
        SUM(IF(event_name = 'first_purchase', uniq_event_count, 0)) AS uniq_first_purchase,
        SUM(IF(event_name = "af_purchase", event_revenue, 0)) AS revenue,
        SUM(IF(event_name = "af_purchase", event_count, 0)) AS purchase,
        SUM(IF(event_name = "af_purchase", uniq_event_count, 0)) AS uniq_purchase,
    FROM af_conversions
    WHERE NOT REGEXP_CONTAINS(campaign_name, r'\(r\)')
    AND (
        REGEXP_CONTAINS(campaign_name, r'\(exact\)|зоо') OR
        mediasource = 'Apple Search Ads'
    )
    AND is_retargeting = FALSE
    GROUP BY 1,2,3,4,5,6,7
),

asa AS (
    SELECT
        COALESCE(asa_convs.date, asa_cost.date) AS date,
        COALESCE(asa_convs.campaign_name, asa_cost.campaign_name) AS campaign_name,
        COALESCE(asa_convs.platform, asa_cost.platform) AS platform,
        COALESCE(asa_convs.promo_type, asa_cost.promo_type) AS promo_type,
        COALESCE(asa_convs.geo, asa_cost.geo) AS geo,
        COALESCE(asa_convs.campaign_type, asa_cost.campaign_type) AS campaign_type,
        COALESCE(asa_convs.promo_search, asa_cost.promo_search) AS promo_search,
        COALESCE(impressions,0) AS impressions,
        COALESCE(clicks,0) AS clicks,
        COALESCE(installs,0) AS installs,
        COALESCE(revenue,0) AS revenue,
        COALESCE(purchase,0) AS purchase,
        COALESCE(uniq_purchase,0) AS uniq_purchase,
        COALESCE(first_purchase_revenue,0) AS first_purchase_revenue,
        COALESCE(first_purchase,0) AS first_purchase,
        COALESCE(uniq_first_purchase,0) AS uniq_first_purchase,
        COALESCE(spend,0) AS spend,
        'Apple Search Ads' AS source,
        'context' AS adv_type
    FROM asa_convs
    FULL OUTER JOIN asa_cost
    ON asa_convs.date = asa_cost.date 
    AND asa_convs.campaign_name = asa_cost.campaign_name
    AND asa_convs.promo_type = asa_cost.promo_type
    AND asa_convs.geo = asa_cost.geo
    AND asa_convs.promo_search = asa_cost.promo_search
    WHERE 
        COALESCE(installs,0) + 
        COALESCE(revenue,0) + 
        COALESCE(purchase,0) + 
        COALESCE(uniq_purchase,0) +
        COALESCE(first_purchase_revenue,0) +
        COALESCE(first_purchase,0) + 
        COALESCE(uniq_first_purchase,0) +
        COALESCE(spend,0) > 0
    AND COALESCE(asa_convs.campaign_name, asa_cost.campaign_name) != 'None'
),

----------------------- google -------------------------

google_cost AS (
    SELECT
        date,
        campaign_name,
        {{ platform('campaign_name') }} as platform,
        {{ promo_type('campaign_name', 'adset_name') }} as promo_type,
        {{ geo('campaign_name', 'adset_name') }} AS geo,
        campaign_type,
        {{ promo_search('campaign_name', 'adset_name') }} as promo_search,
        SUM(impressions) AS impressions,
        SUM(clicks) AS clicks,
        SUM(spend) AS spend,
        SUM(IF({{ platform('campaign_name') }} = 'ios', installs, NULL)) AS installs
    FROM {{ ref('stg_google_cab_sheets') }}
    WHERE campaign_type = 'UA'
    AND REGEXP_CONTAINS(campaign_name, r'realweb')
    AND campaign_name NOT IN (
            'realweb_uac_2022 [p:and] [cpi] [mskspb] [new] [general] [darkstore] [purchase] [firebase]',
            'realweb_uac_2022 [p:and] [cpi] [reg1] [new] [general] [darkstore] [purchase] [firebase]')
    GROUP BY 1,2,3,4,5,6,7
),

google_convs AS (
    SELECT 
        date,
        campaign_name,
        platform,
        promo_type,
        geo,
        'UA' as campaign_type,
        promo_search,
        SUM(IF(event_name = 'install', event_count,0)) AS installs,
        SUM(IF(event_name = 'first_purchase', event_revenue,0)) AS first_purchase_revenue,
        SUM(IF(event_name = 'first_purchase',event_count, 0)) AS first_purchase,
        SUM(IF(event_name = 'first_purchase', uniq_event_count, 0)) AS uniq_first_purchase,
        SUM(IF(event_name = "af_purchase", event_revenue, 0)) AS revenue,
        SUM(IF(event_name = "af_purchase", event_count, 0)) AS purchase,
        SUM(IF(event_name = "af_purchase", uniq_event_count, 0)) AS uniq_purchase,
    FROM af_conversions
    WHERE REGEXP_CONTAINS(campaign_name, r'realweb_uac_')
    AND is_retargeting = FALSE
    AND campaign_name NOT IN (
            'realweb_uac_2022 [p:and] [cpi] [mskspb] [new] [general] [darkstore] [purchase] [firebase]',
            'realweb_uac_2022 [p:and] [cpi] [reg1] [new] [general] [darkstore] [purchase] [firebase]')
    GROUP BY 1,2,3,4,5,6,7
),

google AS (
    SELECT
        COALESCE(google_convs.date, google_cost.date) AS date,
        COALESCE(google_convs.campaign_name, google_cost.campaign_name) AS campaign_name,
        COALESCE(google_convs.platform, google_cost.platform) AS platform,
        COALESCE(google_convs.promo_type, google_cost.promo_type) AS promo_type,
        COALESCE(google_convs.geo, google_cost.geo) AS geo,
        COALESCE(google_convs.campaign_type, google_cost.campaign_type) AS campaign_type,
        COALESCE(google_convs.promo_search, google_cost.promo_search) AS promo_search,
        COALESCE(impressions,0) AS impressions,
        COALESCE(clicks,0) AS clicks,
        COALESCE(google_cost.installs,google_convs.installs,0) AS installs,
        COALESCE(revenue,0) AS revenue,
        COALESCE(purchase,0) AS purchase,
        COALESCE(uniq_purchase,0) AS uniq_purchase,
        COALESCE(first_purchase_revenue,0) AS first_purchase_revenue,
        COALESCE(first_purchase,0) AS first_purchase,
        COALESCE(uniq_first_purchase,0) AS uniq_first_purchase,
        COALESCE(spend,0) AS spend,
        'Google Ads' AS source,
        'context' AS adv_type
    FROM google_convs
    FULL OUTER JOIN google_cost
    ON google_convs.date = google_cost.date 
    AND google_convs.campaign_name = google_cost.campaign_name
    AND google_convs.promo_type = google_cost.promo_type
    AND google_convs.geo = google_cost.geo
    AND google_convs.promo_search = google_cost.promo_search
    WHERE 
        COALESCE(google_cost.installs,google_convs.installs,0) + 
        COALESCE(revenue,0) + 
        COALESCE(purchase,0) + 
        COALESCE(uniq_purchase,0) +
        COALESCE(first_purchase_revenue,0) +
        COALESCE(first_purchase,0) + 
        COALESCE(uniq_first_purchase,0) +
        COALESCE(spend,0) > 0
    AND COALESCE(google_convs.campaign_name, google_cost.campaign_name) != 'None'
),

----------------------- huawei -------------------------

huawei_cost AS (
    ---- TODO: поменять на источник костов -----
    SELECT
        DATE('2010-12-31') date,
        campaign_name,
        {{ platform('campaign_name') }} as platform,
        {{ promo_type('campaign_name', 'adset_name') }} as promo_type,
        {{ geo('campaign_name', 'adset_name') }} AS geo,
        {{ promo_search('campaign_name', 'adset_name') }} as promo_search,
        campaign_type,
        SUM(impressions) AS impressions,
        SUM(clicks) AS clicks,
        SUM(spend) AS spend
    FROM {{ ref('int_mytarget_cab_meta') }}
    WHERE campaign_type = 'UA'
    AND REGEXP_CONTAINS(campaign_name, r'realweb')
    GROUP BY 1,2,3,4,5,6,7
),

huawei_convs AS (
    SELECT 
        date,
        campaign_name,
        platform,
        promo_type,
        geo,
        'UA' as campaign_type,
        promo_search,
        SUM(IF(event_name = 'install', event_count,0)) AS installs,
        SUM(IF(event_name = 'first_purchase', event_revenue,0)) AS first_purchase_revenue,
        SUM(IF(event_name = 'first_purchase',event_count, 0)) AS first_purchase,
        SUM(IF(event_name = 'first_purchase', uniq_event_count, 0)) AS uniq_first_purchase,
        SUM(IF(event_name = "af_purchase", event_revenue, 0)) AS revenue,
        SUM(IF(event_name = "af_purchase", event_count, 0)) AS purchase,
        SUM(IF(event_name = "af_purchase", uniq_event_count, 0)) AS uniq_purchase,
    FROM af_conversions
    WHERE is_retargeting = FALSE
    AND REGEXP_CONTAINS(campaign_name, r'realweb_hw')
    AND REGEXP_CONTAINS(campaign_name, r'new')
    GROUP BY 1,2,3,4,5,6,7
),

huawei AS (
    SELECT
        COALESCE(huawei_convs.date, huawei_cost.date) AS date,
        COALESCE(huawei_convs.campaign_name, huawei_cost.campaign_name) AS campaign_name,
        COALESCE(huawei_convs.platform, huawei_cost.platform) AS platform,
        COALESCE(huawei_convs.promo_type, huawei_cost.promo_type) AS promo_type,
        COALESCE(huawei_convs.geo, huawei_cost.geo) AS geo,
        COALESCE(huawei_convs.campaign_type, huawei_cost.campaign_type) AS campaign_type,
        COALESCE(huawei_convs.promo_search, huawei_cost.promo_search) AS promo_search,
        COALESCE(impressions,0) AS impressions,
        COALESCE(clicks,0) AS clicks,
        COALESCE(installs,0) AS installs,
        COALESCE(revenue,0) AS revenue,
        COALESCE(purchase,0) AS purchase,
        COALESCE(uniq_purchase,0) AS uniq_purchase,
        COALESCE(first_purchase_revenue,0) AS first_purchase_revenue,
        COALESCE(first_purchase,0) AS first_purchase,
        COALESCE(uniq_first_purchase,0) AS uniq_first_purchase,
        COALESCE(spend,0) AS spend,
        'Huawei' AS source,
        'context' AS adv_type
    FROM huawei_convs
    FULL OUTER JOIN huawei_cost
    ON huawei_convs.date = huawei_cost.date 
    AND huawei_convs.campaign_name = huawei_cost.campaign_name
    AND huawei_convs.promo_type = huawei_cost.promo_type
    AND huawei_convs.geo = huawei_cost.geo
    AND huawei_convs.promo_search = huawei_cost.promo_search
    WHERE 
        COALESCE(installs,0) + 
        COALESCE(revenue,0) + 
        COALESCE(purchase,0) + 
        COALESCE(uniq_purchase,0) +
        COALESCE(first_purchase_revenue,0) +
        COALESCE(first_purchase,0) + 
        COALESCE(uniq_first_purchase,0) +
        COALESCE(spend,0) > 0
    AND COALESCE(huawei_convs.campaign_name, huawei_cost.campaign_name) != 'None'
),

----------------------inapp----------------------------

rate AS (
    SELECT
        start_date,
        end_date,
        partner,
        platform,
        rate_for_us
FROM {{ ref('stg_rate_info') }}
WHERE type = 'UA'
),

limits_table AS (
    SELECT
        start_date,
        end_date,
        partner,
        limits
    FROM {{ ref('stg_partner_limits') }}
    WHERE type = 'UA'
),

inapp_convs_without_cumulation AS (
    SELECT 
        date,
        campaign_name,
        {{ partner('campaign_name') }} AS partner,
        platform,
        promo_type,
        geo,
        'UA' as campaign_type,
        promo_search,
        SUM(IF(event_name = 'install', event_count,0)) AS installs,
        SUM(IF(event_name = 'first_purchase', event_revenue,0)) AS first_purchase_revenue,
        SUM(IF(event_name = 'first_purchase',event_count, 0)) AS first_purchase,
        SUM(IF(event_name = 'first_purchase', uniq_event_count, 0)) AS uniq_first_purchase,
        SUM(IF(event_name = "af_purchase", event_revenue, 0)) AS revenue,
        SUM(IF(event_name = "af_purchase", event_count, 0)) AS purchase,
        SUM(IF(event_name = "af_purchase", uniq_event_count, 0)) AS uniq_purchase,
    FROM af_conversions
    WHERE REGEXP_CONTAINS(campaign_name, r'realweb_inapp')
    AND is_retargeting = FALSE
    GROUP BY 1,2,3,4,5,6,7,8
),

inapp_convs_with_cumulation AS (
    SELECT
        date,
        campaign_name,
        partner,
        platform,
        promo_type,
        geo,
        campaign_type,
        promo_search,
        installs,
        first_purchase_revenue,
        first_purchase,
        uniq_first_purchase,
        revenue,
        purchase,
        uniq_purchase,
        SUM(first_purchase) 
            OVER(PARTITION BY DATE_TRUNC(date, MONTH), partner ORDER BY date, first_purchase_revenue)
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
        geo,
        campaign_type,
        promo_search,
        installs,
        IF(cum_event_count_by_prt <= COALESCE(limits, 1000000), first_purchase_revenue, 0) AS first_purchase_revenue,
        IF(cum_event_count_by_prt <= COALESCE(limits, 1000000), first_purchase, 0) AS first_purchase,
        IF(cum_event_count_by_prt <= COALESCE(limits, 1000000), uniq_first_purchase, 0) AS uniq_first_purchase,
        revenue,
        purchase,
        uniq_purchase,
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
        geo,
        campaign_type,
        promo_search,
        0 AS impressions,
        0 AS clicks,
        COALESCE(installs,0) AS installs,
        COALESCE(revenue,0) AS revenue,
        COALESCE(purchase,0) AS purchase,
        COALESCE(uniq_purchase,0) AS uniq_purchase,
        COALESCE(first_purchase_revenue,0) AS first_purchase_revenue,
        COALESCE(first_purchase,0) AS first_purchase,
        COALESCE(uniq_first_purchase,0) AS uniq_first_purchase,
        COALESCE(first_purchase * rate_for_us,0)  AS spend,
        'inapp' AS source,
        'inapp' AS adv_type
    FROM inapp_convs
    LEFT JOIN rate
    ON inapp_convs.partner = rate.partner 
    AND inapp_convs.date BETWEEN rate.start_date AND rate.end_date
    AND inapp_convs.platform = rate.platform 
    WHERE 
        COALESCE(installs,0) + 
        COALESCE(revenue,0) + 
        COALESCE(purchase,0) + 
        COALESCE(uniq_purchase,0) +
        COALESCE(first_purchase_revenue,0) +
        COALESCE(first_purchase,0) + 
        COALESCE(uniq_first_purchase,0) +
        COALESCE(first_purchase * rate_for_us,0) > 0
    AND campaign_name != 'None'
),

----------------------final----------------------------

unions AS (
    SELECT * FROM yandex
    UNION ALL 
    SELECT * FROM mt
    UNION ALL  
    SELECT * FROM tiktok
    UNION ALL
    SELECT * FROM asa
    UNION ALL
    SELECT * FROM facebook
    UNION ALL
    SELECT * FROM google
    UNION ALL
    SELECT * FROM huawei
    WHERE date != '2010-12-31'
    UNION ALL
    SELECT * FROM inapp
),

final AS (
    SELECT 
        date,
        campaign_name,
        platform,
        promo_type,
        geo,
        campaign_type,
        promo_search,
        impressions,
        clicks,
        installs,
        revenue,
        purchase,
        uniq_purchase,
        first_purchase_revenue,
        first_purchase,
        uniq_first_purchase,
        spend,
        source,
        {{ conversion_source_type('campaign_name', 'source') }} AS conversion_source_type,
        adv_type
    FROM unions
)

SELECT 
    date,
    campaign_name,
    platform,
    promo_type,
    geo,
    campaign_type,
    promo_search,
    impressions,
    clicks,
    installs,
    revenue,
    purchase,
    uniq_purchase,
    first_purchase_revenue,
    first_purchase,
    uniq_first_purchase,
    spend,
    source,
    conversion_source_type,
    adv_type
FROM final
