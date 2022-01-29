WITH af_conversions AS (
    SELECT
        date,
        is_retargeting,
        af_cid,
        adset_name,
        mediasource,
        platform,
        event_name,
        uniq_event_count,
        event_revenue,
        event_count,
        campaign_name
    FROM  {{ ref('stg_af_client_data') }}
    WHERE is_retargeting = TRUE
    -- AND REGEXP_CONTAINS(campaign_name, 'realweb')
)

facebook AS (
    SELECT
        unique_key,
        date,
        campaign_name,
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
    FROM {{ ref('stg_facebook_cab_meta') }}
    WHERE campaign_type = 'retargeting'
),

yandex_cost AS (
    SELECT
        date,
        campaign_name,
        campaign_type,
        adset_name,
        impressions,
        clicks,
        spend
    FROM {{ ref('int_yandex_cab_meta') }}
)

select *
from yandex_conv