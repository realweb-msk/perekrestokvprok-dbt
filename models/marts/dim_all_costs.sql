WITH google_ads AS (
    SELECT
        date,
        campaign_name,
        adset_name,
        costs,
        installs,
        clicks,
        impressions
    FROM {{ ref('int_google_cab_sheets')}}
),

yandex AS (
    SELECT
        date,
        campaign_name,
        adset_name,
        impressions,
        clicks,
        spend
    FROM {{ ref('int_yandex_cab_meta') }}
),

twitter AS (
    SELECT
        date,
        campaign_name,
        impressions,
        spend
    FROM {{ ref('int_twitter_cab') }}
),

facebook AS (
    SELECT
        date,
        campaign_name,
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
),

mytarget AS (
    SELECT
        date,
        campaign_name,
        adset_name,
        impressions,
        clicks,
        spend
    FROM {{ ref('int_mytarget_cab_meta') }}
),

asa AS (
    SELECT
        date,
        campaign_name,
        adset_name,
        impressions,
        clicks,
        spend
    FROM {{ ref('int_asa_cab_meta') }}
),

tiktok AS (
    SELECT
        unique_key,
        date,
        campaign_name,
        adset_name,
        impressions,
        reach,
        spend,
        clicks,
        purchase,
        first_purchase
    FROM {{ ref('stg_tiktok_cab_meta') }}
),

vk AS (
    SELECT
        date,
        campaign_name,
        adset_name,
        impressions,
        clicks,
        spend
    FROM {{ ref('int_vk_cab_meta') }}
),

google_dbm AS (
    SELECT
        date,
        campaign_name,
        adset_name,
        impressions,
        clicks,
        spend
    FROM {{ ref('int_google_dbm_dv360_cost') }}
),

mistake AS (
    SELECT
        mistake,
        correct
    FROM {{ source('sheets_data', 'mistake_cmp') }}
)

select *
from mistake
