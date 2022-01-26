WITH 
facebook_storage AS (
    SELECT DISTINCT
        date,
        campaign_name,
        adset_name,
        ad_name,
        show,
        clicks,
        installs,
        spend,
        cnt_af_purchase,
        revenue,
        cnt_first_purchase,
        first_purchase_revenue,
        add_to_card
    FROM {{ ref('stg_facebook_cab') }}
    WHERE date < DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY)
),

facebook_sheet AS (
    SELECT DISTINCT
        date,
        campaign as campaign_name,
        adset as adset_name,
        ad as ad_name,
        SUM(IFNULL(show,0)) as show,
        SUM(IFNULL(clicks,0)) as clicks,
        SUM(IFNULL(installs,0)) as installs,
        SUM(IFNULL(spend,0)) as spend,
        SUM(IFNULL(purchase,0)) as cnt_af_purchase,
        SUM(IFNULL(purchase_revenue,0)) as revenue,
        SUM(IFNULL(first_purchase,0)) as cnt_first_purchase,
        SUM(IFNULL(first_purchase_revenue,0)) as first_purchase_revenue,
        SUM(IFNULL(ad_to_cars,0)) as add_to_card,
    FROM {{ source('sheets_data', 'fb_spreadsheets_data') }}
    WHERE date >= DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY)
    GROUP BY 1,2,3,4
),

final AS (
    SELECT *
    FROM facebook_storage
    UNION DISTINCT 
    SELECT *
    FROM facebook_sheet
)

SELECT 
    date,
    campaign_name,
    adset_name,
    ad_name,
    show AS impressions,
    clicks,
    installs,
    spend AS costs,
    cnt_af_purchase AS purchase,
    revenue,
    cnt_first_purchase AS first_purchase,
    first_purchase_revenue,
    add_to_card
FROM final