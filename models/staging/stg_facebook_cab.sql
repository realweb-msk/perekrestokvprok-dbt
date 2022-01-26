{% if target.name == 'prod' %}

  
{{
  config(
    materialized = 'incremental',
    partition_by = {
      "field": "date",
      "data_type": "date",
      "granularity": "day"
    },
    cluster_by = ["campaign_name"],
  )
}}

{% endif %}

{% if not is_incremental() %}

    SELECT DISTINCT
        date,
        campaign_name,
        adset_name,
        ad_name,
        show,
        clicks,
        installs,
        spend,
        cnt_installs,
        cnt_af_purchase,
        revenue,
        cnt_first_purchase,
        first_purchase_revenue,
        add_to_card
    FROM {{ source('sheets_data', 'FBNEW_data') }}

    UNION ALL

{% endif %}

-- SELECT DISTINCT
--     date,
--     campaign_name,
--     adset_name,
--     ad_name,
--     show,
--     clicks,
--     installs,
--     spend,
--     cnt_installs,
--     cnt_af_purchase,
--     revenue,
--     cnt_first_purchase,
--     first_purchase_revenue,
--     add_to_card
-- FROM {{ this }}

-- UNION ALL 

SELECT 
    date,
    campaign as campaign_name,
    adset as adset_name,
    ad as ad_name,
    SUM(IFNULL(show,0)) as show,
    SUM(IFNULL(clicks,0)) as clicks,
    SUM(IFNULL(installs,0)) as installs,
    SUM(IFNULL(spend,0)) as spend,
    SUM(IFNULL(installs,0)) as cnt_installs,
    SUM(IFNULL(purchase,0)) as cnt_af_purchase,
    SUM(IFNULL(purchase_revenue,0)) as revenue,
    SUM(IFNULL(first_purchase,0)) as cnt_first_purchase,
    SUM(IFNULL(first_purchase_revenue,0)) as first_purchase_revenue,
    SUM(IFNULL(ad_to_cars,0)) as add_to_card,
FROM {{ source('sheets_data', 'fb_spreadsheets_data') }}
WHERE date IS NOT NULL

{% if is_incremental() %}

    AND ARRAY_TO_STRING([campaign,adset,ad,CAST(date AS STRING)],'') NOT IN 
    (
        SELECT DISTINCT ARRAY_TO_STRING([campaign,adset,ad,CAST(date AS STRING)],'') 
        FROM {{ this }}
    )

{% endif %}

GROUP BY 1,2,3,4