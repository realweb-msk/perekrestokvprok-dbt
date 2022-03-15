WITH source_ios AS (
    SELECT 
        date,
        Agency_PMD__af_prt_	AS agency,
        Media_Source__pid_ AS media_source,
        Campaign__c_ AS campaign,
        'retargeting' as report_type,
        'ios' AS platform,
        clicks,
        conversions,
        sessions,
        total_revenue,
        total_cost,
        af_purchase__Event_counter_ AS purchase,
        af_purchase__Sales_in_RUB_ AS revenue,
        af_purchase_rw__Event_counter_ AS rw_purchase,
        af_purchase_rw__Sales_in_RUB_ AS rw_revenue,
        first_purchase__Event_counter_ AS first_purchase,
        first_purchase__Sales_in_RUB_ AS first_purchase_revenue,
        first_purchase_rw__Event_counter_ AS rw_first_purchase,
        first_purchase_rw__Sales_in_RUB_ AS rw_first_purchase_revenue,
        ROW_NUMBER() OVER(PARTITION BY date, Media_Source__pid_ , Campaign__c_ ORDER BY _TABLE_SUFFIX DESC) AS counter
    FROM  {{ source('AF_data', 'ios_rtg_partners_by_date_report_*') }}
),

source_android AS (
    SELECT 
        date,
        Agency_PMD__af_prt_	AS agency,
        Media_Source__pid_ AS media_source,
        Campaign__c_ AS campaign,
        'retargeting' as report_type,
        'android' AS platform,
        clicks,
        conversions,
        sessions,
        total_revenue,
        total_cost,
        af_purchase__Event_counter_ AS purchase,
        af_purchase__Sales_in_RUB_ AS revenue,
        af_purchase_rw__Event_counter_ AS rw_purchase,
        af_purchase_rw__Sales_in_RUB_ AS rw_revenue,
        first_purchase__Event_counter_ AS first_purchase,
        first_purchase__Sales_in_RUB_ AS first_purchase_revenue,
        first_purchase_rw__Event_counter_ AS rw_first_purchase,
        first_purchase_rw__Sales_in_RUB_ AS rw_first_purchase_revenue,
        ROW_NUMBER() OVER(PARTITION BY date, Media_Source__pid_ , Campaign__c_ ORDER BY _TABLE_SUFFIX DESC) AS counter
    FROM  {{ source('AF_data', 'android_rtg_partners_by_date_report_*') }}
),

final AS (
    SELECT * FROM source_ios
    UNION ALL
    SELECT * FROM source_android
)

SELECT 
    DATE(date) AS date,
    agency,
    media_source,
    {{ normalize('campaign') }} AS campaign_name,
    report_type,
    platform,
    SUM(clicks) clicks,
    SUM(conversions) conversions,
    SUM(sessions) sessions,
    SUM(total_revenue) total_revenue,
    SUM(total_cost) total_cost,
    SUM(purchase) purchase,
    SUM(revenue) revenue,
    SUM(rw_purchase) rw_purchase,
    SUM(rw_revenue) rw_revenue,
    SUM(first_purchase) first_purchase,
    SUM(first_purchase_revenue) first_purchase_revenue,
    SUM(rw_first_purchase) rw_first_purchase,
    SUM(rw_first_purchase_revenue) rw_first_purchase_revenue,
FROM final
WHERE counter = 1
GROUP BY 1,2,3,4,5,6