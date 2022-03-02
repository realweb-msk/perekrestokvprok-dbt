WITH source AS (
    SELECT 
        date,
        Agency_PMD__af_prt_	AS agency,
        Media_Source__pid_ AS media_source,
        Campaign__c_ AS campaign,
        IF(REGEXP_CONTAINS(Campaign__c_, r'realweb_'), 1, 0) AS is_true_realweb,
        'UA' as report_type,
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
    FROM  {{ source('AF_data', 'rtg_partners_by_date_report_*') }}
)

SELECT 
    date,
    agency,
    media_source,
    campaign,
    is_true_realweb,
    clicks,
    conversions,
    sessions,
    total_revenue,
    total_cost,
    purchase,
    revenue,
    rw_purchase,
    rw_revenue,
    first_purchase,
    first_purchase_revenue,
    rw_first_purchase,
    rw_first_purchase_revenue,
FROM source
WHERE counter = 1