WITH source AS (
    SELECT *,
    ROW_NUMBER() 
        OVER (PARTITION BY InsertionOrderID,LineItemID, Date ORDER BY DATE(_PARTITIONTIME) DESC) AS pd_rw
    FROM `perekrestokvprok-bq`.`test2`.`google_dbm_existed_dbm_report_123`
)

SELECT 
    Date,
    Advertiser,
    AdvertiserID AS advertiser_id,
    AdvertiserCurrency AS advertiser_currency,
    InsertionOrder AS insertion_order,
    InsertionOrderID AS insertion_order_id,
    LineItem AS line_item,
    LineItemID AS line_item_id,
    LineItemType AS line_item_type,
    SAFE_CAST(Impressions AS INT64) AS impressions,
    SAFE_CAST(Clicks AS INT64) AS clicks,
    SAFE_CAST(ClickRateCTR AS FLOAT64) AS click_rate_ctr,
    SAFE_CAST(RevenueAdvCurrency AS FLOAT64) AS revenue_adv_currency,
    SAFE_CAST(ProfitAdvertiserCurrency AS FLOAT64) AS profit_advertiser_currency,
    SAFE_CAST(ProfitMargin AS FLOAT64) AS profit_margin
FROM source
WHERE pd_rw = 1