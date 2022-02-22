SELECT
    atr_date AS date,
    is_retargeting,
    LOWER(Campaign) AS campaign_name,
    MediaSource AS media_source,
    platform,
    EventName AS event_name,
    unig_event AS uniq_event,
    EventRevenue AS revenue,
    cnt_event AS event_count
FROM `perekrestokvprok-bq`.`AF_data`.`af_atr_data_for_media_ads_TABLE`