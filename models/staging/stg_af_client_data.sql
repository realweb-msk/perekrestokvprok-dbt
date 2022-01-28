SELECT 
    date,
    is_retargeting,
    af_c_id AS af_cid,
    af_adset AS adset_name,
    mediasource,
    platform,
    eventname AS event_name,
    unig_event AS uniq_event_count,
    EventRevenue AS event_revenue,
    cnt_event AS event_count,
    campaign AS campaign_name
FROM  {{ source ('agg_data', 'AF_client_data')}}

