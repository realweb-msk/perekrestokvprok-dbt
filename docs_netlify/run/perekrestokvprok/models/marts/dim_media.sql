

  create or replace view `perekrestokvprok-bq`.`dbt_krepin`.`dim_media`
  OPTIONS()
  as WITH clicks_table AS (
    SELECT
        date,
        insertion_order,
        insertion_order_id,
        impressions,
        clicks,
        revenue_adv_currency,
        profit_advertiser_currency
    FROM `perekrestokvprok-bq`.`dbt_krepin`.`int_google_dbm_impressions_clicks_revenue_meta`
),

af AS (
    SELECT
        date,
        campaign_name,
        media_source,
        SUM(IF(event_name = 'af_purchase' AND is_retargeting = FALSE, event_count, 0)) AS af_ua_purchase,
        SUM(IF(event_name = 'af_purchase' AND is_retargeting = TRUE, event_count, 0)) AS af_rtg_purchase,
        SUM(IF(event_name = 'install', event_count, 0)) AS af_install,
        SUM(IF(event_name IN ('re-attribution','re-engagement'), event_count, 0)) AS af_re_engagement
    FROM `perekrestokvprok-bq`.`dbt_krepin`.`stg_af_for_media`
    GROUP BY 1,2,3
),

placement_dict AS (
        SELECT
        placement_id,
        placement,
        insertion_order_id
    FROM `perekrestokvprok-bq`.`dbt_krepin`.`stg_placement_dict`
),

activity AS (
    SELECT
        interaction_date,
        placement_id,
        placement,
        purchase,
        retarget,
        installs
    FROM `perekrestokvprok-bq`.`dbt_krepin`.`int_google_dcm_activity_meta`
),

reach AS (
    SELECT
        date,
        insertion_order,
        insertion_order_id,
        impression_reach
    FROM  `perekrestokvprok-bq`.`dbt_krepin`.`int_google_dcm_reach`
),

final AS (
    SELECT
        clicks_table.date,
        clicks_table.insertion_order,
        clicks_table.insertion_order_id,
        clicks_table.impressions,
        clicks_table.clicks,
        reach.impression_reach,
        clicks_table.revenue_adv_currency,
        clicks_table.profit_advertiser_currency,
        SUM(activity.purchase) AS purchase,
        SUM(activity.retarget) AS retarget,
        SUM(activity.installs) AS installs,
        SUM(af.af_ua_purchase) AS af_ua_purchase,
        SUM(af.af_rtg_purchase) AS af_rtg_purchase,
        SUM(af.af_install) AS af_install,
        SUM(af.af_re_engagement) AS af_re_engagement
    FROM clicks_table
    LEFT JOIN reach ON reach.date = clicks_table.date 
        AND reach.insertion_order_id = clicks_table.insertion_order_id
    LEFT JOIN placement_dict ON placement_dict.insertion_order_id = clicks_table.insertion_order_id
    LEFT JOIN activity ON activity.interaction_date = clicks_table.date
        AND placement_dict.placement_id = activity.placement_id
    LEFT JOIN af ON af.date = clicks_table.date AND placement_dict.placement_id = af.campaign_name
    GROUP BY 1,2,3,4,5,6,7,8
    ORDER BY 1,2
)

SELECT
    date,
    insertion_order,
    insertion_order_id,
    impressions,
    clicks,
    impression_reach,
    revenue_adv_currency,
    profit_advertiser_currency,
    purchase,
    retarget,
    installs,
    af_ua_purchase,
    af_rtg_purchase,
    af_install,
    af_re_engagement
FROM final;

