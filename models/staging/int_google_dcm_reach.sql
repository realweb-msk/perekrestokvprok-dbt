WITH source AS (
    SELECT
        Date,
        InsertionOrder,
        InsertionOrderID,
        UniqueReach_ImpressionReach,
        ROW_NUMBER() OVER (
            PARTITION BY InsertionOrderID,LineItemID, Date 
            ORDER BY DATE(_PARTITIONTIME) DESC
            ) AS pd_rw
    FROM {{ source('DCM_impression_reach', 'google_dbm_existed_dbm_report_293044') }}
),

final AS (
    SELECT 
        date,
        InsertionOrder AS insertion_order,
        InsertionOrderID AS insertion_order_id,
        SUM(
            SAFE_CAST(
                (IF(UniqueReach_ImpressionReach='-','0',UniqueReach_ImpressionReach)) AS INT64)
            ) AS impression_reach,
  FROM source
  GROUP BY 1, 2, 3
)

SELECT
    date,
    insertion_order,
    insertion_order_id,
    impression_reach
FROM final