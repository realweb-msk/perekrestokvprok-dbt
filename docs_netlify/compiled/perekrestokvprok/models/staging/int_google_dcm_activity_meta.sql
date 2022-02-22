WITH source AS (
    SELECT
        interactionTime,
        placementId,
        placement,
        activityGroup,
        conversionId
    FROM `perekrestokvprok-bq`.`DCM`.`google_dcm_activitiIO`
    WHERE placement != ''
),

final AS (
    SELECT 
      DATE(interactionTime) interaction_date,
      placementId AS placement_id,
      placement,
      COUNT(distinct (CASE WHEN activityGroup = 'vpr1pur' THEN conversionId END)) purchase,
      COUNT(distinct (CASE WHEN activityGroup = 'vpr1ret' THEN conversionId END)) retarget,
      COUNT(distinct (CASE WHEN activityGroup = 'vpr1inst' THEN conversionId END)) installs,
  FROM source
  GROUP BY 1, 2, 3
)

SELECT
    interaction_date,
    placement_id,
    placement,
    purchase,
    retarget,
    installs
FROM final