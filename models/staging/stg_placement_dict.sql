{% if target.name == 'prod' %}

{{
  config(
    materialized='table',
  )
}}

{% endif %}


SELECT
    PlacementID AS placement_id,
    Placement,
    InsertionOrderID AS insertion_order_id
FROM {{ source('DCM', 'Placemant_dict') }}
WHERE PlacementID IS NOT NULL