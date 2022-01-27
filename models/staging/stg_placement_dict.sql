SELECT
    PlacementID,
    Placement,
    InsertionOrderID
FROM {{ source('DCM', 'Placemant_dict') }}