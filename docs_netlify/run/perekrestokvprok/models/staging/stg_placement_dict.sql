
  
    

    create or replace table `perekrestokvprok-bq`.`dbt_production`.`stg_placement_dict`
    
    
    OPTIONS()
    as (
      






SELECT
    PlacementID AS placement_id,
    Placement,
    InsertionOrderID AS insertion_order_id
FROM `perekrestokvprok-bq`.`DCM`.`Placemant_dict`
WHERE PlacementID IS NOT NULL
    );
  