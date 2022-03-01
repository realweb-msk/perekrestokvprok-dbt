select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from `perekrestokvprok-bq`.`dbt_rsultanov_dbt_test__audit`.`freshness_stg_google_dbm_date__2`
    
      
    ) dbt_internal_test