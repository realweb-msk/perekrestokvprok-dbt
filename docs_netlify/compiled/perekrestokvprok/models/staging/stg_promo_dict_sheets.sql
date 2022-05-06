


SELECT DISTINCT
    LOWER(promo) AS promo,
    name
FROM `perekrestokvprok-bq`.`sheets_data`.`promo_dict_sheets`
WHERE promo IS NOT NULL