SELECT
    promo,
    name
FROM {{ source('sheets_data', 'promo_dict_sheets') }}