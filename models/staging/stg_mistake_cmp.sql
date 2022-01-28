SELECT
    mistake,
    correct
FROM {{ source('sheets_data', 'mistake_cmp') }}