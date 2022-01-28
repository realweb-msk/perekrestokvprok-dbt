SELECT
    start_date,
    end_date,
    partner,
    platform,
    rate_for_parthner AS rate_for_partner,
    plan_f_p,
    rate_for_us,
    type
FROM {{ source('sheets_data','rate_info') }}