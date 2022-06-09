{% macro partner(campaign_name) %}
    CASE 
        WHEN REGEXP_CONTAINS({{campaign_name}}, r'_ms_') THEN 'Mobisharks'
        WHEN REGEXP_CONTAINS({{campaign_name}}, r'_tl_') THEN '2leads'
        WHEN REGEXP_CONTAINS({{campaign_name}}, r'_mx_') THEN 'MobX'
        WHEN REGEXP_CONTAINS({{campaign_name}}, r'_sw_') THEN 'SW'
        WHEN REGEXP_CONTAINS({{campaign_name}}, r'_tm_') THEN 'Think Mobile'
        WHEN REGEXP_CONTAINS({{campaign_name}}, r'_abc[_\s]|_sf_') THEN 'Mediasurfer'
    ELSE '-' END
{% endmacro %}