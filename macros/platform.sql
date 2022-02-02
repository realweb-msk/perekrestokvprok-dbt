{% macro platform(campaign_name) %}
    CASE
        WHEN REGEXP_CONTAINS(LOWER({{campaign_name}}), r'\[p:ios\]|_ios_|p02') THEN 'ios'
        WHEN REGEXP_CONTAINS(LOWER({{campaign_name}}), r'\[p:and\]|_and_|android|p01') THEN 'android'
    ELSE 'no_platform' END
{% endmacro %}