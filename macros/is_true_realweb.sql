{% macro is_true_realweb(campaign_name, media_source) %}
    CASE
        WHEN REGEXP_CONTAINS(LOWER(campaign_name), r'realweb_|^ohm|\(exact\)|зоо') 
        OR media_source = 'Apple Search Ads' THEN 1
        ELSE 0 END
{% endmacro %}