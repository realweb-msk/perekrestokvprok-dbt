{% macro promo_type(campaign_name, adset_name='-') %}
    CASE
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([{{campaign_name}}, {{adset_name}}],'')), r'promo.*regular') THEN 'promo regular'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([{{campaign_name}}, {{adset_name}}],'')), r'promo.*global') THEN 'promo global'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([{{campaign_name}}, {{adset_name}}],'')), r'promo.*feed') THEN 'promo feed'
    ELSE '-' END
{% endmacro %}