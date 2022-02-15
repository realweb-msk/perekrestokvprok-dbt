{% macro promo_search(campaign_name, adset_name='"-"', ad_name='"-"') %}

{%- set promos = get_promo() -%}

    CASE
{%- for promo in promos %}
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([{{campaign_name}}, {{adset_name}}, {{ad_name}}], ' ')), r'{{promo}}') THEN '{{promo}}'
{% endfor %}
    ELSE '-' END

{% endmacro %}