{% macro conversion_source_type(campaign_name, source) %}
    CASE 
        WHEN REGEXP_CONTAINS({{campaign_name}}, r'[\[_]cpi[\]_]') OR {{source}} = 'Apple Search Ads' THEN 'CPI'
        WHEN REGEXP_CONTAINS({{campaign_name}}, r'[\[_]cpa[\]_]') OR REGEXP_CONTAINS({{campaign_name}}, r'^realwebcpa') THEN 'CPA'
        WHEN REGEXP_CONTAINS({{campaign_name}}, r'[\[_]cpc[\]_]') THEN 'CPC'
        WHEN REGEXP_CONTAINS({{campaign_name}}, r'[\[_]cpm[\]_]') THEN 'CPM'
        WHEN REGEXP_CONTAINS({{campaign_name}}, r'[\[_]cpo[\]_]') THEN 'CPO'
    ELSE 'Не определено' END
{% endmacro %}