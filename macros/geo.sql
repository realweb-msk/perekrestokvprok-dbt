{% macro geo(campaign_name, adset_name='"-"') %}
    CASE
          WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([{{campaign_name}}, {{adset_name}}], ' ')), r'msk+spb|mskspb|msk_spb') THEN 'МСК, СПб'
          WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([{{campaign_name}}, {{adset_name}}], ' ')), r'spb') THEN 'СПб'
          WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([{{campaign_name}}, {{adset_name}}], ' ')), r'msk') THEN 'МСК'
          WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([{{campaign_name}}, {{adset_name}}], ' ')), r'_nn_') THEN 'НН'
          WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([{{campaign_name}}, {{adset_name}}], ' ')), r'reg1') THEN 'Регионы с доставкой'
          WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([{{campaign_name}}, {{adset_name}}], ' ')), r'reg2|rostov|kzn|g_all')THEN 'Регионы без доставки'
        ELSE 'Россия' END
{% endmacro %}