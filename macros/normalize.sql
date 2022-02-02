{% macro normalize(campaign_name) %}
    TRIM(
        REGEXP_REPLACE(
            LOWER(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE({{campaign_name}},  r'(_install_week.*)', '_install_weekend'), 
                    r'(_install_promo_gl.*)', '_install_promo_global'), 
                r'(_install_promo_re.*)', '_install_promo_regular')
            ), r'\+|-', '_')
        )
{% endmacro %}