{% macro get_promo() %}

{{ return(get_column_values('promo', ref('stg_promo_dict_sheets'))) }}

{% endmacro %}