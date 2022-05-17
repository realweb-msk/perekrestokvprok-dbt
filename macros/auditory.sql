{% macro aud(campaign_name, adset_name='"-"') %}
    CASE
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([{{campaign_name}}, {{adset_name}}], ' ')), 'minusfrequency2')
        THEN 'All buyers minus frequency 2'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([{{campaign_name}}, {{adset_name}}], ' ')), 'aud.cart')
        THEN 'Add to cart not buy'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([{{campaign_name}}, {{adset_name}}], ' ')), 'core_promo_5')
        THEN 'Ядро_Промо+5_fix'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([{{campaign_name}}, {{adset_name}}], ' ')), 'reg_not_buy')
        THEN 'Registered but not buy'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([{{campaign_name}}, {{adset_name}}], ' ')), 'purchase15_22')
        THEN 'Предотток'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([{{campaign_name}}, {{adset_name}}], ' ')), 'purchase23_73')
        THEN 'Отток'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([{{campaign_name}}, {{adset_name}}], ' ')), 'purchase74_130')
        OR REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([{{campaign_name}}, {{adset_name}}], ' ')), 'purchase_aft130')
        THEN 'Глубокий отток'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([{{campaign_name}}, {{adset_name}}], ' ')), 'notused_30d')
        THEN 'Не использовали приложение более 30 дней'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([{{campaign_name}}, {{adset_name}}], ' ')), 'purch_less_2')
        THEN 'Покупают реже, чем 2 раза в месяц'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([{{campaign_name}}, {{adset_name}}], ' ')), 'purch_from_2')
        THEN 'Покупают от 2 раз в месяц'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([{{campaign_name}}, {{adset_name}}], ' ')), 'deep_outflow_rtg')
        THEN 'Deep_outflow_rtg'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([{{campaign_name}}, {{adset_name}}], ' ')), 'cpo_outflow_rtg')
        THEN 'outflow'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([{{campaign_name}}, {{adset_name}}], ' ')), 'cpo_preflow_rtg')
        THEN 'preflow'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([{{campaign_name}}, {{adset_name}}], ' ')), 'cpo_installed_the_app_but_not_buy_')
        THEN 'Installed_the_app_but_not_buy'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([{{campaign_name}}, {{adset_name}}], ' ')), 'first_open_.ot_buy_rtg')
        THEN 'First_open_not_buy_rtg'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([{{campaign_name}}, {{adset_name}}], ' ')), 'installed_the_app_but_not_buy_rtg')
        THEN 'Installed_the_app_but_not_buy_rtg'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([{{campaign_name}}, {{adset_name}}], ' ')), 'registered_but_not_buy')
        THEN 'Registered_but_not_buy'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([{{campaign_name}}, {{adset_name}}], ' ')), 'firebase')
        THEN  "Предиктивная аудитория" 
        WHEN REGEXP_CONTAINS(LOWER({{campaign_name}}), 'inapp')
        THEN 'Аудитории площадки (inapp)'
        ELSE 'Нет аудитории' END
{% endmacro %}