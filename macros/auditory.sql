{% macro aud(campaign_name, source, platform) %}
    CASE
        WHEN REGEXP_CONTAINS(LOWER({{campaign_name}}), 'minusfrequency2')
        AND {{platform}} = 'ios'
        THEN 'All buyers minus frequency 2 ios'
        WHEN REGEXP_CONTAINS(LOWER({{campaign_name}}), 'minusfrequency2')
        AND {{platform}} = 'android'
        THEN 'All buyers minus frequency 2 android'
        WHEN REGEXP_CONTAINS(LOWER({{campaign_name}}), 'aud.cart')
        AND {{platform}} = 'android'
        THEN 'Add to cart not buy android'
        WHEN REGEXP_CONTAINS(LOWER({{campaign_name}}), 'aud.cart')
        AND {{platform}} = 'ios'
        THEN 'Add to cart not buy ios'
        WHEN REGEXP_CONTAINS(LOWER({{campaign_name}}), 'core_promo_5')
        THEN 'Ядро_Промо+5_fix'
        WHEN REGEXP_CONTAINS(LOWER({{campaign_name}}), 'reg_not_buy')
        THEN 'Registered but not buy'
        WHEN REGEXP_CONTAINS(LOWER({{campaign_name}}), 'purchase15_22')
        THEN 'Предотток'
        WHEN REGEXP_CONTAINS(LOWER({{campaign_name}}), 'purchase23_73')
        THEN 'Отток'
        WHEN REGEXP_CONTAINS(LOWER({{campaign_name}}), 'purchase74_130')
        OR REGEXP_CONTAINS(LOWER({{campaign_name}}), 'purchase_aft130')
        THEN 'Глубокий отток'
        WHEN REGEXP_CONTAINS(LOWER({{campaign_name}}), 'notused_30d')
        THEN 'Не использовали приложение более 30 дней'
        WHEN REGEXP_CONTAINS(LOWER({{campaign_name}}), 'purch_less_2')
        THEN 'Покупают реже, чем 2 раза в месяц'
        WHEN REGEXP_CONTAINS(LOWER({{campaign_name}}), 'purch_from_2')
        THEN 'Покупают от 2 раз в месяц'
        WHEN REGEXP_CONTAINS(LOWER({{campaign_name}}), 'deep_outflow_rtg')
        THEN 'Deep_outflow_rtg'
        WHEN REGEXP_CONTAINS(LOWER({{campaign_name}}), 'first_open_.ot_buy_rtg')
        THEN 'First_open_not_buy_rtg'
        WHEN REGEXP_CONTAINS(LOWER({{campaign_name}}), 'installed_the_app_but_not_buy_rtg')
        THEN 'Installed_the_app_but_not_buy_rtg'
        WHEN REGEXP_CONTAINS(LOWER({{campaign_name}}), 'registered_but_not_buy_rtg')
        THEN 'Registered_but_not_buy_rtg'
        WHEN REGEXP_CONTAINS(LOWER({{campaign_name}}), 'firebase')
        THEN  "Предиктивная аудитория" 
        WHEN {{source}} = 'inapp'
        THEN 'Аудитории площадки (inapp)'
        ELSE 'Нет аудитории' END
{% endmacro %}