/* 
для лучшего понимания лучше заглянуть сюда: https://github.com/realweb-msk/perekrestokvprok-dbt
или сюда: https://brave-hermann-395dc3.netlify.app/#!/model/model.perekrestokvprok.dim_ret
*/

WITH af_conversions AS (
    SELECT
        date,
        is_retargeting,
        --af_cid,
        --adset_name,
        
    CASE
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name],'')), r'promo.*regular') THEN 'promo regular'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name],'')), r'promo.*global') THEN 'promo global'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name],'')), r'promo.*feed') THEN 'promo feed'
    ELSE '-' END
 as promo_type,
        CASE
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'yagoda_14.09-28.09') THEN 'yagoda_14.09-28.09'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'tovar_rub_13.09-19.09') THEN 'tovar_rub_13.09-19.09'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'season_orange') THEN 'season_orange'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'season_mandarin') THEN 'season_mandarin'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'season_lemon') THEN 'season_lemon'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_ya_2022_ios_cpi_reg1_brand_display_promo_global') THEN 'realweb_ya_2022_ios_cpi_reg1_brand_display_promo_global'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_ya_2022_ios_cpi_nn_brand_display_promo_global') THEN 'realweb_ya_2022_ios_cpi_nn_brand_display_promo_global'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_ya_2022_ios_cpi_mskspb_brand_display_promo_global') THEN 'realweb_ya_2022_ios_cpi_mskspb_brand_display_promo_global'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_ya_2022_and_cpi_reg1_brand_display_promo_global') THEN 'realweb_ya_2022_and_cpi_reg1_brand_display_promo_global'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_ya_2022_and_cpi_nn_brand_display_promo_global') THEN 'realweb_ya_2022_and_cpi_nn_brand_display_promo_global'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_ya_2022_and_cpi_mskspb_brand_display_promo_global') THEN 'realweb_ya_2022_and_cpi_mskspb_brand_display_promo_global'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_ya_2021_and_cpi_rf_general_display_install_promo_regular') THEN 'realweb_ya_2021_and_cpi_rf_general_display_install_promo_regular'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_vk_2022_and_cpo_rf_old_ret_promo_global_cyber_monday') THEN 'realweb_vk_2022_and_cpo_rf_old_ret_promo_global_cyber_monday'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_tiktok_2022_mskspb_and_new_promo_global') THEN 'realweb_tiktok_2022_mskspb_and_new_promo_global'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_mt_2021 [p:and] [cpa] [mskspb] [old] [minusfrequency2] [promo_global]') THEN 'realweb_mt_2021 [p:and] [cpa] [mskspb] [old] [minusfrequency2] [promo_global]'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_fb_2021 [p:ios] [g:msk] [cpo] [feed] [old] [general] [darkstore] [ng]') THEN 'realweb_fb_2021 [p:ios] [g:msk] [cpo] [feed] [old] [general] [darkstore] [ng]'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_fb_2021 [p:ios] [g:msk] [cpo] [feed] [new] [general] [darkstore] [ng]') THEN 'realweb_fb_2021 [p:ios] [g:msk] [cpo] [feed] [new] [general] [darkstore] [ng]'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_fb_2021 [p:ios] [cpo] [feed] [discount] [old] [general] [darkstore] [forder] [i14]') THEN 'realweb_fb_2021 [p:ios] [cpo] [feed] [discount] [old] [general] [darkstore] [forder] [i14]'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_fb_2021 [p:ios] [cpi] [feed] [discount] [new] [general] [darkstore] [install] [i14]') THEN 'realweb_fb_2021 [p:ios] [cpi] [feed] [discount] [new] [general] [darkstore] [install] [i14]'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_fb_2021 [p:ios] [cpa] [feed] [discount] [new] [general] [darkstore] [forder] [i14]') THEN 'realweb_fb_2021 [p:ios] [cpa] [feed] [discount] [new] [general] [darkstore] [forder] [i14]'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_fb_2021 [p:and] [g:msk] [cpo] [feed] [old] [general] [darkstore] [ng]') THEN 'realweb_fb_2021 [p:and] [g:msk] [cpo] [feed] [old] [general] [darkstore] [ng]'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_fb_2021 [p:and] [g:msk] [cpo] [feed] [new] [general] [darkstore] [ng]') THEN 'realweb_fb_2021 [p:and] [g:msk] [cpo] [feed] [new] [general] [darkstore] [ng]'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_fb_2021 [p:and] [cpo_catalog] [feed] [discount] [old] [general] [darkstore] [forder]') THEN 'realweb_fb_2021 [p:and] [cpo_catalog] [feed] [discount] [old] [general] [darkstore] [forder]'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_fb_2021 [p:and] [cpo] [feed] [discount] [old] [general] [darkstore] [forder]') THEN 'realweb_fb_2021 [p:and] [cpo] [feed] [discount] [old] [general] [darkstore] [forder]'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_fb_2021 [p:and] [cpo] [feed] [discount] [new] [general] [darkstore] [install]') THEN 'realweb_fb_2021 [p:and] [cpo] [feed] [discount] [new] [general] [darkstore] [install]'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_fb_2021 [p:and] [cpi] [feed] [discount] [new] [general] [darkstore] [install]') THEN 'realweb_fb_2021 [p:and] [cpi] [feed] [discount] [new] [general] [darkstore] [install]'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promofeed') THEN 'promofeed'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_zavtrak') THEN 'promo_zavtrak'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_watermelon') THEN 'promo_watermelon'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_vsepo99') THEN 'promo_vsepo99'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_vse_po_49_i_99') THEN 'promo_vse_po_49_i_99'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_tea_coffee') THEN 'promo_tea_coffee'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_tea_and_cofee') THEN 'promo_tea_and_cofee'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_sun') THEN 'promo_sun'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_strawberry') THEN 'promo_strawberry'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_smoothies') THEN 'promo_smoothies'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_school') THEN 'promo_school'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_regular_present_vid') THEN 'promo_regular_present_vid'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_regular_present_2') THEN 'promo_regular_present_2'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_regular_present') THEN 'promo_regular_present'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_plov') THEN 'promo_plov'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_okroshka') THEN 'promo_okroshka'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_nemoloko') THEN 'promo_nemoloko'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_napitok') THEN 'promo_napitok'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_more_lososya') THEN 'promo_more_lososya'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_moloko_parmalat') THEN 'promo_moloko_parmalat'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_meet') THEN 'promo_meet'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_meat_21.07 - 02.08') THEN 'promo_meat_21.07 - 02.08'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_lisichki') THEN 'promo_lisichki'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_linzy') THEN 'promo_linzy'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_limonad') THEN 'promo_limonad'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_lego_december') THEN 'promo_lego_december'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_lego') THEN 'promo_lego'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_kvas') THEN 'promo_kvas'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_kolbasa') THEN 'promo_kolbasa'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_kasha') THEN 'promo_kasha'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_karamel_tokio') THEN 'promo_karamel_tokio'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_icecream') THEN 'promo_icecream'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_ice_cream') THEN 'promo_ice_cream'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_him') THEN 'promo_him'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_grill') THEN 'promo_grill'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_global_dyson_ret') THEN 'promo_global_dyson_ret'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_global_cyber_monday_zewa') THEN 'promo_global_cyber_monday_zewa'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_global_cyber_monday_vid2') THEN 'promo_global_cyber_monday_vid2'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_global_cyber_monday_vid1') THEN 'promo_global_cyber_monday_vid1'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_global_cyber_monday_pepsi') THEN 'promo_global_cyber_monday_pepsi'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_global_cyber_monday_losos') THEN 'promo_global_cyber_monday_losos'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_global_cyber_monday_drink') THEN 'promo_global_cyber_monday_drink'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_global_cyber_monday_25.01') THEN 'promo_global_cyber_monday_25.01'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_global_cyber_monday') THEN 'promo_global_cyber_monday'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_global_auto_carusel_2') THEN 'promo_global_auto_carusel_2'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_global_auto_carusel') THEN 'promo_global_auto_carusel'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_global_auto') THEN 'promo_global_auto'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_fruits_ berries') THEN 'promo_fruits_ berries'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_fish') THEN 'promo_fish'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_festmorozh') THEN 'promo_festmorozh'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_dostavka_dacha') THEN 'promo_dostavka_dacha'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_diapers') THEN 'promo_diapers'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_cosmetics') THEN 'promo_cosmetics'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_coffee') THEN 'promo_coffee'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_choco_ball') THEN 'promo_choco_ball'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_children') THEN 'promo_children'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_cherry') THEN 'promo_cherry'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_cheese_chechil') THEN 'promo_cheese_chechil'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_cheese') THEN 'promo_cheese'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_baton_alpin') THEN 'promo_baton_alpin'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_baton') THEN 'promo_baton'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_aurora') THEN 'promo_aurora'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_arbuz') THEN 'promo_arbuz'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'ng_olivier') THEN 'ng_olivier'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'ng_mandarin') THEN 'ng_mandarin'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'ng_kura') THEN 'ng_kura'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'ng_granata') THEN 'ng_granata'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'fixprice_17.09-27.09') THEN 'fixprice_17.09-27.09'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'express') THEN 'express'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'coffee_illy') THEN 'coffee_illy'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'black_friday') THEN 'black_friday'

    ELSE '-' END

 as promo_search,
        
    CASE
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'minusfrequency2')
        THEN 'All buyers minus frequency 2'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'aud.cart')
        THEN 'Add to cart not buy'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'core_promo_5')
        THEN 'Ядро_Промо+5_fix'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'reg_not_buy')
        THEN 'Registered but not buy'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'purchase15_22')
        THEN 'Предотток'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'purchase23_73')
        THEN 'Отток'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'purchase74_130')
        OR REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'purchase_aft130')
        THEN 'Глубокий отток'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'notused_30d')
        THEN 'Не использовали приложение более 30 дней'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'purch_less_2')
        THEN 'Покупают реже, чем 2 раза в месяц'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'purch_from_2')
        THEN 'Покупают от 2 раз в месяц'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'deep_outflow_rtg')
        THEN 'Deep_outflow_rtg'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'first_open_.ot_buy_rtg')
        THEN 'First_open_not_buy_rtg'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'installed_the_app_but_not_buy_rtg')
        THEN 'Installed_the_app_but_not_buy_rtg'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'registered_but_not_buy_rtg')
        THEN 'Registered_but_not_buy_rtg'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'firebase')
        THEN  "Предиктивная аудитория" 
        WHEN REGEXP_CONTAINS(LOWER(campaign_name), 'inapp')
        THEN 'Аудитории площадки (inapp)'
        ELSE 'Нет аудитории' END
 AS auditory,
        mediasource,
        platform,
        CASE
            WHEN event_name IN ('re-attribution','re-engagement')
            THEN 're-engagement' 
            WHEN event_name = "af_purchase"
            THEN "af_purchase"
            ELSE 'no' END AS event_name,
        uniq_event_count,
        event_revenue,
        event_count,
        campaign_name
    FROM  `perekrestokvprok-bq`.`dbt_production`.`stg_af_client_data`
    WHERE event_name IN('re-attribution','re-engagement',"af_purchase")
    -- WHERE is_retargeting = TRUE
    -- AND REGEXP_CONTAINS(campaign_name, 'realweb')
),

----------------------- facebook -------------------------

facebook AS (
    SELECT
        date,
        campaign_name,
        
    CASE
        WHEN REGEXP_CONTAINS(LOWER(campaign_name), r'\[p:ios\]|_ios_|p02') THEN 'ios'
        WHEN REGEXP_CONTAINS(LOWER(campaign_name), r'\[p:and\]|_and_|android|p01') THEN 'android'
    ELSE 'no_platform' END
 as platform,
        
    CASE
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name],'')), r'promo.*regular') THEN 'promo regular'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name],'')), r'promo.*global') THEN 'promo global'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name],'')), r'promo.*feed') THEN 'promo feed'
    ELSE '-' END
 as promo_type, 
        CASE
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, ad_name], ' ')), r'yagoda_14.09-28.09') THEN 'yagoda_14.09-28.09'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, ad_name], ' ')), r'tovar_rub_13.09-19.09') THEN 'tovar_rub_13.09-19.09'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, ad_name], ' ')), r'season_orange') THEN 'season_orange'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, ad_name], ' ')), r'season_mandarin') THEN 'season_mandarin'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, ad_name], ' ')), r'season_lemon') THEN 'season_lemon'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, ad_name], ' ')), r'realweb_ya_2022_ios_cpi_reg1_brand_display_promo_global') THEN 'realweb_ya_2022_ios_cpi_reg1_brand_display_promo_global'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, ad_name], ' ')), r'realweb_ya_2022_ios_cpi_nn_brand_display_promo_global') THEN 'realweb_ya_2022_ios_cpi_nn_brand_display_promo_global'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, ad_name], ' ')), r'realweb_ya_2022_ios_cpi_mskspb_brand_display_promo_global') THEN 'realweb_ya_2022_ios_cpi_mskspb_brand_display_promo_global'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, ad_name], ' ')), r'realweb_ya_2022_and_cpi_reg1_brand_display_promo_global') THEN 'realweb_ya_2022_and_cpi_reg1_brand_display_promo_global'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, ad_name], ' ')), r'realweb_ya_2022_and_cpi_nn_brand_display_promo_global') THEN 'realweb_ya_2022_and_cpi_nn_brand_display_promo_global'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, ad_name], ' ')), r'realweb_ya_2022_and_cpi_mskspb_brand_display_promo_global') THEN 'realweb_ya_2022_and_cpi_mskspb_brand_display_promo_global'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, ad_name], ' ')), r'realweb_ya_2021_and_cpi_rf_general_display_install_promo_regular') THEN 'realweb_ya_2021_and_cpi_rf_general_display_install_promo_regular'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, ad_name], ' ')), r'realweb_vk_2022_and_cpo_rf_old_ret_promo_global_cyber_monday') THEN 'realweb_vk_2022_and_cpo_rf_old_ret_promo_global_cyber_monday'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, ad_name], ' ')), r'realweb_tiktok_2022_mskspb_and_new_promo_global') THEN 'realweb_tiktok_2022_mskspb_and_new_promo_global'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, ad_name], ' ')), r'realweb_mt_2021 [p:and] [cpa] [mskspb] [old] [minusfrequency2] [promo_global]') THEN 'realweb_mt_2021 [p:and] [cpa] [mskspb] [old] [minusfrequency2] [promo_global]'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, ad_name], ' ')), r'realweb_fb_2021 [p:ios] [g:msk] [cpo] [feed] [old] [general] [darkstore] [ng]') THEN 'realweb_fb_2021 [p:ios] [g:msk] [cpo] [feed] [old] [general] [darkstore] [ng]'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, ad_name], ' ')), r'realweb_fb_2021 [p:ios] [g:msk] [cpo] [feed] [new] [general] [darkstore] [ng]') THEN 'realweb_fb_2021 [p:ios] [g:msk] [cpo] [feed] [new] [general] [darkstore] [ng]'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, ad_name], ' ')), r'realweb_fb_2021 [p:ios] [cpo] [feed] [discount] [old] [general] [darkstore] [forder] [i14]') THEN 'realweb_fb_2021 [p:ios] [cpo] [feed] [discount] [old] [general] [darkstore] [forder] [i14]'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, ad_name], ' ')), r'realweb_fb_2021 [p:ios] [cpi] [feed] [discount] [new] [general] [darkstore] [install] [i14]') THEN 'realweb_fb_2021 [p:ios] [cpi] [feed] [discount] [new] [general] [darkstore] [install] [i14]'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, ad_name], ' ')), r'realweb_fb_2021 [p:ios] [cpa] [feed] [discount] [new] [general] [darkstore] [forder] [i14]') THEN 'realweb_fb_2021 [p:ios] [cpa] [feed] [discount] [new] [general] [darkstore] [forder] [i14]'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, ad_name], ' ')), r'realweb_fb_2021 [p:and] [g:msk] [cpo] [feed] [old] [general] [darkstore] [ng]') THEN 'realweb_fb_2021 [p:and] [g:msk] [cpo] [feed] [old] [general] [darkstore] [ng]'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, ad_name], ' ')), r'realweb_fb_2021 [p:and] [g:msk] [cpo] [feed] [new] [general] [darkstore] [ng]') THEN 'realweb_fb_2021 [p:and] [g:msk] [cpo] [feed] [new] [general] [darkstore] [ng]'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, ad_name], ' ')), r'realweb_fb_2021 [p:and] [cpo_catalog] [feed] [discount] [old] [general] [darkstore] [forder]') THEN 'realweb_fb_2021 [p:and] [cpo_catalog] [feed] [discount] [old] [general] [darkstore] [forder]'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, ad_name], ' ')), r'realweb_fb_2021 [p:and] [cpo] [feed] [discount] [old] [general] [darkstore] [forder]') THEN 'realweb_fb_2021 [p:and] [cpo] [feed] [discount] [old] [general] [darkstore] [forder]'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, ad_name], ' ')), r'realweb_fb_2021 [p:and] [cpo] [feed] [discount] [new] [general] [darkstore] [install]') THEN 'realweb_fb_2021 [p:and] [cpo] [feed] [discount] [new] [general] [darkstore] [install]'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, ad_name], ' ')), r'realweb_fb_2021 [p:and] [cpi] [feed] [discount] [new] [general] [darkstore] [install]') THEN 'realweb_fb_2021 [p:and] [cpi] [feed] [discount] [new] [general] [darkstore] [install]'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, ad_name], ' ')), r'promofeed') THEN 'promofeed'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, ad_name], ' ')), r'promo_zavtrak') THEN 'promo_zavtrak'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, ad_name], ' ')), r'promo_watermelon') THEN 'promo_watermelon'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, ad_name], ' ')), r'promo_vsepo99') THEN 'promo_vsepo99'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, ad_name], ' ')), r'promo_vse_po_49_i_99') THEN 'promo_vse_po_49_i_99'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, ad_name], ' ')), r'promo_tea_coffee') THEN 'promo_tea_coffee'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, ad_name], ' ')), r'promo_tea_and_cofee') THEN 'promo_tea_and_cofee'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, ad_name], ' ')), r'promo_sun') THEN 'promo_sun'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, ad_name], ' ')), r'promo_strawberry') THEN 'promo_strawberry'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, ad_name], ' ')), r'promo_smoothies') THEN 'promo_smoothies'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, ad_name], ' ')), r'promo_school') THEN 'promo_school'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, ad_name], ' ')), r'promo_regular_present_vid') THEN 'promo_regular_present_vid'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, ad_name], ' ')), r'promo_regular_present_2') THEN 'promo_regular_present_2'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, ad_name], ' ')), r'promo_regular_present') THEN 'promo_regular_present'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, ad_name], ' ')), r'promo_plov') THEN 'promo_plov'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, ad_name], ' ')), r'promo_okroshka') THEN 'promo_okroshka'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, ad_name], ' ')), r'promo_nemoloko') THEN 'promo_nemoloko'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, ad_name], ' ')), r'promo_napitok') THEN 'promo_napitok'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, ad_name], ' ')), r'promo_more_lososya') THEN 'promo_more_lososya'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, ad_name], ' ')), r'promo_moloko_parmalat') THEN 'promo_moloko_parmalat'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, ad_name], ' ')), r'promo_meet') THEN 'promo_meet'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, ad_name], ' ')), r'promo_meat_21.07 - 02.08') THEN 'promo_meat_21.07 - 02.08'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, ad_name], ' ')), r'promo_lisichki') THEN 'promo_lisichki'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, ad_name], ' ')), r'promo_linzy') THEN 'promo_linzy'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, ad_name], ' ')), r'promo_limonad') THEN 'promo_limonad'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, ad_name], ' ')), r'promo_lego_december') THEN 'promo_lego_december'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, ad_name], ' ')), r'promo_lego') THEN 'promo_lego'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, ad_name], ' ')), r'promo_kvas') THEN 'promo_kvas'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, ad_name], ' ')), r'promo_kolbasa') THEN 'promo_kolbasa'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, ad_name], ' ')), r'promo_kasha') THEN 'promo_kasha'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, ad_name], ' ')), r'promo_karamel_tokio') THEN 'promo_karamel_tokio'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, ad_name], ' ')), r'promo_icecream') THEN 'promo_icecream'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, ad_name], ' ')), r'promo_ice_cream') THEN 'promo_ice_cream'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, ad_name], ' ')), r'promo_him') THEN 'promo_him'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, ad_name], ' ')), r'promo_grill') THEN 'promo_grill'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, ad_name], ' ')), r'promo_global_dyson_ret') THEN 'promo_global_dyson_ret'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, ad_name], ' ')), r'promo_global_cyber_monday_zewa') THEN 'promo_global_cyber_monday_zewa'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, ad_name], ' ')), r'promo_global_cyber_monday_vid2') THEN 'promo_global_cyber_monday_vid2'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, ad_name], ' ')), r'promo_global_cyber_monday_vid1') THEN 'promo_global_cyber_monday_vid1'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, ad_name], ' ')), r'promo_global_cyber_monday_pepsi') THEN 'promo_global_cyber_monday_pepsi'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, ad_name], ' ')), r'promo_global_cyber_monday_losos') THEN 'promo_global_cyber_monday_losos'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, ad_name], ' ')), r'promo_global_cyber_monday_drink') THEN 'promo_global_cyber_monday_drink'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, ad_name], ' ')), r'promo_global_cyber_monday_25.01') THEN 'promo_global_cyber_monday_25.01'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, ad_name], ' ')), r'promo_global_cyber_monday') THEN 'promo_global_cyber_monday'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, ad_name], ' ')), r'promo_global_auto_carusel_2') THEN 'promo_global_auto_carusel_2'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, ad_name], ' ')), r'promo_global_auto_carusel') THEN 'promo_global_auto_carusel'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, ad_name], ' ')), r'promo_global_auto') THEN 'promo_global_auto'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, ad_name], ' ')), r'promo_fruits_ berries') THEN 'promo_fruits_ berries'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, ad_name], ' ')), r'promo_fish') THEN 'promo_fish'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, ad_name], ' ')), r'promo_festmorozh') THEN 'promo_festmorozh'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, ad_name], ' ')), r'promo_dostavka_dacha') THEN 'promo_dostavka_dacha'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, ad_name], ' ')), r'promo_diapers') THEN 'promo_diapers'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, ad_name], ' ')), r'promo_cosmetics') THEN 'promo_cosmetics'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, ad_name], ' ')), r'promo_coffee') THEN 'promo_coffee'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, ad_name], ' ')), r'promo_choco_ball') THEN 'promo_choco_ball'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, ad_name], ' ')), r'promo_children') THEN 'promo_children'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, ad_name], ' ')), r'promo_cherry') THEN 'promo_cherry'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, ad_name], ' ')), r'promo_cheese_chechil') THEN 'promo_cheese_chechil'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, ad_name], ' ')), r'promo_cheese') THEN 'promo_cheese'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, ad_name], ' ')), r'promo_baton_alpin') THEN 'promo_baton_alpin'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, ad_name], ' ')), r'promo_baton') THEN 'promo_baton'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, ad_name], ' ')), r'promo_aurora') THEN 'promo_aurora'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, ad_name], ' ')), r'promo_arbuz') THEN 'promo_arbuz'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, ad_name], ' ')), r'ng_olivier') THEN 'ng_olivier'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, ad_name], ' ')), r'ng_mandarin') THEN 'ng_mandarin'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, ad_name], ' ')), r'ng_kura') THEN 'ng_kura'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, ad_name], ' ')), r'ng_granata') THEN 'ng_granata'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, ad_name], ' ')), r'fixprice_17.09-27.09') THEN 'fixprice_17.09-27.09'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, ad_name], ' ')), r'express') THEN 'express'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, ad_name], ' ')), r'coffee_illy') THEN 'coffee_illy'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, ad_name], ' ')), r'black_friday') THEN 'black_friday'

    ELSE '-' END

 as promo_search,
        
    CASE
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'minusfrequency2')
        THEN 'All buyers minus frequency 2'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'aud.cart')
        THEN 'Add to cart not buy'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'core_promo_5')
        THEN 'Ядро_Промо+5_fix'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'reg_not_buy')
        THEN 'Registered but not buy'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'purchase15_22')
        THEN 'Предотток'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'purchase23_73')
        THEN 'Отток'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'purchase74_130')
        OR REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'purchase_aft130')
        THEN 'Глубокий отток'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'notused_30d')
        THEN 'Не использовали приложение более 30 дней'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'purch_less_2')
        THEN 'Покупают реже, чем 2 раза в месяц'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'purch_from_2')
        THEN 'Покупают от 2 раз в месяц'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'deep_outflow_rtg')
        THEN 'Deep_outflow_rtg'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'first_open_.ot_buy_rtg')
        THEN 'First_open_not_buy_rtg'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'installed_the_app_but_not_buy_rtg')
        THEN 'Installed_the_app_but_not_buy_rtg'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'registered_but_not_buy_rtg')
        THEN 'Registered_but_not_buy_rtg'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'firebase')
        THEN  "Предиктивная аудитория" 
        WHEN REGEXP_CONTAINS(LOWER(campaign_name), 'inapp')
        THEN 'Аудитории площадки (inapp)'
        ELSE 'Нет аудитории' END
 AS auditory,
        SUM(installs) AS re_engagement,
        SUM(revenue) AS revenue,
        SUM(purchase) AS purchase,
        SUM(spend) AS spend,
        'Facebook' AS source
    FROM `perekrestokvprok-bq`.`dbt_production`.`stg_facebook_cab_sheets`
    --`perekrestokvprok-bq`.`dbt_production`.`stg_facebook_cab_meta`
    WHERE campaign_type = 'retargeting'
    GROUP BY 1,2,3,4,5,6
),

----------------------- yandex -------------------------

yandex_cost AS (
    SELECT
        date,
        campaign_name,
        
    CASE
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name],'')), r'promo.*regular') THEN 'promo regular'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name],'')), r'promo.*global') THEN 'promo global'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name],'')), r'promo.*feed') THEN 'promo feed'
    ELSE '-' END
 as promo_type, 
        
    CASE
        WHEN REGEXP_CONTAINS(LOWER(campaign_name), r'\[p:ios\]|_ios_|p02') THEN 'ios'
        WHEN REGEXP_CONTAINS(LOWER(campaign_name), r'\[p:and\]|_and_|android|p01') THEN 'android'
    ELSE 'no_platform' END
 as platform,
        CASE
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'yagoda_14.09-28.09') THEN 'yagoda_14.09-28.09'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'tovar_rub_13.09-19.09') THEN 'tovar_rub_13.09-19.09'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'season_orange') THEN 'season_orange'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'season_mandarin') THEN 'season_mandarin'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'season_lemon') THEN 'season_lemon'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_ya_2022_ios_cpi_reg1_brand_display_promo_global') THEN 'realweb_ya_2022_ios_cpi_reg1_brand_display_promo_global'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_ya_2022_ios_cpi_nn_brand_display_promo_global') THEN 'realweb_ya_2022_ios_cpi_nn_brand_display_promo_global'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_ya_2022_ios_cpi_mskspb_brand_display_promo_global') THEN 'realweb_ya_2022_ios_cpi_mskspb_brand_display_promo_global'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_ya_2022_and_cpi_reg1_brand_display_promo_global') THEN 'realweb_ya_2022_and_cpi_reg1_brand_display_promo_global'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_ya_2022_and_cpi_nn_brand_display_promo_global') THEN 'realweb_ya_2022_and_cpi_nn_brand_display_promo_global'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_ya_2022_and_cpi_mskspb_brand_display_promo_global') THEN 'realweb_ya_2022_and_cpi_mskspb_brand_display_promo_global'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_ya_2021_and_cpi_rf_general_display_install_promo_regular') THEN 'realweb_ya_2021_and_cpi_rf_general_display_install_promo_regular'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_vk_2022_and_cpo_rf_old_ret_promo_global_cyber_monday') THEN 'realweb_vk_2022_and_cpo_rf_old_ret_promo_global_cyber_monday'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_tiktok_2022_mskspb_and_new_promo_global') THEN 'realweb_tiktok_2022_mskspb_and_new_promo_global'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_mt_2021 [p:and] [cpa] [mskspb] [old] [minusfrequency2] [promo_global]') THEN 'realweb_mt_2021 [p:and] [cpa] [mskspb] [old] [minusfrequency2] [promo_global]'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_fb_2021 [p:ios] [g:msk] [cpo] [feed] [old] [general] [darkstore] [ng]') THEN 'realweb_fb_2021 [p:ios] [g:msk] [cpo] [feed] [old] [general] [darkstore] [ng]'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_fb_2021 [p:ios] [g:msk] [cpo] [feed] [new] [general] [darkstore] [ng]') THEN 'realweb_fb_2021 [p:ios] [g:msk] [cpo] [feed] [new] [general] [darkstore] [ng]'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_fb_2021 [p:ios] [cpo] [feed] [discount] [old] [general] [darkstore] [forder] [i14]') THEN 'realweb_fb_2021 [p:ios] [cpo] [feed] [discount] [old] [general] [darkstore] [forder] [i14]'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_fb_2021 [p:ios] [cpi] [feed] [discount] [new] [general] [darkstore] [install] [i14]') THEN 'realweb_fb_2021 [p:ios] [cpi] [feed] [discount] [new] [general] [darkstore] [install] [i14]'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_fb_2021 [p:ios] [cpa] [feed] [discount] [new] [general] [darkstore] [forder] [i14]') THEN 'realweb_fb_2021 [p:ios] [cpa] [feed] [discount] [new] [general] [darkstore] [forder] [i14]'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_fb_2021 [p:and] [g:msk] [cpo] [feed] [old] [general] [darkstore] [ng]') THEN 'realweb_fb_2021 [p:and] [g:msk] [cpo] [feed] [old] [general] [darkstore] [ng]'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_fb_2021 [p:and] [g:msk] [cpo] [feed] [new] [general] [darkstore] [ng]') THEN 'realweb_fb_2021 [p:and] [g:msk] [cpo] [feed] [new] [general] [darkstore] [ng]'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_fb_2021 [p:and] [cpo_catalog] [feed] [discount] [old] [general] [darkstore] [forder]') THEN 'realweb_fb_2021 [p:and] [cpo_catalog] [feed] [discount] [old] [general] [darkstore] [forder]'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_fb_2021 [p:and] [cpo] [feed] [discount] [old] [general] [darkstore] [forder]') THEN 'realweb_fb_2021 [p:and] [cpo] [feed] [discount] [old] [general] [darkstore] [forder]'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_fb_2021 [p:and] [cpo] [feed] [discount] [new] [general] [darkstore] [install]') THEN 'realweb_fb_2021 [p:and] [cpo] [feed] [discount] [new] [general] [darkstore] [install]'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_fb_2021 [p:and] [cpi] [feed] [discount] [new] [general] [darkstore] [install]') THEN 'realweb_fb_2021 [p:and] [cpi] [feed] [discount] [new] [general] [darkstore] [install]'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promofeed') THEN 'promofeed'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_zavtrak') THEN 'promo_zavtrak'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_watermelon') THEN 'promo_watermelon'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_vsepo99') THEN 'promo_vsepo99'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_vse_po_49_i_99') THEN 'promo_vse_po_49_i_99'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_tea_coffee') THEN 'promo_tea_coffee'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_tea_and_cofee') THEN 'promo_tea_and_cofee'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_sun') THEN 'promo_sun'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_strawberry') THEN 'promo_strawberry'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_smoothies') THEN 'promo_smoothies'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_school') THEN 'promo_school'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_regular_present_vid') THEN 'promo_regular_present_vid'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_regular_present_2') THEN 'promo_regular_present_2'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_regular_present') THEN 'promo_regular_present'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_plov') THEN 'promo_plov'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_okroshka') THEN 'promo_okroshka'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_nemoloko') THEN 'promo_nemoloko'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_napitok') THEN 'promo_napitok'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_more_lososya') THEN 'promo_more_lososya'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_moloko_parmalat') THEN 'promo_moloko_parmalat'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_meet') THEN 'promo_meet'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_meat_21.07 - 02.08') THEN 'promo_meat_21.07 - 02.08'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_lisichki') THEN 'promo_lisichki'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_linzy') THEN 'promo_linzy'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_limonad') THEN 'promo_limonad'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_lego_december') THEN 'promo_lego_december'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_lego') THEN 'promo_lego'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_kvas') THEN 'promo_kvas'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_kolbasa') THEN 'promo_kolbasa'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_kasha') THEN 'promo_kasha'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_karamel_tokio') THEN 'promo_karamel_tokio'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_icecream') THEN 'promo_icecream'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_ice_cream') THEN 'promo_ice_cream'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_him') THEN 'promo_him'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_grill') THEN 'promo_grill'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_global_dyson_ret') THEN 'promo_global_dyson_ret'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_global_cyber_monday_zewa') THEN 'promo_global_cyber_monday_zewa'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_global_cyber_monday_vid2') THEN 'promo_global_cyber_monday_vid2'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_global_cyber_monday_vid1') THEN 'promo_global_cyber_monday_vid1'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_global_cyber_monday_pepsi') THEN 'promo_global_cyber_monday_pepsi'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_global_cyber_monday_losos') THEN 'promo_global_cyber_monday_losos'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_global_cyber_monday_drink') THEN 'promo_global_cyber_monday_drink'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_global_cyber_monday_25.01') THEN 'promo_global_cyber_monday_25.01'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_global_cyber_monday') THEN 'promo_global_cyber_monday'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_global_auto_carusel_2') THEN 'promo_global_auto_carusel_2'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_global_auto_carusel') THEN 'promo_global_auto_carusel'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_global_auto') THEN 'promo_global_auto'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_fruits_ berries') THEN 'promo_fruits_ berries'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_fish') THEN 'promo_fish'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_festmorozh') THEN 'promo_festmorozh'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_dostavka_dacha') THEN 'promo_dostavka_dacha'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_diapers') THEN 'promo_diapers'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_cosmetics') THEN 'promo_cosmetics'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_coffee') THEN 'promo_coffee'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_choco_ball') THEN 'promo_choco_ball'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_children') THEN 'promo_children'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_cherry') THEN 'promo_cherry'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_cheese_chechil') THEN 'promo_cheese_chechil'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_cheese') THEN 'promo_cheese'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_baton_alpin') THEN 'promo_baton_alpin'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_baton') THEN 'promo_baton'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_aurora') THEN 'promo_aurora'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_arbuz') THEN 'promo_arbuz'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'ng_olivier') THEN 'ng_olivier'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'ng_mandarin') THEN 'ng_mandarin'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'ng_kura') THEN 'ng_kura'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'ng_granata') THEN 'ng_granata'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'fixprice_17.09-27.09') THEN 'fixprice_17.09-27.09'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'express') THEN 'express'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'coffee_illy') THEN 'coffee_illy'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'black_friday') THEN 'black_friday'

    ELSE '-' END

 as promo_search,
        
    CASE
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'minusfrequency2')
        THEN 'All buyers minus frequency 2'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'aud.cart')
        THEN 'Add to cart not buy'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'core_promo_5')
        THEN 'Ядро_Промо+5_fix'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'reg_not_buy')
        THEN 'Registered but not buy'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'purchase15_22')
        THEN 'Предотток'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'purchase23_73')
        THEN 'Отток'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'purchase74_130')
        OR REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'purchase_aft130')
        THEN 'Глубокий отток'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'notused_30d')
        THEN 'Не использовали приложение более 30 дней'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'purch_less_2')
        THEN 'Покупают реже, чем 2 раза в месяц'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'purch_from_2')
        THEN 'Покупают от 2 раз в месяц'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'deep_outflow_rtg')
        THEN 'Deep_outflow_rtg'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'first_open_.ot_buy_rtg')
        THEN 'First_open_not_buy_rtg'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'installed_the_app_but_not_buy_rtg')
        THEN 'Installed_the_app_but_not_buy_rtg'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'registered_but_not_buy_rtg')
        THEN 'Registered_but_not_buy_rtg'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'firebase')
        THEN  "Предиктивная аудитория" 
        WHEN REGEXP_CONTAINS(LOWER(campaign_name), 'inapp')
        THEN 'Аудитории площадки (inapp)'
        ELSE 'Нет аудитории' END
 AS auditory,
        -- SUM(impressions),
        -- SUM(clicks),
        SUM(spend) AS spend
    FROM `perekrestokvprok-bq`.`dbt_production`.`int_yandex_cab_meta`
    WHERE campaign_type = 'retargeting'
    AND REGEXP_CONTAINS(campaign_name, r'realweb')
    GROUP BY 1,2,3,4,5,6
),

yandex_convs AS (
    SELECT 
        date,
        campaign_name,
        platform,
        promo_type,
        promo_search,
        auditory,
        SUM(IF(event_name = "af_purchase", event_revenue, 0)) AS revenue,
        SUM(IF(event_name = "af_purchase", event_count, 0)) AS purchase,
        SUM(IF(event_name = 're-engagement', event_count, 0)) AS re_engagement
    FROM af_conversions
    WHERE mediasource ='yandexdirect_int'
    AND is_retargeting = TRUE
    AND REGEXP_CONTAINS(campaign_name, r'realweb')
    AND REGEXP_CONTAINS(campaign_name, r'_ret_|[_\[]old[_\]]')
    GROUP BY 1,2,3,4,5,6
),

yandex AS (
    SELECT
        COALESCE(yandex_convs.date, yandex_cost.date) AS date,
        COALESCE(yandex_convs.campaign_name, yandex_cost.campaign_name) AS campaign_name,
        COALESCE(yandex_convs.platform, yandex_cost.platform) AS platform,
        COALESCE(yandex_convs.promo_type, yandex_cost.promo_type) AS promo_type,
        COALESCE(yandex_convs.promo_search, yandex_cost.promo_search) AS promo_search,
        COALESCE(yandex_convs.auditory, yandex_cost.auditory) AS auditory,
        COALESCE(re_engagement,0) AS re_engagement,
        COALESCE(revenue,0) AS revenue,
        COALESCE(purchase,0) AS purchase,
        COALESCE(spend,0) AS spend,
        'Яндекс.Директ' AS source,
    FROM yandex_convs
    FULL OUTER JOIN yandex_cost
    ON yandex_convs.date = yandex_cost.date 
    AND yandex_convs.campaign_name = yandex_cost.campaign_name
    AND yandex_convs.promo_type = yandex_cost.promo_type
    AND yandex_convs.promo_search = yandex_cost.promo_search
    AND yandex_convs.auditory = yandex_cost.auditory
    WHERE COALESCE(re_engagement,0) + COALESCE(revenue,0) + COALESCE(purchase,0) + COALESCE(spend,0) > 0
    AND COALESCE(yandex_convs.campaign_name, yandex_cost.campaign_name) != 'None'
),

----------------------- vk -------------------------

vk_cost AS (
    SELECT
        date,
        campaign_name,
        
    CASE
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name],'')), r'promo.*regular') THEN 'promo regular'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name],'')), r'promo.*global') THEN 'promo global'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name],'')), r'promo.*feed') THEN 'promo feed'
    ELSE '-' END
 as promo_type,
        
    CASE
        WHEN REGEXP_CONTAINS(LOWER(campaign_name), r'\[p:ios\]|_ios_|p02') THEN 'ios'
        WHEN REGEXP_CONTAINS(LOWER(campaign_name), r'\[p:and\]|_and_|android|p01') THEN 'android'
    ELSE 'no_platform' END
 as platform,
        CASE
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'yagoda_14.09-28.09') THEN 'yagoda_14.09-28.09'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'tovar_rub_13.09-19.09') THEN 'tovar_rub_13.09-19.09'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'season_orange') THEN 'season_orange'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'season_mandarin') THEN 'season_mandarin'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'season_lemon') THEN 'season_lemon'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_ya_2022_ios_cpi_reg1_brand_display_promo_global') THEN 'realweb_ya_2022_ios_cpi_reg1_brand_display_promo_global'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_ya_2022_ios_cpi_nn_brand_display_promo_global') THEN 'realweb_ya_2022_ios_cpi_nn_brand_display_promo_global'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_ya_2022_ios_cpi_mskspb_brand_display_promo_global') THEN 'realweb_ya_2022_ios_cpi_mskspb_brand_display_promo_global'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_ya_2022_and_cpi_reg1_brand_display_promo_global') THEN 'realweb_ya_2022_and_cpi_reg1_brand_display_promo_global'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_ya_2022_and_cpi_nn_brand_display_promo_global') THEN 'realweb_ya_2022_and_cpi_nn_brand_display_promo_global'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_ya_2022_and_cpi_mskspb_brand_display_promo_global') THEN 'realweb_ya_2022_and_cpi_mskspb_brand_display_promo_global'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_ya_2021_and_cpi_rf_general_display_install_promo_regular') THEN 'realweb_ya_2021_and_cpi_rf_general_display_install_promo_regular'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_vk_2022_and_cpo_rf_old_ret_promo_global_cyber_monday') THEN 'realweb_vk_2022_and_cpo_rf_old_ret_promo_global_cyber_monday'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_tiktok_2022_mskspb_and_new_promo_global') THEN 'realweb_tiktok_2022_mskspb_and_new_promo_global'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_mt_2021 [p:and] [cpa] [mskspb] [old] [minusfrequency2] [promo_global]') THEN 'realweb_mt_2021 [p:and] [cpa] [mskspb] [old] [minusfrequency2] [promo_global]'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_fb_2021 [p:ios] [g:msk] [cpo] [feed] [old] [general] [darkstore] [ng]') THEN 'realweb_fb_2021 [p:ios] [g:msk] [cpo] [feed] [old] [general] [darkstore] [ng]'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_fb_2021 [p:ios] [g:msk] [cpo] [feed] [new] [general] [darkstore] [ng]') THEN 'realweb_fb_2021 [p:ios] [g:msk] [cpo] [feed] [new] [general] [darkstore] [ng]'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_fb_2021 [p:ios] [cpo] [feed] [discount] [old] [general] [darkstore] [forder] [i14]') THEN 'realweb_fb_2021 [p:ios] [cpo] [feed] [discount] [old] [general] [darkstore] [forder] [i14]'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_fb_2021 [p:ios] [cpi] [feed] [discount] [new] [general] [darkstore] [install] [i14]') THEN 'realweb_fb_2021 [p:ios] [cpi] [feed] [discount] [new] [general] [darkstore] [install] [i14]'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_fb_2021 [p:ios] [cpa] [feed] [discount] [new] [general] [darkstore] [forder] [i14]') THEN 'realweb_fb_2021 [p:ios] [cpa] [feed] [discount] [new] [general] [darkstore] [forder] [i14]'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_fb_2021 [p:and] [g:msk] [cpo] [feed] [old] [general] [darkstore] [ng]') THEN 'realweb_fb_2021 [p:and] [g:msk] [cpo] [feed] [old] [general] [darkstore] [ng]'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_fb_2021 [p:and] [g:msk] [cpo] [feed] [new] [general] [darkstore] [ng]') THEN 'realweb_fb_2021 [p:and] [g:msk] [cpo] [feed] [new] [general] [darkstore] [ng]'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_fb_2021 [p:and] [cpo_catalog] [feed] [discount] [old] [general] [darkstore] [forder]') THEN 'realweb_fb_2021 [p:and] [cpo_catalog] [feed] [discount] [old] [general] [darkstore] [forder]'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_fb_2021 [p:and] [cpo] [feed] [discount] [old] [general] [darkstore] [forder]') THEN 'realweb_fb_2021 [p:and] [cpo] [feed] [discount] [old] [general] [darkstore] [forder]'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_fb_2021 [p:and] [cpo] [feed] [discount] [new] [general] [darkstore] [install]') THEN 'realweb_fb_2021 [p:and] [cpo] [feed] [discount] [new] [general] [darkstore] [install]'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_fb_2021 [p:and] [cpi] [feed] [discount] [new] [general] [darkstore] [install]') THEN 'realweb_fb_2021 [p:and] [cpi] [feed] [discount] [new] [general] [darkstore] [install]'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promofeed') THEN 'promofeed'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_zavtrak') THEN 'promo_zavtrak'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_watermelon') THEN 'promo_watermelon'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_vsepo99') THEN 'promo_vsepo99'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_vse_po_49_i_99') THEN 'promo_vse_po_49_i_99'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_tea_coffee') THEN 'promo_tea_coffee'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_tea_and_cofee') THEN 'promo_tea_and_cofee'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_sun') THEN 'promo_sun'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_strawberry') THEN 'promo_strawberry'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_smoothies') THEN 'promo_smoothies'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_school') THEN 'promo_school'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_regular_present_vid') THEN 'promo_regular_present_vid'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_regular_present_2') THEN 'promo_regular_present_2'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_regular_present') THEN 'promo_regular_present'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_plov') THEN 'promo_plov'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_okroshka') THEN 'promo_okroshka'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_nemoloko') THEN 'promo_nemoloko'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_napitok') THEN 'promo_napitok'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_more_lososya') THEN 'promo_more_lososya'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_moloko_parmalat') THEN 'promo_moloko_parmalat'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_meet') THEN 'promo_meet'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_meat_21.07 - 02.08') THEN 'promo_meat_21.07 - 02.08'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_lisichki') THEN 'promo_lisichki'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_linzy') THEN 'promo_linzy'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_limonad') THEN 'promo_limonad'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_lego_december') THEN 'promo_lego_december'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_lego') THEN 'promo_lego'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_kvas') THEN 'promo_kvas'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_kolbasa') THEN 'promo_kolbasa'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_kasha') THEN 'promo_kasha'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_karamel_tokio') THEN 'promo_karamel_tokio'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_icecream') THEN 'promo_icecream'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_ice_cream') THEN 'promo_ice_cream'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_him') THEN 'promo_him'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_grill') THEN 'promo_grill'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_global_dyson_ret') THEN 'promo_global_dyson_ret'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_global_cyber_monday_zewa') THEN 'promo_global_cyber_monday_zewa'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_global_cyber_monday_vid2') THEN 'promo_global_cyber_monday_vid2'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_global_cyber_monday_vid1') THEN 'promo_global_cyber_monday_vid1'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_global_cyber_monday_pepsi') THEN 'promo_global_cyber_monday_pepsi'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_global_cyber_monday_losos') THEN 'promo_global_cyber_monday_losos'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_global_cyber_monday_drink') THEN 'promo_global_cyber_monday_drink'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_global_cyber_monday_25.01') THEN 'promo_global_cyber_monday_25.01'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_global_cyber_monday') THEN 'promo_global_cyber_monday'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_global_auto_carusel_2') THEN 'promo_global_auto_carusel_2'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_global_auto_carusel') THEN 'promo_global_auto_carusel'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_global_auto') THEN 'promo_global_auto'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_fruits_ berries') THEN 'promo_fruits_ berries'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_fish') THEN 'promo_fish'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_festmorozh') THEN 'promo_festmorozh'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_dostavka_dacha') THEN 'promo_dostavka_dacha'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_diapers') THEN 'promo_diapers'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_cosmetics') THEN 'promo_cosmetics'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_coffee') THEN 'promo_coffee'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_choco_ball') THEN 'promo_choco_ball'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_children') THEN 'promo_children'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_cherry') THEN 'promo_cherry'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_cheese_chechil') THEN 'promo_cheese_chechil'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_cheese') THEN 'promo_cheese'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_baton_alpin') THEN 'promo_baton_alpin'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_baton') THEN 'promo_baton'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_aurora') THEN 'promo_aurora'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_arbuz') THEN 'promo_arbuz'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'ng_olivier') THEN 'ng_olivier'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'ng_mandarin') THEN 'ng_mandarin'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'ng_kura') THEN 'ng_kura'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'ng_granata') THEN 'ng_granata'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'fixprice_17.09-27.09') THEN 'fixprice_17.09-27.09'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'express') THEN 'express'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'coffee_illy') THEN 'coffee_illy'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'black_friday') THEN 'black_friday'

    ELSE '-' END

 as promo_search,
        
    CASE
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'minusfrequency2')
        THEN 'All buyers minus frequency 2'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'aud.cart')
        THEN 'Add to cart not buy'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'core_promo_5')
        THEN 'Ядро_Промо+5_fix'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'reg_not_buy')
        THEN 'Registered but not buy'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'purchase15_22')
        THEN 'Предотток'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'purchase23_73')
        THEN 'Отток'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'purchase74_130')
        OR REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'purchase_aft130')
        THEN 'Глубокий отток'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'notused_30d')
        THEN 'Не использовали приложение более 30 дней'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'purch_less_2')
        THEN 'Покупают реже, чем 2 раза в месяц'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'purch_from_2')
        THEN 'Покупают от 2 раз в месяц'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'deep_outflow_rtg')
        THEN 'Deep_outflow_rtg'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'first_open_.ot_buy_rtg')
        THEN 'First_open_not_buy_rtg'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'installed_the_app_but_not_buy_rtg')
        THEN 'Installed_the_app_but_not_buy_rtg'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'registered_but_not_buy_rtg')
        THEN 'Registered_but_not_buy_rtg'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'firebase')
        THEN  "Предиктивная аудитория" 
        WHEN REGEXP_CONTAINS(LOWER(campaign_name), 'inapp')
        THEN 'Аудитории площадки (inapp)'
        ELSE 'Нет аудитории' END
 AS auditory,
        -- SUM(impressions),
        -- SUM(clicks),
        SUM(spend) AS spend
    FROM `perekrestokvprok-bq`.`dbt_production`.`int_vk_cab_meta`
    WHERE campaign_type = 'retargeting'
    AND REGEXP_CONTAINS(campaign_name, r'realweb')
    GROUP BY 1,2,3,4,5,6
),

vk_convs AS (
    SELECT 
        date,
        campaign_name,
        platform,
        promo_type,
        promo_search,
        auditory,
        SUM(IF(event_name = "af_purchase", event_revenue, 0)) AS revenue,
        SUM(IF(event_name = "af_purchase", event_count, 0)) AS purchase,
        SUM(IF(event_name = 're-engagement', event_count, 0)) AS re_engagement
    FROM af_conversions
    WHERE mediasource ='vk_int'
    AND is_retargeting = TRUE
    AND REGEXP_CONTAINS(campaign_name, r'realweb')
    AND REGEXP_CONTAINS(campaign_name, r'_ret_|[_\[]old[_\]]')
    GROUP BY 1,2,3,4,5,6
),

vk AS (
    SELECT
        COALESCE(vk_convs.date, vk_cost.date) AS date,
        COALESCE(vk_convs.campaign_name, vk_cost.campaign_name) AS campaign_name,
        COALESCE(vk_convs.platform, vk_cost.platform) AS platform,
        COALESCE(vk_convs.promo_type, vk_cost.promo_type) AS promo_type,
        COALESCE(vk_convs.promo_search, vk_cost.promo_search) AS promo_search,
        COALESCE(vk_convs.auditory, vk_cost.auditory) AS auditory,
        COALESCE(re_engagement,0) AS re_engagement,
        COALESCE(revenue,0) AS revenue,
        COALESCE(purchase,0) AS purchase,
        COALESCE(spend,0) AS spend,
        'ВК' AS source,
    FROM vk_convs
    FULL OUTER JOIN vk_cost
    ON vk_convs.date = vk_cost.date 
    AND vk_convs.campaign_name = vk_cost.campaign_name
    AND vk_convs.promo_type = vk_cost.promo_type
    AND vk_convs.promo_search = vk_cost.promo_search
    AND vk_convs.auditory = vk_cost.auditory
    WHERE COALESCE(re_engagement,0) + COALESCE(revenue,0) + COALESCE(purchase,0) + COALESCE(spend,0) > 0
    AND COALESCE(vk_convs.campaign_name, vk_cost.campaign_name) != 'None'
),

----------------------- mytarget -------------------------

mt_cost AS (
    SELECT
        date,
        campaign_name,
        
    CASE
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name],'')), r'promo.*regular') THEN 'promo regular'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name],'')), r'promo.*global') THEN 'promo global'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name],'')), r'promo.*feed') THEN 'promo feed'
    ELSE '-' END
 as promo_type,
        
    CASE
        WHEN REGEXP_CONTAINS(LOWER(campaign_name), r'\[p:ios\]|_ios_|p02') THEN 'ios'
        WHEN REGEXP_CONTAINS(LOWER(campaign_name), r'\[p:and\]|_and_|android|p01') THEN 'android'
    ELSE 'no_platform' END
 as platform,
        CASE
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'yagoda_14.09-28.09') THEN 'yagoda_14.09-28.09'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'tovar_rub_13.09-19.09') THEN 'tovar_rub_13.09-19.09'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'season_orange') THEN 'season_orange'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'season_mandarin') THEN 'season_mandarin'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'season_lemon') THEN 'season_lemon'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_ya_2022_ios_cpi_reg1_brand_display_promo_global') THEN 'realweb_ya_2022_ios_cpi_reg1_brand_display_promo_global'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_ya_2022_ios_cpi_nn_brand_display_promo_global') THEN 'realweb_ya_2022_ios_cpi_nn_brand_display_promo_global'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_ya_2022_ios_cpi_mskspb_brand_display_promo_global') THEN 'realweb_ya_2022_ios_cpi_mskspb_brand_display_promo_global'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_ya_2022_and_cpi_reg1_brand_display_promo_global') THEN 'realweb_ya_2022_and_cpi_reg1_brand_display_promo_global'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_ya_2022_and_cpi_nn_brand_display_promo_global') THEN 'realweb_ya_2022_and_cpi_nn_brand_display_promo_global'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_ya_2022_and_cpi_mskspb_brand_display_promo_global') THEN 'realweb_ya_2022_and_cpi_mskspb_brand_display_promo_global'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_ya_2021_and_cpi_rf_general_display_install_promo_regular') THEN 'realweb_ya_2021_and_cpi_rf_general_display_install_promo_regular'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_vk_2022_and_cpo_rf_old_ret_promo_global_cyber_monday') THEN 'realweb_vk_2022_and_cpo_rf_old_ret_promo_global_cyber_monday'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_tiktok_2022_mskspb_and_new_promo_global') THEN 'realweb_tiktok_2022_mskspb_and_new_promo_global'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_mt_2021 [p:and] [cpa] [mskspb] [old] [minusfrequency2] [promo_global]') THEN 'realweb_mt_2021 [p:and] [cpa] [mskspb] [old] [minusfrequency2] [promo_global]'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_fb_2021 [p:ios] [g:msk] [cpo] [feed] [old] [general] [darkstore] [ng]') THEN 'realweb_fb_2021 [p:ios] [g:msk] [cpo] [feed] [old] [general] [darkstore] [ng]'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_fb_2021 [p:ios] [g:msk] [cpo] [feed] [new] [general] [darkstore] [ng]') THEN 'realweb_fb_2021 [p:ios] [g:msk] [cpo] [feed] [new] [general] [darkstore] [ng]'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_fb_2021 [p:ios] [cpo] [feed] [discount] [old] [general] [darkstore] [forder] [i14]') THEN 'realweb_fb_2021 [p:ios] [cpo] [feed] [discount] [old] [general] [darkstore] [forder] [i14]'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_fb_2021 [p:ios] [cpi] [feed] [discount] [new] [general] [darkstore] [install] [i14]') THEN 'realweb_fb_2021 [p:ios] [cpi] [feed] [discount] [new] [general] [darkstore] [install] [i14]'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_fb_2021 [p:ios] [cpa] [feed] [discount] [new] [general] [darkstore] [forder] [i14]') THEN 'realweb_fb_2021 [p:ios] [cpa] [feed] [discount] [new] [general] [darkstore] [forder] [i14]'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_fb_2021 [p:and] [g:msk] [cpo] [feed] [old] [general] [darkstore] [ng]') THEN 'realweb_fb_2021 [p:and] [g:msk] [cpo] [feed] [old] [general] [darkstore] [ng]'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_fb_2021 [p:and] [g:msk] [cpo] [feed] [new] [general] [darkstore] [ng]') THEN 'realweb_fb_2021 [p:and] [g:msk] [cpo] [feed] [new] [general] [darkstore] [ng]'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_fb_2021 [p:and] [cpo_catalog] [feed] [discount] [old] [general] [darkstore] [forder]') THEN 'realweb_fb_2021 [p:and] [cpo_catalog] [feed] [discount] [old] [general] [darkstore] [forder]'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_fb_2021 [p:and] [cpo] [feed] [discount] [old] [general] [darkstore] [forder]') THEN 'realweb_fb_2021 [p:and] [cpo] [feed] [discount] [old] [general] [darkstore] [forder]'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_fb_2021 [p:and] [cpo] [feed] [discount] [new] [general] [darkstore] [install]') THEN 'realweb_fb_2021 [p:and] [cpo] [feed] [discount] [new] [general] [darkstore] [install]'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_fb_2021 [p:and] [cpi] [feed] [discount] [new] [general] [darkstore] [install]') THEN 'realweb_fb_2021 [p:and] [cpi] [feed] [discount] [new] [general] [darkstore] [install]'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promofeed') THEN 'promofeed'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_zavtrak') THEN 'promo_zavtrak'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_watermelon') THEN 'promo_watermelon'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_vsepo99') THEN 'promo_vsepo99'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_vse_po_49_i_99') THEN 'promo_vse_po_49_i_99'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_tea_coffee') THEN 'promo_tea_coffee'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_tea_and_cofee') THEN 'promo_tea_and_cofee'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_sun') THEN 'promo_sun'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_strawberry') THEN 'promo_strawberry'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_smoothies') THEN 'promo_smoothies'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_school') THEN 'promo_school'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_regular_present_vid') THEN 'promo_regular_present_vid'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_regular_present_2') THEN 'promo_regular_present_2'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_regular_present') THEN 'promo_regular_present'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_plov') THEN 'promo_plov'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_okroshka') THEN 'promo_okroshka'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_nemoloko') THEN 'promo_nemoloko'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_napitok') THEN 'promo_napitok'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_more_lososya') THEN 'promo_more_lososya'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_moloko_parmalat') THEN 'promo_moloko_parmalat'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_meet') THEN 'promo_meet'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_meat_21.07 - 02.08') THEN 'promo_meat_21.07 - 02.08'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_lisichki') THEN 'promo_lisichki'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_linzy') THEN 'promo_linzy'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_limonad') THEN 'promo_limonad'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_lego_december') THEN 'promo_lego_december'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_lego') THEN 'promo_lego'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_kvas') THEN 'promo_kvas'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_kolbasa') THEN 'promo_kolbasa'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_kasha') THEN 'promo_kasha'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_karamel_tokio') THEN 'promo_karamel_tokio'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_icecream') THEN 'promo_icecream'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_ice_cream') THEN 'promo_ice_cream'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_him') THEN 'promo_him'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_grill') THEN 'promo_grill'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_global_dyson_ret') THEN 'promo_global_dyson_ret'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_global_cyber_monday_zewa') THEN 'promo_global_cyber_monday_zewa'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_global_cyber_monday_vid2') THEN 'promo_global_cyber_monday_vid2'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_global_cyber_monday_vid1') THEN 'promo_global_cyber_monday_vid1'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_global_cyber_monday_pepsi') THEN 'promo_global_cyber_monday_pepsi'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_global_cyber_monday_losos') THEN 'promo_global_cyber_monday_losos'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_global_cyber_monday_drink') THEN 'promo_global_cyber_monday_drink'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_global_cyber_monday_25.01') THEN 'promo_global_cyber_monday_25.01'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_global_cyber_monday') THEN 'promo_global_cyber_monday'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_global_auto_carusel_2') THEN 'promo_global_auto_carusel_2'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_global_auto_carusel') THEN 'promo_global_auto_carusel'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_global_auto') THEN 'promo_global_auto'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_fruits_ berries') THEN 'promo_fruits_ berries'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_fish') THEN 'promo_fish'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_festmorozh') THEN 'promo_festmorozh'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_dostavka_dacha') THEN 'promo_dostavka_dacha'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_diapers') THEN 'promo_diapers'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_cosmetics') THEN 'promo_cosmetics'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_coffee') THEN 'promo_coffee'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_choco_ball') THEN 'promo_choco_ball'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_children') THEN 'promo_children'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_cherry') THEN 'promo_cherry'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_cheese_chechil') THEN 'promo_cheese_chechil'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_cheese') THEN 'promo_cheese'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_baton_alpin') THEN 'promo_baton_alpin'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_baton') THEN 'promo_baton'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_aurora') THEN 'promo_aurora'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_arbuz') THEN 'promo_arbuz'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'ng_olivier') THEN 'ng_olivier'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'ng_mandarin') THEN 'ng_mandarin'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'ng_kura') THEN 'ng_kura'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'ng_granata') THEN 'ng_granata'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'fixprice_17.09-27.09') THEN 'fixprice_17.09-27.09'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'express') THEN 'express'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'coffee_illy') THEN 'coffee_illy'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'black_friday') THEN 'black_friday'

    ELSE '-' END

 as promo_search,
        
    CASE
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'minusfrequency2')
        THEN 'All buyers minus frequency 2'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'aud.cart')
        THEN 'Add to cart not buy'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'core_promo_5')
        THEN 'Ядро_Промо+5_fix'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'reg_not_buy')
        THEN 'Registered but not buy'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'purchase15_22')
        THEN 'Предотток'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'purchase23_73')
        THEN 'Отток'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'purchase74_130')
        OR REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'purchase_aft130')
        THEN 'Глубокий отток'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'notused_30d')
        THEN 'Не использовали приложение более 30 дней'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'purch_less_2')
        THEN 'Покупают реже, чем 2 раза в месяц'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'purch_from_2')
        THEN 'Покупают от 2 раз в месяц'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'deep_outflow_rtg')
        THEN 'Deep_outflow_rtg'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'first_open_.ot_buy_rtg')
        THEN 'First_open_not_buy_rtg'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'installed_the_app_but_not_buy_rtg')
        THEN 'Installed_the_app_but_not_buy_rtg'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'registered_but_not_buy_rtg')
        THEN 'Registered_but_not_buy_rtg'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'firebase')
        THEN  "Предиктивная аудитория" 
        WHEN REGEXP_CONTAINS(LOWER(campaign_name), 'inapp')
        THEN 'Аудитории площадки (inapp)'
        ELSE 'Нет аудитории' END
 AS auditory,
        -- SUM(impressions),
        -- SUM(clicks),
        SUM(spend) AS spend
    FROM `perekrestokvprok-bq`.`dbt_production`.`int_mytarget_cab_meta`
    WHERE campaign_type = 'retargeting'
    AND REGEXP_CONTAINS(campaign_name, r'realweb')
    GROUP BY 1,2,3,4,5,6
),

mt_convs AS (
    SELECT 
        date,
        campaign_name,
        platform,
        promo_type,
        promo_search,
        auditory,
        SUM(IF(event_name = "af_purchase", event_revenue, 0)) AS revenue,
        SUM(IF(event_name = "af_purchase", event_count, 0)) AS purchase,
        SUM(IF(event_name = 're-engagement', event_count, 0)) AS re_engagement
    FROM af_conversions
    WHERE mediasource = 'mail.ru_int'
    AND is_retargeting = TRUE
    AND REGEXP_CONTAINS(campaign_name, r'realweb')
    AND REGEXP_CONTAINS(campaign_name, r'_ret_|[_\[]old[_\]]')
    GROUP BY 1,2,3,4,5,6
),

mt AS (
    SELECT
        COALESCE(mt_convs.date, mt_cost.date) AS date,
        COALESCE(mt_convs.campaign_name, mt_cost.campaign_name) AS campaign_name,
        COALESCE(mt_convs.platform, mt_cost.platform) AS platform,
        COALESCE(mt_convs.promo_type, mt_cost.promo_type) AS promo_type,
        COALESCE(mt_convs.promo_search, mt_cost.promo_search) AS promo_search,
        COALESCE(mt_convs.auditory, mt_cost.auditory) AS auditory,
        COALESCE(re_engagement,0) AS re_engagement,
        COALESCE(revenue,0) AS revenue,
        COALESCE(purchase,0) AS purchase,
        COALESCE(spend,0) AS spend,
        'MyTarget' AS source,
    FROM mt_convs
    FULL OUTER JOIN mt_cost
    ON mt_convs.date = mt_cost.date 
    AND mt_convs.campaign_name = mt_cost.campaign_name
    AND mt_convs.promo_type = mt_cost.promo_type
    AND mt_convs.promo_search = mt_cost.promo_search
    AND mt_convs.auditory = mt_cost.auditory
    WHERE COALESCE(re_engagement,0) + COALESCE(revenue,0) + COALESCE(purchase,0) + COALESCE(spend,0) > 0
    AND COALESCE(mt_convs.campaign_name, mt_cost.campaign_name) != 'None'
),

----------------------- twitter -------------------------

tw_cost AS (
    SELECT
        date,
        campaign_name,
        
    CASE
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-"],'')), r'promo.*regular') THEN 'promo regular'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-"],'')), r'promo.*global') THEN 'promo global'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-"],'')), r'promo.*feed') THEN 'promo feed'
    ELSE '-' END
 as promo_type,
        
    CASE
        WHEN REGEXP_CONTAINS(LOWER(campaign_name), r'\[p:ios\]|_ios_|p02') THEN 'ios'
        WHEN REGEXP_CONTAINS(LOWER(campaign_name), r'\[p:and\]|_and_|android|p01') THEN 'android'
    ELSE 'no_platform' END
 as platform,
        CASE
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-", "-"], ' ')), r'yagoda_14.09-28.09') THEN 'yagoda_14.09-28.09'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-", "-"], ' ')), r'tovar_rub_13.09-19.09') THEN 'tovar_rub_13.09-19.09'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-", "-"], ' ')), r'season_orange') THEN 'season_orange'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-", "-"], ' ')), r'season_mandarin') THEN 'season_mandarin'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-", "-"], ' ')), r'season_lemon') THEN 'season_lemon'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-", "-"], ' ')), r'realweb_ya_2022_ios_cpi_reg1_brand_display_promo_global') THEN 'realweb_ya_2022_ios_cpi_reg1_brand_display_promo_global'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-", "-"], ' ')), r'realweb_ya_2022_ios_cpi_nn_brand_display_promo_global') THEN 'realweb_ya_2022_ios_cpi_nn_brand_display_promo_global'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-", "-"], ' ')), r'realweb_ya_2022_ios_cpi_mskspb_brand_display_promo_global') THEN 'realweb_ya_2022_ios_cpi_mskspb_brand_display_promo_global'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-", "-"], ' ')), r'realweb_ya_2022_and_cpi_reg1_brand_display_promo_global') THEN 'realweb_ya_2022_and_cpi_reg1_brand_display_promo_global'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-", "-"], ' ')), r'realweb_ya_2022_and_cpi_nn_brand_display_promo_global') THEN 'realweb_ya_2022_and_cpi_nn_brand_display_promo_global'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-", "-"], ' ')), r'realweb_ya_2022_and_cpi_mskspb_brand_display_promo_global') THEN 'realweb_ya_2022_and_cpi_mskspb_brand_display_promo_global'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-", "-"], ' ')), r'realweb_ya_2021_and_cpi_rf_general_display_install_promo_regular') THEN 'realweb_ya_2021_and_cpi_rf_general_display_install_promo_regular'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-", "-"], ' ')), r'realweb_vk_2022_and_cpo_rf_old_ret_promo_global_cyber_monday') THEN 'realweb_vk_2022_and_cpo_rf_old_ret_promo_global_cyber_monday'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-", "-"], ' ')), r'realweb_tiktok_2022_mskspb_and_new_promo_global') THEN 'realweb_tiktok_2022_mskspb_and_new_promo_global'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-", "-"], ' ')), r'realweb_mt_2021 [p:and] [cpa] [mskspb] [old] [minusfrequency2] [promo_global]') THEN 'realweb_mt_2021 [p:and] [cpa] [mskspb] [old] [minusfrequency2] [promo_global]'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-", "-"], ' ')), r'realweb_fb_2021 [p:ios] [g:msk] [cpo] [feed] [old] [general] [darkstore] [ng]') THEN 'realweb_fb_2021 [p:ios] [g:msk] [cpo] [feed] [old] [general] [darkstore] [ng]'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-", "-"], ' ')), r'realweb_fb_2021 [p:ios] [g:msk] [cpo] [feed] [new] [general] [darkstore] [ng]') THEN 'realweb_fb_2021 [p:ios] [g:msk] [cpo] [feed] [new] [general] [darkstore] [ng]'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-", "-"], ' ')), r'realweb_fb_2021 [p:ios] [cpo] [feed] [discount] [old] [general] [darkstore] [forder] [i14]') THEN 'realweb_fb_2021 [p:ios] [cpo] [feed] [discount] [old] [general] [darkstore] [forder] [i14]'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-", "-"], ' ')), r'realweb_fb_2021 [p:ios] [cpi] [feed] [discount] [new] [general] [darkstore] [install] [i14]') THEN 'realweb_fb_2021 [p:ios] [cpi] [feed] [discount] [new] [general] [darkstore] [install] [i14]'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-", "-"], ' ')), r'realweb_fb_2021 [p:ios] [cpa] [feed] [discount] [new] [general] [darkstore] [forder] [i14]') THEN 'realweb_fb_2021 [p:ios] [cpa] [feed] [discount] [new] [general] [darkstore] [forder] [i14]'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-", "-"], ' ')), r'realweb_fb_2021 [p:and] [g:msk] [cpo] [feed] [old] [general] [darkstore] [ng]') THEN 'realweb_fb_2021 [p:and] [g:msk] [cpo] [feed] [old] [general] [darkstore] [ng]'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-", "-"], ' ')), r'realweb_fb_2021 [p:and] [g:msk] [cpo] [feed] [new] [general] [darkstore] [ng]') THEN 'realweb_fb_2021 [p:and] [g:msk] [cpo] [feed] [new] [general] [darkstore] [ng]'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-", "-"], ' ')), r'realweb_fb_2021 [p:and] [cpo_catalog] [feed] [discount] [old] [general] [darkstore] [forder]') THEN 'realweb_fb_2021 [p:and] [cpo_catalog] [feed] [discount] [old] [general] [darkstore] [forder]'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-", "-"], ' ')), r'realweb_fb_2021 [p:and] [cpo] [feed] [discount] [old] [general] [darkstore] [forder]') THEN 'realweb_fb_2021 [p:and] [cpo] [feed] [discount] [old] [general] [darkstore] [forder]'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-", "-"], ' ')), r'realweb_fb_2021 [p:and] [cpo] [feed] [discount] [new] [general] [darkstore] [install]') THEN 'realweb_fb_2021 [p:and] [cpo] [feed] [discount] [new] [general] [darkstore] [install]'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-", "-"], ' ')), r'realweb_fb_2021 [p:and] [cpi] [feed] [discount] [new] [general] [darkstore] [install]') THEN 'realweb_fb_2021 [p:and] [cpi] [feed] [discount] [new] [general] [darkstore] [install]'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-", "-"], ' ')), r'promofeed') THEN 'promofeed'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-", "-"], ' ')), r'promo_zavtrak') THEN 'promo_zavtrak'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-", "-"], ' ')), r'promo_watermelon') THEN 'promo_watermelon'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-", "-"], ' ')), r'promo_vsepo99') THEN 'promo_vsepo99'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-", "-"], ' ')), r'promo_vse_po_49_i_99') THEN 'promo_vse_po_49_i_99'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-", "-"], ' ')), r'promo_tea_coffee') THEN 'promo_tea_coffee'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-", "-"], ' ')), r'promo_tea_and_cofee') THEN 'promo_tea_and_cofee'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-", "-"], ' ')), r'promo_sun') THEN 'promo_sun'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-", "-"], ' ')), r'promo_strawberry') THEN 'promo_strawberry'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-", "-"], ' ')), r'promo_smoothies') THEN 'promo_smoothies'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-", "-"], ' ')), r'promo_school') THEN 'promo_school'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-", "-"], ' ')), r'promo_regular_present_vid') THEN 'promo_regular_present_vid'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-", "-"], ' ')), r'promo_regular_present_2') THEN 'promo_regular_present_2'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-", "-"], ' ')), r'promo_regular_present') THEN 'promo_regular_present'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-", "-"], ' ')), r'promo_plov') THEN 'promo_plov'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-", "-"], ' ')), r'promo_okroshka') THEN 'promo_okroshka'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-", "-"], ' ')), r'promo_nemoloko') THEN 'promo_nemoloko'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-", "-"], ' ')), r'promo_napitok') THEN 'promo_napitok'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-", "-"], ' ')), r'promo_more_lososya') THEN 'promo_more_lososya'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-", "-"], ' ')), r'promo_moloko_parmalat') THEN 'promo_moloko_parmalat'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-", "-"], ' ')), r'promo_meet') THEN 'promo_meet'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-", "-"], ' ')), r'promo_meat_21.07 - 02.08') THEN 'promo_meat_21.07 - 02.08'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-", "-"], ' ')), r'promo_lisichki') THEN 'promo_lisichki'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-", "-"], ' ')), r'promo_linzy') THEN 'promo_linzy'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-", "-"], ' ')), r'promo_limonad') THEN 'promo_limonad'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-", "-"], ' ')), r'promo_lego_december') THEN 'promo_lego_december'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-", "-"], ' ')), r'promo_lego') THEN 'promo_lego'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-", "-"], ' ')), r'promo_kvas') THEN 'promo_kvas'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-", "-"], ' ')), r'promo_kolbasa') THEN 'promo_kolbasa'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-", "-"], ' ')), r'promo_kasha') THEN 'promo_kasha'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-", "-"], ' ')), r'promo_karamel_tokio') THEN 'promo_karamel_tokio'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-", "-"], ' ')), r'promo_icecream') THEN 'promo_icecream'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-", "-"], ' ')), r'promo_ice_cream') THEN 'promo_ice_cream'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-", "-"], ' ')), r'promo_him') THEN 'promo_him'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-", "-"], ' ')), r'promo_grill') THEN 'promo_grill'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-", "-"], ' ')), r'promo_global_dyson_ret') THEN 'promo_global_dyson_ret'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-", "-"], ' ')), r'promo_global_cyber_monday_zewa') THEN 'promo_global_cyber_monday_zewa'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-", "-"], ' ')), r'promo_global_cyber_monday_vid2') THEN 'promo_global_cyber_monday_vid2'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-", "-"], ' ')), r'promo_global_cyber_monday_vid1') THEN 'promo_global_cyber_monday_vid1'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-", "-"], ' ')), r'promo_global_cyber_monday_pepsi') THEN 'promo_global_cyber_monday_pepsi'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-", "-"], ' ')), r'promo_global_cyber_monday_losos') THEN 'promo_global_cyber_monday_losos'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-", "-"], ' ')), r'promo_global_cyber_monday_drink') THEN 'promo_global_cyber_monday_drink'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-", "-"], ' ')), r'promo_global_cyber_monday_25.01') THEN 'promo_global_cyber_monday_25.01'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-", "-"], ' ')), r'promo_global_cyber_monday') THEN 'promo_global_cyber_monday'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-", "-"], ' ')), r'promo_global_auto_carusel_2') THEN 'promo_global_auto_carusel_2'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-", "-"], ' ')), r'promo_global_auto_carusel') THEN 'promo_global_auto_carusel'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-", "-"], ' ')), r'promo_global_auto') THEN 'promo_global_auto'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-", "-"], ' ')), r'promo_fruits_ berries') THEN 'promo_fruits_ berries'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-", "-"], ' ')), r'promo_fish') THEN 'promo_fish'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-", "-"], ' ')), r'promo_festmorozh') THEN 'promo_festmorozh'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-", "-"], ' ')), r'promo_dostavka_dacha') THEN 'promo_dostavka_dacha'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-", "-"], ' ')), r'promo_diapers') THEN 'promo_diapers'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-", "-"], ' ')), r'promo_cosmetics') THEN 'promo_cosmetics'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-", "-"], ' ')), r'promo_coffee') THEN 'promo_coffee'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-", "-"], ' ')), r'promo_choco_ball') THEN 'promo_choco_ball'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-", "-"], ' ')), r'promo_children') THEN 'promo_children'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-", "-"], ' ')), r'promo_cherry') THEN 'promo_cherry'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-", "-"], ' ')), r'promo_cheese_chechil') THEN 'promo_cheese_chechil'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-", "-"], ' ')), r'promo_cheese') THEN 'promo_cheese'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-", "-"], ' ')), r'promo_baton_alpin') THEN 'promo_baton_alpin'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-", "-"], ' ')), r'promo_baton') THEN 'promo_baton'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-", "-"], ' ')), r'promo_aurora') THEN 'promo_aurora'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-", "-"], ' ')), r'promo_arbuz') THEN 'promo_arbuz'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-", "-"], ' ')), r'ng_olivier') THEN 'ng_olivier'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-", "-"], ' ')), r'ng_mandarin') THEN 'ng_mandarin'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-", "-"], ' ')), r'ng_kura') THEN 'ng_kura'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-", "-"], ' ')), r'ng_granata') THEN 'ng_granata'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-", "-"], ' ')), r'fixprice_17.09-27.09') THEN 'fixprice_17.09-27.09'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-", "-"], ' ')), r'express') THEN 'express'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-", "-"], ' ')), r'coffee_illy') THEN 'coffee_illy'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-", "-"], ' ')), r'black_friday') THEN 'black_friday'

    ELSE '-' END

 as promo_search,
        
    CASE
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-"], ' ')), 'minusfrequency2')
        THEN 'All buyers minus frequency 2'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-"], ' ')), 'aud.cart')
        THEN 'Add to cart not buy'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-"], ' ')), 'core_promo_5')
        THEN 'Ядро_Промо+5_fix'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-"], ' ')), 'reg_not_buy')
        THEN 'Registered but not buy'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-"], ' ')), 'purchase15_22')
        THEN 'Предотток'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-"], ' ')), 'purchase23_73')
        THEN 'Отток'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-"], ' ')), 'purchase74_130')
        OR REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-"], ' ')), 'purchase_aft130')
        THEN 'Глубокий отток'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-"], ' ')), 'notused_30d')
        THEN 'Не использовали приложение более 30 дней'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-"], ' ')), 'purch_less_2')
        THEN 'Покупают реже, чем 2 раза в месяц'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-"], ' ')), 'purch_from_2')
        THEN 'Покупают от 2 раз в месяц'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-"], ' ')), 'deep_outflow_rtg')
        THEN 'Deep_outflow_rtg'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-"], ' ')), 'first_open_.ot_buy_rtg')
        THEN 'First_open_not_buy_rtg'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-"], ' ')), 'installed_the_app_but_not_buy_rtg')
        THEN 'Installed_the_app_but_not_buy_rtg'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-"], ' ')), 'registered_but_not_buy_rtg')
        THEN 'Registered_but_not_buy_rtg'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-"], ' ')), 'firebase')
        THEN  "Предиктивная аудитория" 
        WHEN REGEXP_CONTAINS(LOWER(campaign_name), 'inapp')
        THEN 'Аудитории площадки (inapp)'
        ELSE 'Нет аудитории' END
 AS auditory,
        -- SUM(impressions),
        -- SUM(clicks),
        SUM(spend) AS spend
    FROM `perekrestokvprok-bq`.`dbt_production`.`stg_twitter_cab_sheets`
    WHERE campaign_type = 'retargeting'
    AND REGEXP_CONTAINS(campaign_name, r'realweb_tw')
    GROUP BY 1,2,3,4,5,6
),

tw_convs AS (
    SELECT 
        date,
        campaign_name,
        platform,
        promo_type,
        promo_search,
        auditory,
        SUM(IF(event_name = "af_purchase", event_revenue, 0)) AS revenue,
        SUM(IF(event_name = "af_purchase", event_count, 0)) AS purchase,
        SUM(IF(event_name = 're-engagement', event_count, 0)) AS re_engagement
    FROM af_conversions
    WHERE is_retargeting = TRUE
    AND REGEXP_CONTAINS(campaign_name, r'realweb_tw')
    AND REGEXP_CONTAINS(campaign_name, r'_ret_|[_\[]old[_\]]')
    GROUP BY 1,2,3,4,5,6
),

tw AS (
    SELECT
        COALESCE(tw_convs.date, tw_cost.date) AS date,
        COALESCE(tw_convs.campaign_name, tw_cost.campaign_name) AS campaign_name,
        COALESCE(tw_convs.platform, tw_cost.platform) AS platform,
        COALESCE(tw_convs.promo_type, tw_cost.promo_type) AS promo_type,
        COALESCE(tw_convs.promo_search, tw_cost.promo_search) AS promo_search,
        COALESCE(tw_convs.auditory, tw_cost.auditory) AS auditory,
        COALESCE(re_engagement,0) AS re_engagement,
        COALESCE(revenue,0) AS revenue,
        COALESCE(purchase,0) AS purchase,
        COALESCE(spend,0) AS spend,
        'Twitter' AS source,
    FROM tw_convs
    FULL OUTER JOIN tw_cost
    ON tw_convs.date = tw_cost.date 
    AND tw_convs.campaign_name = tw_cost.campaign_name
    AND tw_convs.promo_type = tw_cost.promo_type
    AND tw_convs.promo_search = tw_cost.promo_search
    AND tw_convs.auditory = tw_cost.auditory
    WHERE COALESCE(re_engagement,0) + COALESCE(revenue,0) + COALESCE(purchase,0) + COALESCE(spend,0) > 0
    AND COALESCE(tw_convs.campaign_name, tw_cost.campaign_name) != 'None'
),

----------------------- tiktok -------------------------

tiktok_cost AS (
    SELECT
        date,
        campaign_name,
        
    CASE
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name],'')), r'promo.*regular') THEN 'promo regular'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name],'')), r'promo.*global') THEN 'promo global'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name],'')), r'promo.*feed') THEN 'promo feed'
    ELSE '-' END
 as promo_type,
        
    CASE
        WHEN REGEXP_CONTAINS(LOWER(campaign_name), r'\[p:ios\]|_ios_|p02') THEN 'ios'
        WHEN REGEXP_CONTAINS(LOWER(campaign_name), r'\[p:and\]|_and_|android|p01') THEN 'android'
    ELSE 'no_platform' END
 as platform,
        CASE
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'yagoda_14.09-28.09') THEN 'yagoda_14.09-28.09'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'tovar_rub_13.09-19.09') THEN 'tovar_rub_13.09-19.09'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'season_orange') THEN 'season_orange'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'season_mandarin') THEN 'season_mandarin'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'season_lemon') THEN 'season_lemon'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_ya_2022_ios_cpi_reg1_brand_display_promo_global') THEN 'realweb_ya_2022_ios_cpi_reg1_brand_display_promo_global'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_ya_2022_ios_cpi_nn_brand_display_promo_global') THEN 'realweb_ya_2022_ios_cpi_nn_brand_display_promo_global'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_ya_2022_ios_cpi_mskspb_brand_display_promo_global') THEN 'realweb_ya_2022_ios_cpi_mskspb_brand_display_promo_global'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_ya_2022_and_cpi_reg1_brand_display_promo_global') THEN 'realweb_ya_2022_and_cpi_reg1_brand_display_promo_global'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_ya_2022_and_cpi_nn_brand_display_promo_global') THEN 'realweb_ya_2022_and_cpi_nn_brand_display_promo_global'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_ya_2022_and_cpi_mskspb_brand_display_promo_global') THEN 'realweb_ya_2022_and_cpi_mskspb_brand_display_promo_global'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_ya_2021_and_cpi_rf_general_display_install_promo_regular') THEN 'realweb_ya_2021_and_cpi_rf_general_display_install_promo_regular'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_vk_2022_and_cpo_rf_old_ret_promo_global_cyber_monday') THEN 'realweb_vk_2022_and_cpo_rf_old_ret_promo_global_cyber_monday'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_tiktok_2022_mskspb_and_new_promo_global') THEN 'realweb_tiktok_2022_mskspb_and_new_promo_global'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_mt_2021 [p:and] [cpa] [mskspb] [old] [minusfrequency2] [promo_global]') THEN 'realweb_mt_2021 [p:and] [cpa] [mskspb] [old] [minusfrequency2] [promo_global]'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_fb_2021 [p:ios] [g:msk] [cpo] [feed] [old] [general] [darkstore] [ng]') THEN 'realweb_fb_2021 [p:ios] [g:msk] [cpo] [feed] [old] [general] [darkstore] [ng]'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_fb_2021 [p:ios] [g:msk] [cpo] [feed] [new] [general] [darkstore] [ng]') THEN 'realweb_fb_2021 [p:ios] [g:msk] [cpo] [feed] [new] [general] [darkstore] [ng]'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_fb_2021 [p:ios] [cpo] [feed] [discount] [old] [general] [darkstore] [forder] [i14]') THEN 'realweb_fb_2021 [p:ios] [cpo] [feed] [discount] [old] [general] [darkstore] [forder] [i14]'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_fb_2021 [p:ios] [cpi] [feed] [discount] [new] [general] [darkstore] [install] [i14]') THEN 'realweb_fb_2021 [p:ios] [cpi] [feed] [discount] [new] [general] [darkstore] [install] [i14]'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_fb_2021 [p:ios] [cpa] [feed] [discount] [new] [general] [darkstore] [forder] [i14]') THEN 'realweb_fb_2021 [p:ios] [cpa] [feed] [discount] [new] [general] [darkstore] [forder] [i14]'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_fb_2021 [p:and] [g:msk] [cpo] [feed] [old] [general] [darkstore] [ng]') THEN 'realweb_fb_2021 [p:and] [g:msk] [cpo] [feed] [old] [general] [darkstore] [ng]'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_fb_2021 [p:and] [g:msk] [cpo] [feed] [new] [general] [darkstore] [ng]') THEN 'realweb_fb_2021 [p:and] [g:msk] [cpo] [feed] [new] [general] [darkstore] [ng]'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_fb_2021 [p:and] [cpo_catalog] [feed] [discount] [old] [general] [darkstore] [forder]') THEN 'realweb_fb_2021 [p:and] [cpo_catalog] [feed] [discount] [old] [general] [darkstore] [forder]'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_fb_2021 [p:and] [cpo] [feed] [discount] [old] [general] [darkstore] [forder]') THEN 'realweb_fb_2021 [p:and] [cpo] [feed] [discount] [old] [general] [darkstore] [forder]'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_fb_2021 [p:and] [cpo] [feed] [discount] [new] [general] [darkstore] [install]') THEN 'realweb_fb_2021 [p:and] [cpo] [feed] [discount] [new] [general] [darkstore] [install]'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_fb_2021 [p:and] [cpi] [feed] [discount] [new] [general] [darkstore] [install]') THEN 'realweb_fb_2021 [p:and] [cpi] [feed] [discount] [new] [general] [darkstore] [install]'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promofeed') THEN 'promofeed'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_zavtrak') THEN 'promo_zavtrak'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_watermelon') THEN 'promo_watermelon'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_vsepo99') THEN 'promo_vsepo99'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_vse_po_49_i_99') THEN 'promo_vse_po_49_i_99'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_tea_coffee') THEN 'promo_tea_coffee'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_tea_and_cofee') THEN 'promo_tea_and_cofee'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_sun') THEN 'promo_sun'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_strawberry') THEN 'promo_strawberry'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_smoothies') THEN 'promo_smoothies'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_school') THEN 'promo_school'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_regular_present_vid') THEN 'promo_regular_present_vid'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_regular_present_2') THEN 'promo_regular_present_2'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_regular_present') THEN 'promo_regular_present'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_plov') THEN 'promo_plov'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_okroshka') THEN 'promo_okroshka'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_nemoloko') THEN 'promo_nemoloko'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_napitok') THEN 'promo_napitok'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_more_lososya') THEN 'promo_more_lososya'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_moloko_parmalat') THEN 'promo_moloko_parmalat'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_meet') THEN 'promo_meet'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_meat_21.07 - 02.08') THEN 'promo_meat_21.07 - 02.08'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_lisichki') THEN 'promo_lisichki'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_linzy') THEN 'promo_linzy'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_limonad') THEN 'promo_limonad'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_lego_december') THEN 'promo_lego_december'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_lego') THEN 'promo_lego'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_kvas') THEN 'promo_kvas'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_kolbasa') THEN 'promo_kolbasa'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_kasha') THEN 'promo_kasha'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_karamel_tokio') THEN 'promo_karamel_tokio'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_icecream') THEN 'promo_icecream'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_ice_cream') THEN 'promo_ice_cream'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_him') THEN 'promo_him'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_grill') THEN 'promo_grill'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_global_dyson_ret') THEN 'promo_global_dyson_ret'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_global_cyber_monday_zewa') THEN 'promo_global_cyber_monday_zewa'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_global_cyber_monday_vid2') THEN 'promo_global_cyber_monday_vid2'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_global_cyber_monday_vid1') THEN 'promo_global_cyber_monday_vid1'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_global_cyber_monday_pepsi') THEN 'promo_global_cyber_monday_pepsi'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_global_cyber_monday_losos') THEN 'promo_global_cyber_monday_losos'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_global_cyber_monday_drink') THEN 'promo_global_cyber_monday_drink'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_global_cyber_monday_25.01') THEN 'promo_global_cyber_monday_25.01'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_global_cyber_monday') THEN 'promo_global_cyber_monday'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_global_auto_carusel_2') THEN 'promo_global_auto_carusel_2'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_global_auto_carusel') THEN 'promo_global_auto_carusel'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_global_auto') THEN 'promo_global_auto'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_fruits_ berries') THEN 'promo_fruits_ berries'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_fish') THEN 'promo_fish'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_festmorozh') THEN 'promo_festmorozh'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_dostavka_dacha') THEN 'promo_dostavka_dacha'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_diapers') THEN 'promo_diapers'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_cosmetics') THEN 'promo_cosmetics'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_coffee') THEN 'promo_coffee'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_choco_ball') THEN 'promo_choco_ball'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_children') THEN 'promo_children'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_cherry') THEN 'promo_cherry'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_cheese_chechil') THEN 'promo_cheese_chechil'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_cheese') THEN 'promo_cheese'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_baton_alpin') THEN 'promo_baton_alpin'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_baton') THEN 'promo_baton'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_aurora') THEN 'promo_aurora'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_arbuz') THEN 'promo_arbuz'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'ng_olivier') THEN 'ng_olivier'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'ng_mandarin') THEN 'ng_mandarin'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'ng_kura') THEN 'ng_kura'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'ng_granata') THEN 'ng_granata'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'fixprice_17.09-27.09') THEN 'fixprice_17.09-27.09'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'express') THEN 'express'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'coffee_illy') THEN 'coffee_illy'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'black_friday') THEN 'black_friday'

    ELSE '-' END

 as promo_search,
        
    CASE
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'minusfrequency2')
        THEN 'All buyers minus frequency 2'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'aud.cart')
        THEN 'Add to cart not buy'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'core_promo_5')
        THEN 'Ядро_Промо+5_fix'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'reg_not_buy')
        THEN 'Registered but not buy'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'purchase15_22')
        THEN 'Предотток'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'purchase23_73')
        THEN 'Отток'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'purchase74_130')
        OR REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'purchase_aft130')
        THEN 'Глубокий отток'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'notused_30d')
        THEN 'Не использовали приложение более 30 дней'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'purch_less_2')
        THEN 'Покупают реже, чем 2 раза в месяц'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'purch_from_2')
        THEN 'Покупают от 2 раз в месяц'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'deep_outflow_rtg')
        THEN 'Deep_outflow_rtg'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'first_open_.ot_buy_rtg')
        THEN 'First_open_not_buy_rtg'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'installed_the_app_but_not_buy_rtg')
        THEN 'Installed_the_app_but_not_buy_rtg'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'registered_but_not_buy_rtg')
        THEN 'Registered_but_not_buy_rtg'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'firebase')
        THEN  "Предиктивная аудитория" 
        WHEN REGEXP_CONTAINS(LOWER(campaign_name), 'inapp')
        THEN 'Аудитории площадки (inapp)'
        ELSE 'Нет аудитории' END
 AS auditory,
        -- SUM(impressions),
        -- SUM(clicks),
        SUM(spend) AS spend,
        -- для тиктока из кабинета:--
        SUM(purchase) AS purchase
    FROM `perekrestokvprok-bq`.`dbt_production`.`stg_tiktok_cab_meta`
    WHERE campaign_type = 'retargeting'
    AND REGEXP_CONTAINS(campaign_name, r'realweb')
    GROUP BY 1,2,3,4,5,6
),

tiktok_convs AS (
    SELECT 
        date,
        campaign_name,
        platform,
        promo_type,
        promo_search,
        auditory,
        SUM(IF(event_name = "af_purchase", event_revenue, 0)) AS revenue,
        --SUM(IF(event_name = "af_purchase", event_count, 0)) AS purchase,
        SUM(IF(event_name = 're-engagement', event_count, 0)) AS re_engagement
    FROM af_conversions
    WHERE  is_retargeting = TRUE
    AND REGEXP_CONTAINS(campaign_name, r'realweb_tiktok')
    AND REGEXP_CONTAINS(campaign_name, r'_ret_|[_\[]old[_\]]')
    GROUP BY 1,2,3,4,5,6
),

tiktok AS (
    SELECT
        COALESCE(tiktok_convs.date, tiktok_cost.date) AS date,
        COALESCE(tiktok_convs.campaign_name, tiktok_cost.campaign_name) AS campaign_name,
        COALESCE(tiktok_convs.platform, tiktok_cost.platform) AS platform,
        COALESCE(tiktok_convs.promo_type, tiktok_cost.promo_type) AS promo_type,
        COALESCE(tiktok_convs.promo_search, tiktok_cost.promo_search) AS promo_search,
        COALESCE(tiktok_convs.auditory, tiktok_cost.auditory) AS auditory,
        COALESCE(re_engagement,0) AS re_engagement,
        COALESCE(revenue,0) AS revenue,
        COALESCE(purchase,0) AS purchase,
        COALESCE(spend,0) AS spend,
        'TikTok' AS source,
    FROM tiktok_convs
    FULL OUTER JOIN tiktok_cost
    ON tiktok_convs.date = tiktok_cost.date 
    AND tiktok_convs.campaign_name = tiktok_cost.campaign_name
    AND tiktok_convs.promo_type = tiktok_cost.promo_type
    AND tiktok_convs.promo_search = tiktok_cost.promo_search
    AND tiktok_convs.auditory = tiktok_cost.auditory
    WHERE COALESCE(re_engagement,0) + COALESCE(revenue,0) + COALESCE(purchase,0) + COALESCE(spend,0) > 0
    AND COALESCE(tiktok_convs.campaign_name, tiktok_cost.campaign_name) != 'None'
),

----------------------- apple search ads -------------------------

asa_cost AS (
    SELECT
        date,
        campaign_name,
        
    CASE
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name],'')), r'promo.*regular') THEN 'promo regular'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name],'')), r'promo.*global') THEN 'promo global'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name],'')), r'promo.*feed') THEN 'promo feed'
    ELSE '-' END
 as promo_type,
        CASE
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'yagoda_14.09-28.09') THEN 'yagoda_14.09-28.09'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'tovar_rub_13.09-19.09') THEN 'tovar_rub_13.09-19.09'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'season_orange') THEN 'season_orange'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'season_mandarin') THEN 'season_mandarin'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'season_lemon') THEN 'season_lemon'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_ya_2022_ios_cpi_reg1_brand_display_promo_global') THEN 'realweb_ya_2022_ios_cpi_reg1_brand_display_promo_global'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_ya_2022_ios_cpi_nn_brand_display_promo_global') THEN 'realweb_ya_2022_ios_cpi_nn_brand_display_promo_global'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_ya_2022_ios_cpi_mskspb_brand_display_promo_global') THEN 'realweb_ya_2022_ios_cpi_mskspb_brand_display_promo_global'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_ya_2022_and_cpi_reg1_brand_display_promo_global') THEN 'realweb_ya_2022_and_cpi_reg1_brand_display_promo_global'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_ya_2022_and_cpi_nn_brand_display_promo_global') THEN 'realweb_ya_2022_and_cpi_nn_brand_display_promo_global'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_ya_2022_and_cpi_mskspb_brand_display_promo_global') THEN 'realweb_ya_2022_and_cpi_mskspb_brand_display_promo_global'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_ya_2021_and_cpi_rf_general_display_install_promo_regular') THEN 'realweb_ya_2021_and_cpi_rf_general_display_install_promo_regular'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_vk_2022_and_cpo_rf_old_ret_promo_global_cyber_monday') THEN 'realweb_vk_2022_and_cpo_rf_old_ret_promo_global_cyber_monday'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_tiktok_2022_mskspb_and_new_promo_global') THEN 'realweb_tiktok_2022_mskspb_and_new_promo_global'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_mt_2021 [p:and] [cpa] [mskspb] [old] [minusfrequency2] [promo_global]') THEN 'realweb_mt_2021 [p:and] [cpa] [mskspb] [old] [minusfrequency2] [promo_global]'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_fb_2021 [p:ios] [g:msk] [cpo] [feed] [old] [general] [darkstore] [ng]') THEN 'realweb_fb_2021 [p:ios] [g:msk] [cpo] [feed] [old] [general] [darkstore] [ng]'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_fb_2021 [p:ios] [g:msk] [cpo] [feed] [new] [general] [darkstore] [ng]') THEN 'realweb_fb_2021 [p:ios] [g:msk] [cpo] [feed] [new] [general] [darkstore] [ng]'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_fb_2021 [p:ios] [cpo] [feed] [discount] [old] [general] [darkstore] [forder] [i14]') THEN 'realweb_fb_2021 [p:ios] [cpo] [feed] [discount] [old] [general] [darkstore] [forder] [i14]'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_fb_2021 [p:ios] [cpi] [feed] [discount] [new] [general] [darkstore] [install] [i14]') THEN 'realweb_fb_2021 [p:ios] [cpi] [feed] [discount] [new] [general] [darkstore] [install] [i14]'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_fb_2021 [p:ios] [cpa] [feed] [discount] [new] [general] [darkstore] [forder] [i14]') THEN 'realweb_fb_2021 [p:ios] [cpa] [feed] [discount] [new] [general] [darkstore] [forder] [i14]'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_fb_2021 [p:and] [g:msk] [cpo] [feed] [old] [general] [darkstore] [ng]') THEN 'realweb_fb_2021 [p:and] [g:msk] [cpo] [feed] [old] [general] [darkstore] [ng]'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_fb_2021 [p:and] [g:msk] [cpo] [feed] [new] [general] [darkstore] [ng]') THEN 'realweb_fb_2021 [p:and] [g:msk] [cpo] [feed] [new] [general] [darkstore] [ng]'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_fb_2021 [p:and] [cpo_catalog] [feed] [discount] [old] [general] [darkstore] [forder]') THEN 'realweb_fb_2021 [p:and] [cpo_catalog] [feed] [discount] [old] [general] [darkstore] [forder]'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_fb_2021 [p:and] [cpo] [feed] [discount] [old] [general] [darkstore] [forder]') THEN 'realweb_fb_2021 [p:and] [cpo] [feed] [discount] [old] [general] [darkstore] [forder]'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_fb_2021 [p:and] [cpo] [feed] [discount] [new] [general] [darkstore] [install]') THEN 'realweb_fb_2021 [p:and] [cpo] [feed] [discount] [new] [general] [darkstore] [install]'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_fb_2021 [p:and] [cpi] [feed] [discount] [new] [general] [darkstore] [install]') THEN 'realweb_fb_2021 [p:and] [cpi] [feed] [discount] [new] [general] [darkstore] [install]'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promofeed') THEN 'promofeed'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_zavtrak') THEN 'promo_zavtrak'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_watermelon') THEN 'promo_watermelon'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_vsepo99') THEN 'promo_vsepo99'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_vse_po_49_i_99') THEN 'promo_vse_po_49_i_99'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_tea_coffee') THEN 'promo_tea_coffee'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_tea_and_cofee') THEN 'promo_tea_and_cofee'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_sun') THEN 'promo_sun'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_strawberry') THEN 'promo_strawberry'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_smoothies') THEN 'promo_smoothies'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_school') THEN 'promo_school'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_regular_present_vid') THEN 'promo_regular_present_vid'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_regular_present_2') THEN 'promo_regular_present_2'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_regular_present') THEN 'promo_regular_present'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_plov') THEN 'promo_plov'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_okroshka') THEN 'promo_okroshka'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_nemoloko') THEN 'promo_nemoloko'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_napitok') THEN 'promo_napitok'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_more_lososya') THEN 'promo_more_lososya'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_moloko_parmalat') THEN 'promo_moloko_parmalat'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_meet') THEN 'promo_meet'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_meat_21.07 - 02.08') THEN 'promo_meat_21.07 - 02.08'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_lisichki') THEN 'promo_lisichki'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_linzy') THEN 'promo_linzy'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_limonad') THEN 'promo_limonad'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_lego_december') THEN 'promo_lego_december'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_lego') THEN 'promo_lego'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_kvas') THEN 'promo_kvas'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_kolbasa') THEN 'promo_kolbasa'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_kasha') THEN 'promo_kasha'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_karamel_tokio') THEN 'promo_karamel_tokio'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_icecream') THEN 'promo_icecream'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_ice_cream') THEN 'promo_ice_cream'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_him') THEN 'promo_him'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_grill') THEN 'promo_grill'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_global_dyson_ret') THEN 'promo_global_dyson_ret'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_global_cyber_monday_zewa') THEN 'promo_global_cyber_monday_zewa'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_global_cyber_monday_vid2') THEN 'promo_global_cyber_monday_vid2'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_global_cyber_monday_vid1') THEN 'promo_global_cyber_monday_vid1'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_global_cyber_monday_pepsi') THEN 'promo_global_cyber_monday_pepsi'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_global_cyber_monday_losos') THEN 'promo_global_cyber_monday_losos'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_global_cyber_monday_drink') THEN 'promo_global_cyber_monday_drink'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_global_cyber_monday_25.01') THEN 'promo_global_cyber_monday_25.01'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_global_cyber_monday') THEN 'promo_global_cyber_monday'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_global_auto_carusel_2') THEN 'promo_global_auto_carusel_2'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_global_auto_carusel') THEN 'promo_global_auto_carusel'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_global_auto') THEN 'promo_global_auto'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_fruits_ berries') THEN 'promo_fruits_ berries'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_fish') THEN 'promo_fish'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_festmorozh') THEN 'promo_festmorozh'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_dostavka_dacha') THEN 'promo_dostavka_dacha'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_diapers') THEN 'promo_diapers'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_cosmetics') THEN 'promo_cosmetics'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_coffee') THEN 'promo_coffee'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_choco_ball') THEN 'promo_choco_ball'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_children') THEN 'promo_children'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_cherry') THEN 'promo_cherry'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_cheese_chechil') THEN 'promo_cheese_chechil'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_cheese') THEN 'promo_cheese'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_baton_alpin') THEN 'promo_baton_alpin'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_baton') THEN 'promo_baton'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_aurora') THEN 'promo_aurora'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_arbuz') THEN 'promo_arbuz'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'ng_olivier') THEN 'ng_olivier'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'ng_mandarin') THEN 'ng_mandarin'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'ng_kura') THEN 'ng_kura'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'ng_granata') THEN 'ng_granata'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'fixprice_17.09-27.09') THEN 'fixprice_17.09-27.09'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'express') THEN 'express'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'coffee_illy') THEN 'coffee_illy'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'black_friday') THEN 'black_friday'

    ELSE '-' END

 as promo_search,
        
    CASE
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'minusfrequency2')
        THEN 'All buyers minus frequency 2'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'aud.cart')
        THEN 'Add to cart not buy'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'core_promo_5')
        THEN 'Ядро_Промо+5_fix'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'reg_not_buy')
        THEN 'Registered but not buy'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'purchase15_22')
        THEN 'Предотток'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'purchase23_73')
        THEN 'Отток'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'purchase74_130')
        OR REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'purchase_aft130')
        THEN 'Глубокий отток'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'notused_30d')
        THEN 'Не использовали приложение более 30 дней'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'purch_less_2')
        THEN 'Покупают реже, чем 2 раза в месяц'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'purch_from_2')
        THEN 'Покупают от 2 раз в месяц'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'deep_outflow_rtg')
        THEN 'Deep_outflow_rtg'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'first_open_.ot_buy_rtg')
        THEN 'First_open_not_buy_rtg'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'installed_the_app_but_not_buy_rtg')
        THEN 'Installed_the_app_but_not_buy_rtg'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'registered_but_not_buy_rtg')
        THEN 'Registered_but_not_buy_rtg'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'firebase')
        THEN  "Предиктивная аудитория" 
        WHEN REGEXP_CONTAINS(LOWER(campaign_name), 'inapp')
        THEN 'Аудитории площадки (inapp)'
        ELSE 'Нет аудитории' END
 AS auditory,
        'ios' as platform,
        -- SUM(impressions),
        -- SUM(clicks),
        SUM(spend) AS spend
    FROM `perekrestokvprok-bq`.`dbt_production`.`stg_asa_cab_sheets`
    --`perekrestokvprok-bq`.`dbt_production`.`int_asa_cab_meta`
    WHERE campaign_type = 'retargeting'
    GROUP BY 1,2,3,4,5,6
),

asa_convs AS (
    SELECT 
        date,
        campaign_name,
        platform,
        promo_type,
        promo_search,
        auditory,
        SUM(IF(event_name = "af_purchase", event_revenue, 0)) AS revenue,
        SUM(IF(event_name = "af_purchase", event_count, 0)) AS purchase,
        SUM(IF(event_name = 're-engagement', event_count, 0)) AS re_engagement
    FROM af_conversions
    WHERE REGEXP_CONTAINS(campaign_name, r'\(r\)')
    GROUP BY 1,2,3,4,5,6
),

asa AS (
    SELECT
        COALESCE(asa_convs.date, asa_cost.date) AS date,
        COALESCE(asa_convs.campaign_name, asa_cost.campaign_name) AS campaign_name,
        COALESCE(asa_convs.platform, asa_cost.platform) AS platform,
        COALESCE(asa_convs.promo_type, asa_cost.promo_type) AS promo_type,
        COALESCE(asa_convs.promo_search, asa_cost.promo_search) AS promo_search,
        COALESCE(asa_convs.auditory, asa_cost.auditory) AS auditory,
        COALESCE(re_engagement,0) AS re_engagement,
        COALESCE(revenue,0) AS revenue,
        COALESCE(purchase,0) AS purchase,
        COALESCE(spend,0) AS spend,
        'Apple Search Ads' AS source,
    FROM asa_convs
    FULL OUTER JOIN asa_cost
    ON asa_convs.date = asa_cost.date 
    AND asa_convs.campaign_name = asa_cost.campaign_name
    AND asa_convs.promo_type = asa_cost.promo_type
    AND asa_convs.promo_search = asa_cost.promo_search
    AND asa_convs.auditory = asa_cost.auditory
    WHERE COALESCE(re_engagement,0) + COALESCE(revenue,0) + COALESCE(purchase,0) + COALESCE(spend,0) > 0
    AND COALESCE(asa_convs.campaign_name, asa_cost.campaign_name) != 'None'
),

----------------------- Google Ads -------------------------

google_cost AS (
    SELECT
        date,
        campaign_name,
        
    CASE
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name],'')), r'promo.*regular') THEN 'promo regular'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name],'')), r'promo.*global') THEN 'promo global'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name],'')), r'promo.*feed') THEN 'promo feed'
    ELSE '-' END
 as promo_type,
        
    CASE
        WHEN REGEXP_CONTAINS(LOWER(campaign_name), r'\[p:ios\]|_ios_|p02') THEN 'ios'
        WHEN REGEXP_CONTAINS(LOWER(campaign_name), r'\[p:and\]|_and_|android|p01') THEN 'android'
    ELSE 'no_platform' END
 as platform,
        CASE
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'yagoda_14.09-28.09') THEN 'yagoda_14.09-28.09'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'tovar_rub_13.09-19.09') THEN 'tovar_rub_13.09-19.09'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'season_orange') THEN 'season_orange'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'season_mandarin') THEN 'season_mandarin'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'season_lemon') THEN 'season_lemon'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_ya_2022_ios_cpi_reg1_brand_display_promo_global') THEN 'realweb_ya_2022_ios_cpi_reg1_brand_display_promo_global'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_ya_2022_ios_cpi_nn_brand_display_promo_global') THEN 'realweb_ya_2022_ios_cpi_nn_brand_display_promo_global'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_ya_2022_ios_cpi_mskspb_brand_display_promo_global') THEN 'realweb_ya_2022_ios_cpi_mskspb_brand_display_promo_global'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_ya_2022_and_cpi_reg1_brand_display_promo_global') THEN 'realweb_ya_2022_and_cpi_reg1_brand_display_promo_global'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_ya_2022_and_cpi_nn_brand_display_promo_global') THEN 'realweb_ya_2022_and_cpi_nn_brand_display_promo_global'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_ya_2022_and_cpi_mskspb_brand_display_promo_global') THEN 'realweb_ya_2022_and_cpi_mskspb_brand_display_promo_global'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_ya_2021_and_cpi_rf_general_display_install_promo_regular') THEN 'realweb_ya_2021_and_cpi_rf_general_display_install_promo_regular'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_vk_2022_and_cpo_rf_old_ret_promo_global_cyber_monday') THEN 'realweb_vk_2022_and_cpo_rf_old_ret_promo_global_cyber_monday'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_tiktok_2022_mskspb_and_new_promo_global') THEN 'realweb_tiktok_2022_mskspb_and_new_promo_global'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_mt_2021 [p:and] [cpa] [mskspb] [old] [minusfrequency2] [promo_global]') THEN 'realweb_mt_2021 [p:and] [cpa] [mskspb] [old] [minusfrequency2] [promo_global]'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_fb_2021 [p:ios] [g:msk] [cpo] [feed] [old] [general] [darkstore] [ng]') THEN 'realweb_fb_2021 [p:ios] [g:msk] [cpo] [feed] [old] [general] [darkstore] [ng]'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_fb_2021 [p:ios] [g:msk] [cpo] [feed] [new] [general] [darkstore] [ng]') THEN 'realweb_fb_2021 [p:ios] [g:msk] [cpo] [feed] [new] [general] [darkstore] [ng]'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_fb_2021 [p:ios] [cpo] [feed] [discount] [old] [general] [darkstore] [forder] [i14]') THEN 'realweb_fb_2021 [p:ios] [cpo] [feed] [discount] [old] [general] [darkstore] [forder] [i14]'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_fb_2021 [p:ios] [cpi] [feed] [discount] [new] [general] [darkstore] [install] [i14]') THEN 'realweb_fb_2021 [p:ios] [cpi] [feed] [discount] [new] [general] [darkstore] [install] [i14]'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_fb_2021 [p:ios] [cpa] [feed] [discount] [new] [general] [darkstore] [forder] [i14]') THEN 'realweb_fb_2021 [p:ios] [cpa] [feed] [discount] [new] [general] [darkstore] [forder] [i14]'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_fb_2021 [p:and] [g:msk] [cpo] [feed] [old] [general] [darkstore] [ng]') THEN 'realweb_fb_2021 [p:and] [g:msk] [cpo] [feed] [old] [general] [darkstore] [ng]'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_fb_2021 [p:and] [g:msk] [cpo] [feed] [new] [general] [darkstore] [ng]') THEN 'realweb_fb_2021 [p:and] [g:msk] [cpo] [feed] [new] [general] [darkstore] [ng]'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_fb_2021 [p:and] [cpo_catalog] [feed] [discount] [old] [general] [darkstore] [forder]') THEN 'realweb_fb_2021 [p:and] [cpo_catalog] [feed] [discount] [old] [general] [darkstore] [forder]'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_fb_2021 [p:and] [cpo] [feed] [discount] [old] [general] [darkstore] [forder]') THEN 'realweb_fb_2021 [p:and] [cpo] [feed] [discount] [old] [general] [darkstore] [forder]'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_fb_2021 [p:and] [cpo] [feed] [discount] [new] [general] [darkstore] [install]') THEN 'realweb_fb_2021 [p:and] [cpo] [feed] [discount] [new] [general] [darkstore] [install]'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'realweb_fb_2021 [p:and] [cpi] [feed] [discount] [new] [general] [darkstore] [install]') THEN 'realweb_fb_2021 [p:and] [cpi] [feed] [discount] [new] [general] [darkstore] [install]'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promofeed') THEN 'promofeed'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_zavtrak') THEN 'promo_zavtrak'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_watermelon') THEN 'promo_watermelon'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_vsepo99') THEN 'promo_vsepo99'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_vse_po_49_i_99') THEN 'promo_vse_po_49_i_99'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_tea_coffee') THEN 'promo_tea_coffee'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_tea_and_cofee') THEN 'promo_tea_and_cofee'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_sun') THEN 'promo_sun'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_strawberry') THEN 'promo_strawberry'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_smoothies') THEN 'promo_smoothies'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_school') THEN 'promo_school'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_regular_present_vid') THEN 'promo_regular_present_vid'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_regular_present_2') THEN 'promo_regular_present_2'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_regular_present') THEN 'promo_regular_present'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_plov') THEN 'promo_plov'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_okroshka') THEN 'promo_okroshka'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_nemoloko') THEN 'promo_nemoloko'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_napitok') THEN 'promo_napitok'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_more_lososya') THEN 'promo_more_lososya'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_moloko_parmalat') THEN 'promo_moloko_parmalat'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_meet') THEN 'promo_meet'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_meat_21.07 - 02.08') THEN 'promo_meat_21.07 - 02.08'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_lisichki') THEN 'promo_lisichki'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_linzy') THEN 'promo_linzy'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_limonad') THEN 'promo_limonad'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_lego_december') THEN 'promo_lego_december'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_lego') THEN 'promo_lego'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_kvas') THEN 'promo_kvas'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_kolbasa') THEN 'promo_kolbasa'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_kasha') THEN 'promo_kasha'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_karamel_tokio') THEN 'promo_karamel_tokio'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_icecream') THEN 'promo_icecream'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_ice_cream') THEN 'promo_ice_cream'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_him') THEN 'promo_him'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_grill') THEN 'promo_grill'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_global_dyson_ret') THEN 'promo_global_dyson_ret'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_global_cyber_monday_zewa') THEN 'promo_global_cyber_monday_zewa'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_global_cyber_monday_vid2') THEN 'promo_global_cyber_monday_vid2'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_global_cyber_monday_vid1') THEN 'promo_global_cyber_monday_vid1'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_global_cyber_monday_pepsi') THEN 'promo_global_cyber_monday_pepsi'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_global_cyber_monday_losos') THEN 'promo_global_cyber_monday_losos'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_global_cyber_monday_drink') THEN 'promo_global_cyber_monday_drink'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_global_cyber_monday_25.01') THEN 'promo_global_cyber_monday_25.01'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_global_cyber_monday') THEN 'promo_global_cyber_monday'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_global_auto_carusel_2') THEN 'promo_global_auto_carusel_2'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_global_auto_carusel') THEN 'promo_global_auto_carusel'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_global_auto') THEN 'promo_global_auto'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_fruits_ berries') THEN 'promo_fruits_ berries'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_fish') THEN 'promo_fish'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_festmorozh') THEN 'promo_festmorozh'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_dostavka_dacha') THEN 'promo_dostavka_dacha'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_diapers') THEN 'promo_diapers'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_cosmetics') THEN 'promo_cosmetics'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_coffee') THEN 'promo_coffee'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_choco_ball') THEN 'promo_choco_ball'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_children') THEN 'promo_children'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_cherry') THEN 'promo_cherry'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_cheese_chechil') THEN 'promo_cheese_chechil'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_cheese') THEN 'promo_cheese'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_baton_alpin') THEN 'promo_baton_alpin'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_baton') THEN 'promo_baton'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_aurora') THEN 'promo_aurora'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'promo_arbuz') THEN 'promo_arbuz'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'ng_olivier') THEN 'ng_olivier'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'ng_mandarin') THEN 'ng_mandarin'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'ng_kura') THEN 'ng_kura'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'ng_granata') THEN 'ng_granata'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'fixprice_17.09-27.09') THEN 'fixprice_17.09-27.09'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'express') THEN 'express'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'coffee_illy') THEN 'coffee_illy'

        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name, "-"], ' ')), r'black_friday') THEN 'black_friday'

    ELSE '-' END

 as promo_search,
        
    CASE
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'minusfrequency2')
        THEN 'All buyers minus frequency 2'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'aud.cart')
        THEN 'Add to cart not buy'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'core_promo_5')
        THEN 'Ядро_Промо+5_fix'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'reg_not_buy')
        THEN 'Registered but not buy'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'purchase15_22')
        THEN 'Предотток'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'purchase23_73')
        THEN 'Отток'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'purchase74_130')
        OR REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'purchase_aft130')
        THEN 'Глубокий отток'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'notused_30d')
        THEN 'Не использовали приложение более 30 дней'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'purch_less_2')
        THEN 'Покупают реже, чем 2 раза в месяц'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'purch_from_2')
        THEN 'Покупают от 2 раз в месяц'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'deep_outflow_rtg')
        THEN 'Deep_outflow_rtg'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'first_open_.ot_buy_rtg')
        THEN 'First_open_not_buy_rtg'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'installed_the_app_but_not_buy_rtg')
        THEN 'Installed_the_app_but_not_buy_rtg'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'registered_but_not_buy_rtg')
        THEN 'Registered_but_not_buy_rtg'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), 'firebase')
        THEN  "Предиктивная аудитория" 
        WHEN REGEXP_CONTAINS(LOWER(campaign_name), 'inapp')
        THEN 'Аудитории площадки (inapp)'
        ELSE 'Нет аудитории' END
 AS auditory,
        -- SUM(impressions),
        -- SUM(clicks),
        SUM(spend) AS spend,
        SUM(installs) AS re_engagement
    FROM `perekrestokvprok-bq`.`dbt_production`.`stg_google_cab_sheets`
    WHERE (campaign_type = 'retargeting'
    --- костыль 10.02.2022 X5RGPEREK-272 ---
    OR campaign_name IN (
            'realweb_uac_2022 [p:and] [cpi] [mskspb] [new] [general] [darkstore] [purchase] [firebase]',
            'realweb_uac_2022 [p:and] [cpi] [reg1] [new] [general] [darkstore] [purchase] [firebase]'))
    AND REGEXP_CONTAINS(campaign_name, r'realweb_|ohm')
    GROUP BY 1,2,3,4,5,6
),

google_convs AS (
    SELECT 
        date,
        campaign_name,
        platform,
        promo_type,
        promo_search,
        auditory,
        SUM(IF(event_name = "af_purchase", event_revenue, 0)) AS revenue,
        SUM(IF(event_name = "af_purchase", event_count, 0)) AS purchase,
    FROM af_conversions
    WHERE mediasource ='googleadwords_int'
    AND is_retargeting = TRUE
    AND REGEXP_CONTAINS(campaign_name, r'realweb|ohm')
    AND (REGEXP_CONTAINS(campaign_name,  r'[_\[]old[\]_]')
    OR campaign_name IN (
            'realweb_uac_2022 [p:and] [cpi] [mskspb] [new] [general] [darkstore] [purchase] [firebase]',
            'realweb_uac_2022 [p:and] [cpi] [reg1] [new] [general] [darkstore] [purchase] [firebase]'))
    GROUP BY 1,2,3,4,5,6
),

google AS (
    SELECT
        COALESCE(google_convs.date, google_cost.date) AS date,
        COALESCE(google_convs.campaign_name, google_cost.campaign_name) AS campaign_name,
        COALESCE(google_convs.platform, google_cost.platform) AS platform,
        COALESCE(google_convs.promo_type, google_cost.promo_type) AS promo_type,
        COALESCE(google_convs.promo_search, google_cost.promo_search) AS promo_search,
        COALESCE(google_convs.auditory, google_cost.auditory) AS auditory,
        COALESCE(re_engagement,0) AS re_engagement,
        COALESCE(revenue,0) AS revenue,
        COALESCE(purchase,0) AS purchase,
        COALESCE(spend,0) AS spend,
        'Google Ads' AS source,
    FROM google_convs
    FULL OUTER JOIN google_cost
    ON google_convs.date = google_cost.date 
    AND google_convs.campaign_name = google_cost.campaign_name
    AND google_convs.promo_type = google_cost.promo_type
    AND google_convs.promo_search = google_cost.promo_search
    AND google_convs.auditory = google_cost.auditory
    WHERE COALESCE(re_engagement,0) + COALESCE(revenue,0) + COALESCE(purchase,0) + COALESCE(spend,0) > 0
    AND COALESCE(google_convs.campaign_name, google_cost.campaign_name) != 'None'
),

----------------------- inapp -------------------------

rate AS (
    SELECT
        start_date,
        end_date,
        partner,
        platform,
        rate_for_us
FROM `perekrestokvprok-bq`.`dbt_production`.`stg_rate_info`
WHERE type = 'RTG'
),

limits_table AS (
    SELECT
        start_date,
        end_date,
        partner,
        limits
    FROM `perekrestokvprok-bq`.`dbt_production`.`stg_partner_limits`
    WHERE type = 'RTG'
),

inapp_events_without_cumulation AS (
    SELECT DISTINCT
        date,
        campaign_name,
        platform,
        
    CASE 
        WHEN REGEXP_CONTAINS(campaign_name, r'_ms_') THEN 'Mobisharks'
        WHEN REGEXP_CONTAINS(campaign_name, r'_tl_') THEN '2leads'
        WHEN REGEXP_CONTAINS(campaign_name, r'_mx_') THEN 'MobX'
        WHEN REGEXP_CONTAINS(campaign_name, r'_sw_') THEN 'SW'
        WHEN REGEXP_CONTAINS(campaign_name, r'_tm_') THEN 'Think Mobile'
        WHEN REGEXP_CONTAINS(campaign_name, r'_abc_|_sf_') THEN 'Mediasurfer'
    ELSE '-' END
 AS partner,
        promo_type,
        promo_search,
        auditory,
        event_name,
        event_revenue,
        event_count,
        SUM(event_count) 
            OVER(PARTITION BY DATE_TRUNC(date, MONTH), event_name ORDER BY date, event_revenue)
            AS cum_event_count
        FROM af_conversions
        WHERE REGEXP_CONTAINS(campaign_name, r'inapp')
        AND is_retargeting = TRUE
),

inapp_events AS (
    SELECT
        date,
        campaign_name,
        platform,
        partner,
        promo_type,
        promo_search,
        auditory,
        event_name,
        event_revenue,
        event_count,
        cum_event_count,
        SUM(event_count) 
            OVER(PARTITION BY DATE_TRUNC(date, MONTH), event_name, partner ORDER BY date, event_revenue)
            AS cum_event_count_by_prt
    FROM inapp_events_without_cumulation
),

inapp AS (
    SELECT
        date,
        campaign_name,
        inapp_events.platform,
        promo_type,
        promo_search,
        auditory,
        SUM(IF(event_name = 're-engagement', event_count, 0)) AS re_engagement,
        SUM(IF(event_name = "af_purchase" and cum_event_count_by_prt <= COALESCE(limits, 1000000), event_revenue, 0)) AS revenue,
        SUM(IF(event_name = "af_purchase" and cum_event_count_by_prt <= COALESCE(limits, 1000000), event_count, 0)) AS purchase,
        SUM(
            CASE
                WHEN event_name = 'af_purchase' 
                    AND date BETWEEN '2021-10-01' AND '2021-10-31'
                    AND cum_event_count >= 3000 THEN event_count * 140
                WHEN event_name = 'af_purchase' 
                    AND date BETWEEN '2021-10-01' AND '2021-10-31'
                    AND cum_event_count < 3000 THEN event_count * rate_for_us
                WHEN event_name = 'af_purchase' 
                    AND date NOT BETWEEN '2021-10-01' AND '2021-10-31'
                    AND cum_event_count_by_prt <= COALESCE(limits, 1000000)
                    THEN event_count * rate_for_us
                ELSE 0 END
            ) AS spend,
        'inapp' AS source
    FROM inapp_events
    LEFT JOIN rate
    ON inapp_events.partner = rate.partner 
    AND inapp_events.date BETWEEN rate.start_date AND rate.end_date
    AND inapp_events.platform = rate.platform
    LEFT JOIN limits_table
    ON inapp_events.partner = limits_table.partner 
    AND inapp_events.date BETWEEN limits_table.start_date AND limits_table.end_date
    GROUP BY 1,2,3,4,5,6
),

final AS (
    SELECT * FROM yandex
    UNION ALL 
    SELECT * FROM vk
    UNION ALL 
    SELECT * FROM mt
    UNION ALL 
    SELECT * FROM tw
    UNION ALL 
    SELECT * FROM tiktok
    UNION ALL
    SELECT * FROM asa
    UNION ALL
    SELECT * FROM facebook
    UNION ALL
    SELECT * FROM inapp
    UNION ALL
    SELECT * FROM google
)

SELECT 
    date,
    campaign_name,
    platform,
    promo_type,
    promo_search,
    auditory,
    re_engagement,
    revenue,
    purchase,
    spend,
    source,
    
    CASE
          WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-"], ' ')), r'msk+spb|mskspb|msk_spb') THEN 'МСК, СПб'
          WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-"], ' ')), r'spb') THEN 'СПб'
          WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-"], ' ')), r'msk') THEN 'МСК'
          WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-"], ' ')), r'nn') THEN 'НН'
          WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-"], ' ')), r'reg1') THEN 'Регионы с доставкой'
          WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-"], ' ')), r'reg2|rostov|kzn|g_all')THEN 'Регионы без доставки'
        ELSE 'Россия' END
 AS geo
FROM final