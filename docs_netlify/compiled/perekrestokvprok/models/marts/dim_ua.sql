/* 
для лучшего понимания лучше заглянуть сюда: https://github.com/realweb-msk/perekrestokvprok-dbt
или сюда: https://brave-hermann-395dc3.netlify.app/#!/model/model.perekrestokvprok.dim_ua
*/


WITH af_conversions AS (
    SELECT
        date,
        is_retargeting,
        af_cid,
        --adset_name,
        
    CASE
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name],'')), r'promo.*regular') THEN 'promo regular'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name],'')), r'promo.*global') THEN 'promo global'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name],'')), r'promo.*feed') THEN 'promo feed'
    ELSE '-' END
 as promo_type,
        
    CASE
          WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), r'msk+spb|mskspb|msk_spb') THEN 'МСК, СПб'
          WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), r'spb') THEN 'СПб'
          WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), r'msk') THEN 'МСК'
          WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), r'_nn_|g:nn|\[nn\]') THEN 'НН'
          WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), r'reg1') THEN 'Регионы с доставкой'
          WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), r'reg2|rostov|kzn|g_all')THEN 'Регионы без доставки'
        ELSE 'Россия' END
 AS geo,
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
        mediasource,
        platform,
        event_name,
        uniq_event_count,
        event_revenue,
        event_count,
        campaign_name
    FROM  `perekrestokvprok-bq`.`dbt_production`.`stg_af_client_data`
    -- WHERE is_retargeting = FALSE
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
          WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), r'msk+spb|mskspb|msk_spb') THEN 'МСК, СПб'
          WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), r'spb') THEN 'СПб'
          WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), r'msk') THEN 'МСК'
          WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), r'_nn_|g:nn|\[nn\]') THEN 'НН'
          WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), r'reg1') THEN 'Регионы с доставкой'
          WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), r'reg2|rostov|kzn|g_all')THEN 'Регионы без доставки'
        ELSE 'Россия' END
 AS geo,
        campaign_type,
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
        SUM(impressions) AS impressions,
        SUM(clicks) AS clicks,
        SUM(IF(campaign_type = 'UA', installs, 0)) AS installs,
        SUM(revenue) AS revenue,
        SUM(purchase) AS purchase,
        SUM(purchase) AS uniq_purchase,
        SUM(first_purchase_revenue) AS first_purchase_revenue,
        SUM(first_purchase) AS first_purchase,
        SUM(first_purchase) AS uniq_first_purchase,
        SUM(IF(campaign_type = 'UA', spend, 0)) AS spend,
        'Facebook' AS source,
        "social" as adv_type
    FROM `perekrestokvprok-bq`.`dbt_production`.`stg_facebook_cab_sheets`
    --`perekrestokvprok-bq`.`dbt_production`.`stg_facebook_cab_meta`
    GROUP BY 1,2,3,4,5,6,7
),

----------------------- yandex -------------------------

yandex_cost AS (
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
          WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), r'msk+spb|mskspb|msk_spb') THEN 'МСК, СПб'
          WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), r'spb') THEN 'СПб'
          WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), r'msk') THEN 'МСК'
          WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), r'_nn_|g:nn|\[nn\]') THEN 'НН'
          WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), r'reg1') THEN 'Регионы с доставкой'
          WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), r'reg2|rostov|kzn|g_all')THEN 'Регионы без доставки'
        ELSE 'Россия' END
 AS geo,
        campaign_type,
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
        SUM(impressions) AS impressions,
        SUM(clicks) AS clicks,
        SUM(spend) AS spend
    FROM `perekrestokvprok-bq`.`dbt_production`.`int_yandex_cab_meta`
    WHERE campaign_type = 'UA'
    AND REGEXP_CONTAINS(campaign_name, r'realweb')
    GROUP BY 1,2,3,4,5,6,7
),

yandex_convs_ua AS (
    SELECT 
        date,
        campaign_name,
        platform,
        promo_type,
        geo,
        'UA' as campaign_type,
        promo_search,
        SUM(IF(event_name = 'install', event_count,0)) AS installs,
        SUM(IF(event_name = 'first_purchase', event_revenue,0)) AS first_purchase_revenue,
        SUM(IF(event_name = 'first_purchase',event_count, 0)) AS first_purchase,
        SUM(IF(event_name = 'first_purchase', uniq_event_count, 0)) AS uniq_first_purchase,
        SUM(IF(event_name = "af_purchase", event_revenue, 0)) AS revenue,
        SUM(IF(event_name = "af_purchase", event_count, 0)) AS purchase,
        SUM(IF(event_name = "af_purchase", uniq_event_count, 0)) AS uniq_purchase,
    FROM af_conversions
    WHERE is_retargeting = FALSE
    AND REGEXP_CONTAINS(campaign_name, r'realweb_ya')
    AND NOT REGEXP_CONTAINS(campaign_name, r'_ret_|[_\[]old[_\]]')
    GROUP BY 1,2,3,4,5,6,7
),

yandex_convs_rtg AS (
    SELECT 
        date,
        campaign_name,
        platform,
        promo_type,
        geo,
        'retargeting' AS campaign_type,
        promo_search,
        -- информация по покупкам в рет кампаниях должна быть в дашборде UA
        SUM(IF(event_name = 'install', 0,0)) AS installs,
        SUM(IF(event_name = 'first_purchase', event_revenue,0)) AS first_purchase_revenue,
        SUM(IF(event_name = 'first_purchase',event_count, 0)) AS first_purchase,
        SUM(IF(event_name = 'first_purchase', uniq_event_count, 0)) AS uniq_first_purchase,
        SUM(IF(event_name = "af_purchase", event_revenue, 0)) AS revenue,
        SUM(IF(event_name = "af_purchase", event_count, 0)) AS purchase,
        SUM(IF(event_name = "af_purchase", uniq_event_count, 0)) AS uniq_purchase,
    FROM af_conversions
    WHERE is_retargeting = TRUE --? 
    AND REGEXP_CONTAINS(campaign_name, r'realweb_ya')
    AND REGEXP_CONTAINS(campaign_name, r'_ret_|[_\[]old[_\]]')
    GROUP BY 1,2,3,4,5,6,7
),

yandex_convs AS (
    SELECT * FROM yandex_convs_ua
    UNION ALL 
    SELECT * FROM yandex_convs_rtg
),

yandex AS (
    SELECT
        COALESCE(yandex_convs.date, yandex_cost.date) AS date,
        COALESCE(yandex_convs.campaign_name, yandex_cost.campaign_name) AS campaign_name,
        COALESCE(yandex_convs.platform, yandex_cost.platform) AS platform,
        COALESCE(yandex_convs.promo_type, yandex_cost.promo_type) AS promo_type,
        COALESCE(yandex_convs.geo, yandex_cost.geo) AS geo,
        COALESCE(yandex_convs.campaign_type, yandex_cost.campaign_type) AS campaign_type,
        COALESCE(yandex_convs.promo_search, yandex_cost.promo_search) AS promo_search,
        COALESCE(impressions,0) AS impressions,
        COALESCE(clicks,0) AS clicks,
        COALESCE(installs,0) AS installs,
        COALESCE(revenue,0) AS revenue,
        COALESCE(purchase,0) AS purchase,
        COALESCE(uniq_purchase,0) AS uniq_purchase,
        COALESCE(first_purchase_revenue,0) AS first_purchase_revenue,
        COALESCE(first_purchase,0) AS first_purchase,
        COALESCE(uniq_first_purchase,0) AS uniq_first_purchase,
        COALESCE(spend,0) AS spend,
        'Яндекс.Директ' AS source,
        'context' AS adv_type
    FROM yandex_convs
    FULL OUTER JOIN yandex_cost
    ON yandex_convs.date = yandex_cost.date 
    AND yandex_convs.campaign_name = yandex_cost.campaign_name
    AND yandex_convs.promo_type = yandex_cost.promo_type
    AND yandex_convs.geo = yandex_cost.geo
    AND yandex_convs.promo_search = yandex_cost.promo_search
    WHERE 
        COALESCE(installs,0) + 
        COALESCE(revenue,0) + 
        COALESCE(purchase,0) + 
        COALESCE(uniq_purchase,0) +
        COALESCE(first_purchase_revenue,0) +
        COALESCE(first_purchase,0) + 
        COALESCE(uniq_first_purchase,0) +
        COALESCE(spend,0) > 0
    AND COALESCE(yandex_convs.campaign_name, yandex_cost.campaign_name) != 'None'
),

----------------------- mytarget -------------------------

mt_cost AS (
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
          WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), r'msk+spb|mskspb|msk_spb') THEN 'МСК, СПб'
          WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), r'spb') THEN 'СПб'
          WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), r'msk') THEN 'МСК'
          WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), r'_nn_|g:nn|\[nn\]') THEN 'НН'
          WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), r'reg1') THEN 'Регионы с доставкой'
          WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), r'reg2|rostov|kzn|g_all')THEN 'Регионы без доставки'
        ELSE 'Россия' END
 AS geo,
        campaign_type,
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
        SUM(impressions) AS impressions,
        SUM(clicks) AS clicks,
        SUM(spend) AS spend
    FROM `perekrestokvprok-bq`.`dbt_production`.`int_mytarget_cab_meta`
    WHERE campaign_type = 'UA'
    AND REGEXP_CONTAINS(campaign_name, r'realweb')
    GROUP BY 1,2,3,4,5,6,7
),

mt_convs AS (
    SELECT 
        date,
        campaign_name,
        platform,
        promo_type,
        geo,
        'UA' as campaign_type,
        promo_search,
        SUM(IF(event_name = 'install', event_count,0)) AS installs,
        SUM(IF(event_name = 'first_purchase', event_revenue,0)) AS first_purchase_revenue,
        SUM(IF(event_name = 'first_purchase',event_count, 0)) AS first_purchase,
        SUM(IF(event_name = 'first_purchase', uniq_event_count, 0)) AS uniq_first_purchase,
        SUM(IF(event_name = "af_purchase", event_revenue, 0)) AS revenue,
        SUM(IF(event_name = "af_purchase", event_count, 0)) AS purchase,
        SUM(IF(event_name = "af_purchase", uniq_event_count, 0)) AS uniq_purchase,
    FROM af_conversions
    WHERE is_retargeting = FALSE
    AND REGEXP_CONTAINS(campaign_name, r'realweb_mt')
    AND REGEXP_CONTAINS(campaign_name, r'new')
    GROUP BY 1,2,3,4,5,6,7
),

mt AS (
    SELECT
        COALESCE(mt_convs.date, mt_cost.date) AS date,
        COALESCE(mt_convs.campaign_name, mt_cost.campaign_name) AS campaign_name,
        COALESCE(mt_convs.platform, mt_cost.platform) AS platform,
        COALESCE(mt_convs.promo_type, mt_cost.promo_type) AS promo_type,
        COALESCE(mt_convs.geo, mt_cost.geo) AS geo,
        COALESCE(mt_convs.campaign_type, mt_cost.campaign_type) AS campaign_type,
        COALESCE(mt_convs.promo_search, mt_cost.promo_search) AS promo_search,
        COALESCE(impressions,0) AS impressions,
        COALESCE(clicks,0) AS clicks,
        COALESCE(installs,0) AS installs,
        COALESCE(revenue,0) AS revenue,
        COALESCE(purchase,0) AS purchase,
        COALESCE(uniq_purchase,0) AS uniq_purchase,
        COALESCE(first_purchase_revenue,0) AS first_purchase_revenue,
        COALESCE(first_purchase,0) AS first_purchase,
        COALESCE(uniq_first_purchase,0) AS uniq_first_purchase,
        COALESCE(spend,0) AS spend,
        'MyTarget' AS source,
        'social' AS adv_type
    FROM mt_convs
    FULL OUTER JOIN mt_cost
    ON mt_convs.date = mt_cost.date 
    AND mt_convs.campaign_name = mt_cost.campaign_name
    AND mt_convs.promo_type = mt_cost.promo_type
    AND mt_convs.geo = mt_cost.geo
    AND mt_convs.promo_search = mt_cost.promo_search
    WHERE 
        COALESCE(installs,0) + 
        COALESCE(revenue,0) + 
        COALESCE(purchase,0) + 
        COALESCE(uniq_purchase,0) +
        COALESCE(first_purchase_revenue,0) +
        COALESCE(first_purchase,0) + 
        COALESCE(uniq_first_purchase,0) +
        COALESCE(spend,0) > 0
    AND COALESCE(mt_convs.campaign_name, mt_cost.campaign_name) != 'None'
),

----------------------- tiktok -------------------------

tiktok_cost AS (
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
          WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), r'msk+spb|mskspb|msk_spb') THEN 'МСК, СПб'
          WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), r'spb') THEN 'СПб'
          WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), r'msk') THEN 'МСК'
          WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), r'_nn_|g:nn|\[nn\]') THEN 'НН'
          WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), r'reg1') THEN 'Регионы с доставкой'
          WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), r'reg2|rostov|kzn|g_all')THEN 'Регионы без доставки'
        ELSE 'Россия' END
 AS geo,
        campaign_type,
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
        -- информация по покупкам в рет кампаниях должна быть в дашборде UA
        SUM(IF(campaign_type = 'UA',impressions,0)) AS impressions,
        SUM(IF(campaign_type = 'UA',clicks,0)) AS clicks,
        SUM(IF(campaign_type = 'UA',spend,0)) AS spend,
        SUM(purchase) AS purchase,
        SUM(first_purchase) AS first_purchase,
        SUM(SAFE_CAST(app_install AS INT64)) AS app_install
    FROM `perekrestokvprok-bq`.`dbt_production`.`stg_tiktok_cab_meta`
    WHERE REGEXP_CONTAINS(campaign_name, r'realweb')
    GROUP BY 1,2,3,4,5,6,7
),

tiktok_convs AS (
    SELECT  
        date,
        campaign_name,
        platform,
        promo_type,
        geo,
        'UA' as campaign_type,
        promo_search,
        SUM(IF(event_name = 'install', event_count,0)) AS installs,
        SUM(IF(event_name = 'first_purchase', event_revenue,0)) AS first_purchase_revenue,
        --SUM(IF(event_name = 'first_purchase',event_count, 0)) AS first_purchase,
        SUM(IF(event_name = 'first_purchase', uniq_event_count, 0)) AS uniq_first_purchase,
        SUM(IF(event_name = "af_purchase", event_revenue, 0)) AS revenue,
        --SUM(IF(event_name = "af_purchase", event_count, 0)) AS purchase,
        SUM(IF(event_name = "af_purchase", uniq_event_count, 0)) AS uniq_purchase,
    FROM af_conversions
    WHERE is_retargeting = FALSE
    AND REGEXP_CONTAINS(campaign_name, r'realweb_tiktok')
    AND NOT REGEXP_CONTAINS(campaign_name, r'_ret_|[_\[]old[_\]]')
    GROUP BY 1,2,3,4,5,6,7
),

tiktok AS (
    SELECT
        COALESCE(tiktok_convs.date, tiktok_cost.date) AS date,
        COALESCE(tiktok_convs.campaign_name, tiktok_cost.campaign_name) AS campaign_name,
        COALESCE(tiktok_convs.platform, tiktok_cost.platform) AS platform,
        COALESCE(tiktok_convs.promo_type, tiktok_cost.promo_type) AS promo_type,
        COALESCE(tiktok_convs.geo, tiktok_cost.geo) AS geo,
        COALESCE(tiktok_convs.campaign_type, tiktok_cost.campaign_type) AS campaign_type,
        COALESCE(tiktok_convs.promo_search, tiktok_cost.promo_search) AS promo_search,
        COALESCE(impressions,0) AS impressions,
        COALESCE(clicks,0) AS clicks,
        COALESCE(installs, app_install, 0) AS installs,
        COALESCE(revenue,0) AS revenue,
        COALESCE(purchase,0) AS purchase,
        COALESCE(uniq_purchase,0) AS uniq_purchase,
        COALESCE(first_purchase_revenue,0) AS first_purchase_revenue,
        COALESCE(first_purchase,0) AS first_purchase,
        COALESCE(uniq_first_purchase,0) AS uniq_first_purchase,
        COALESCE(spend,0) AS spend,
        'TikTok' AS source,
        'social' AS adv_type
    FROM tiktok_convs
    FULL OUTER JOIN tiktok_cost
    ON tiktok_convs.date = tiktok_cost.date 
    AND tiktok_convs.campaign_name = tiktok_cost.campaign_name
    AND tiktok_convs.promo_type = tiktok_cost.promo_type
    AND tiktok_convs.geo = tiktok_cost.geo
    AND tiktok_convs.promo_search = tiktok_cost.promo_search
    WHERE 
        COALESCE(installs,0) + 
        COALESCE(revenue,0) + 
        COALESCE(purchase,0) + 
        COALESCE(uniq_purchase,0) +
        COALESCE(first_purchase_revenue,0) +
        COALESCE(first_purchase,0) + 
        COALESCE(uniq_first_purchase,0) +
        COALESCE(spend,0) > 0
    AND COALESCE(tiktok_convs.campaign_name, tiktok_cost.campaign_name) != 'None'
),

----------------------- apple search ads -------------------------

asa_cost AS (
    SELECT
        date,
        campaign_name,
        'ios' as platform,
        
    CASE
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name],'')), r'promo.*regular') THEN 'promo regular'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name],'')), r'promo.*global') THEN 'promo global'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name],'')), r'promo.*feed') THEN 'promo feed'
    ELSE '-' END
 as promo_type,
        
    CASE
          WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), r'msk+spb|mskspb|msk_spb') THEN 'МСК, СПб'
          WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), r'spb') THEN 'СПб'
          WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), r'msk') THEN 'МСК'
          WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), r'_nn_|g:nn|\[nn\]') THEN 'НН'
          WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), r'reg1') THEN 'Регионы с доставкой'
          WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), r'reg2|rostov|kzn|g_all')THEN 'Регионы без доставки'
        ELSE 'Россия' END
 AS geo,
        campaign_type,
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
        SUM(meta.impressions) AS impressions,
        SUM(sheet.clicks) AS clicks,
        SUM(sheet.spend) AS spend
    FROM `perekrestokvprok-bq`.`dbt_production`.`stg_asa_cab_sheets` sheet
    --`perekrestokvprok-bq`.`dbt_production`.`int_asa_cab_meta`
    LEFT JOIN `perekrestokvprok-bq`.`dbt_production`.`int_asa_cab_meta` meta
    USING(date, campaign_name, campaign_type, adset_name)
    WHERE campaign_type = 'UA'
    GROUP BY 1,2,3,4,5,6,7
),

asa_convs AS (
    SELECT 
        date,
        campaign_name,
        platform,
        promo_type,
        geo,
        'UA' as campaign_type,
        promo_search,
        SUM(IF(event_name = 'install', event_count,0)) AS installs,
        SUM(IF(event_name = 'first_purchase', event_revenue,0)) AS first_purchase_revenue,
        SUM(IF(event_name = 'first_purchase',event_count, 0)) AS first_purchase,
        SUM(IF(event_name = 'first_purchase', uniq_event_count, 0)) AS uniq_first_purchase,
        SUM(IF(event_name = "af_purchase", event_revenue, 0)) AS revenue,
        SUM(IF(event_name = "af_purchase", event_count, 0)) AS purchase,
        SUM(IF(event_name = "af_purchase", uniq_event_count, 0)) AS uniq_purchase,
    FROM af_conversions
    WHERE NOT REGEXP_CONTAINS(campaign_name, r'\(r\)')
    AND (
        REGEXP_CONTAINS(campaign_name, r'\(exact\)|зоо') OR
        mediasource = 'Apple Search Ads'
    )
    AND is_retargeting = FALSE
    GROUP BY 1,2,3,4,5,6,7
),

asa AS (
    SELECT
        COALESCE(asa_convs.date, asa_cost.date) AS date,
        COALESCE(asa_convs.campaign_name, asa_cost.campaign_name) AS campaign_name,
        COALESCE(asa_convs.platform, asa_cost.platform) AS platform,
        COALESCE(asa_convs.promo_type, asa_cost.promo_type) AS promo_type,
        COALESCE(asa_convs.geo, asa_cost.geo) AS geo,
        COALESCE(asa_convs.campaign_type, asa_cost.campaign_type) AS campaign_type,
        COALESCE(asa_convs.promo_search, asa_cost.promo_search) AS promo_search,
        COALESCE(impressions,0) AS impressions,
        COALESCE(clicks,0) AS clicks,
        COALESCE(installs,0) AS installs,
        COALESCE(revenue,0) AS revenue,
        COALESCE(purchase,0) AS purchase,
        COALESCE(uniq_purchase,0) AS uniq_purchase,
        COALESCE(first_purchase_revenue,0) AS first_purchase_revenue,
        COALESCE(first_purchase,0) AS first_purchase,
        COALESCE(uniq_first_purchase,0) AS uniq_first_purchase,
        COALESCE(spend,0) AS spend,
        'Apple Search Ads' AS source,
        'context' AS adv_type
    FROM asa_convs
    FULL OUTER JOIN asa_cost
    ON asa_convs.date = asa_cost.date 
    AND asa_convs.campaign_name = asa_cost.campaign_name
    AND asa_convs.promo_type = asa_cost.promo_type
    AND asa_convs.geo = asa_cost.geo
    AND asa_convs.promo_search = asa_cost.promo_search
    WHERE 
        COALESCE(installs,0) + 
        COALESCE(revenue,0) + 
        COALESCE(purchase,0) + 
        COALESCE(uniq_purchase,0) +
        COALESCE(first_purchase_revenue,0) +
        COALESCE(first_purchase,0) + 
        COALESCE(uniq_first_purchase,0) +
        COALESCE(spend,0) > 0
    AND COALESCE(asa_convs.campaign_name, asa_cost.campaign_name) != 'None'
),

----------------------- google -------------------------

google_cost AS (
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
          WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), r'msk+spb|mskspb|msk_spb') THEN 'МСК, СПб'
          WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), r'spb') THEN 'СПб'
          WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), r'msk') THEN 'МСК'
          WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), r'_nn_|g:nn|\[nn\]') THEN 'НН'
          WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), r'reg1') THEN 'Регионы с доставкой'
          WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, adset_name], ' ')), r'reg2|rostov|kzn|g_all')THEN 'Регионы без доставки'
        ELSE 'Россия' END
 AS geo,
        campaign_type,
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
        SUM(impressions) AS impressions,
        SUM(clicks) AS clicks,
        SUM(spend) AS spend,
        SUM(IF(
    CASE
        WHEN REGEXP_CONTAINS(LOWER(campaign_name), r'\[p:ios\]|_ios_|p02') THEN 'ios'
        WHEN REGEXP_CONTAINS(LOWER(campaign_name), r'\[p:and\]|_and_|android|p01') THEN 'android'
    ELSE 'no_platform' END
 = 'ios', installs, NULL)) AS installs
    FROM `perekrestokvprok-bq`.`dbt_production`.`stg_google_cab_sheets`
    WHERE campaign_type = 'UA'
    AND REGEXP_CONTAINS(campaign_name, r'realweb')
    AND campaign_name NOT IN (
            'realweb_uac_2022 [p:and] [cpi] [mskspb] [new] [general] [darkstore] [purchase] [firebase]',
            'realweb_uac_2022 [p:and] [cpi] [reg1] [new] [general] [darkstore] [purchase] [firebase]')
    GROUP BY 1,2,3,4,5,6,7
),

google_convs AS (
    SELECT 
        date,
        campaign_name,
        platform,
        promo_type,
        geo,
        'UA' as campaign_type,
        promo_search,
        SUM(IF(event_name = 'install', event_count,0)) AS installs,
        SUM(IF(event_name = 'first_purchase', event_revenue,0)) AS first_purchase_revenue,
        SUM(IF(event_name = 'first_purchase',event_count, 0)) AS first_purchase,
        SUM(IF(event_name = 'first_purchase', uniq_event_count, 0)) AS uniq_first_purchase,
        SUM(IF(event_name = "af_purchase", event_revenue, 0)) AS revenue,
        SUM(IF(event_name = "af_purchase", event_count, 0)) AS purchase,
        SUM(IF(event_name = "af_purchase", uniq_event_count, 0)) AS uniq_purchase,
    FROM af_conversions
    WHERE REGEXP_CONTAINS(campaign_name, r'realweb_uac_')
    AND is_retargeting = FALSE
    AND campaign_name NOT IN (
            'realweb_uac_2022 [p:and] [cpi] [mskspb] [new] [general] [darkstore] [purchase] [firebase]',
            'realweb_uac_2022 [p:and] [cpi] [reg1] [new] [general] [darkstore] [purchase] [firebase]')
    GROUP BY 1,2,3,4,5,6,7
),

google AS (
    SELECT
        COALESCE(google_convs.date, google_cost.date) AS date,
        COALESCE(google_convs.campaign_name, google_cost.campaign_name) AS campaign_name,
        COALESCE(google_convs.platform, google_cost.platform) AS platform,
        COALESCE(google_convs.promo_type, google_cost.promo_type) AS promo_type,
        COALESCE(google_convs.geo, google_cost.geo) AS geo,
        COALESCE(google_convs.campaign_type, google_cost.campaign_type) AS campaign_type,
        COALESCE(google_convs.promo_search, google_cost.promo_search) AS promo_search,
        COALESCE(impressions,0) AS impressions,
        COALESCE(clicks,0) AS clicks,
        COALESCE(google_cost.installs,google_convs.installs,0) AS installs,
        COALESCE(revenue,0) AS revenue,
        COALESCE(purchase,0) AS purchase,
        COALESCE(uniq_purchase,0) AS uniq_purchase,
        COALESCE(first_purchase_revenue,0) AS first_purchase_revenue,
        COALESCE(first_purchase,0) AS first_purchase,
        COALESCE(uniq_first_purchase,0) AS uniq_first_purchase,
        COALESCE(spend,0) AS spend,
        'Google Ads' AS source,
        'context' AS adv_type
    FROM google_convs
    FULL OUTER JOIN google_cost
    ON google_convs.date = google_cost.date 
    AND google_convs.campaign_name = google_cost.campaign_name
    AND google_convs.promo_type = google_cost.promo_type
    AND google_convs.geo = google_cost.geo
    AND google_convs.promo_search = google_cost.promo_search
    WHERE 
        COALESCE(google_cost.installs,google_convs.installs,0) + 
        COALESCE(revenue,0) + 
        COALESCE(purchase,0) + 
        COALESCE(uniq_purchase,0) +
        COALESCE(first_purchase_revenue,0) +
        COALESCE(first_purchase,0) + 
        COALESCE(uniq_first_purchase,0) +
        COALESCE(spend,0) > 0
    AND COALESCE(google_convs.campaign_name, google_cost.campaign_name) != 'None'
),

----------------------- huawei -------------------------

huawei_cost AS (
    SELECT
        date,
        campaign_name,
        
    CASE
        WHEN REGEXP_CONTAINS(LOWER(campaign_name), r'\[p:ios\]|_ios_|p02') THEN 'ios'
        WHEN REGEXP_CONTAINS(LOWER(campaign_name), r'\[p:and\]|_and_|android|p01') THEN 'android'
    ELSE 'no_platform' END
 as platform,
        
    CASE
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-"],'')), r'promo.*regular') THEN 'promo regular'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-"],'')), r'promo.*global') THEN 'promo global'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-"],'')), r'promo.*feed') THEN 'promo feed'
    ELSE '-' END
 as promo_type,
        
    CASE
          WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-"], ' ')), r'msk+spb|mskspb|msk_spb') THEN 'МСК, СПб'
          WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-"], ' ')), r'spb') THEN 'СПб'
          WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-"], ' ')), r'msk') THEN 'МСК'
          WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-"], ' ')), r'_nn_|g:nn|\[nn\]') THEN 'НН'
          WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-"], ' ')), r'reg1') THEN 'Регионы с доставкой'
          WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-"], ' ')), r'reg2|rostov|kzn|g_all')THEN 'Регионы без доставки'
        ELSE 'Россия' END
 AS geo,
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
        campaign_type,
        SUM(impressions) AS impressions,
        SUM(clicks) AS clicks,
        SUM(spend) AS spend
    FROM `perekrestokvprok-bq`.`dbt_production`.`stg_huawei_cab_sheets`
    WHERE campaign_type = 'UA'
    AND status != "Deleted"
    GROUP BY 1,2,3,4,5,6,7
),

huawei_convs AS (
    SELECT 
        date,
        campaign_name,
        platform,
        promo_type,
        geo,
        'UA' as campaign_type,
        promo_search,
        SUM(IF(event_name = 'install', event_count,0)) AS installs,
        SUM(IF(event_name = 'first_purchase', event_revenue,0)) AS first_purchase_revenue,
        SUM(IF(event_name = 'first_purchase',event_count, 0)) AS first_purchase,
        SUM(IF(event_name = 'first_purchase', uniq_event_count, 0)) AS uniq_first_purchase,
        SUM(IF(event_name = "af_purchase", event_revenue, 0)) AS revenue,
        SUM(IF(event_name = "af_purchase", event_count, 0)) AS purchase,
        SUM(IF(event_name = "af_purchase", uniq_event_count, 0)) AS uniq_purchase,
    FROM af_conversions
    WHERE is_retargeting = FALSE
    AND REGEXP_CONTAINS(campaign_name, r'realweb_hw')
    AND REGEXP_CONTAINS(campaign_name, r'new')
    GROUP BY 1,2,3,4,5,6,7
),

huawei AS (
    SELECT
        COALESCE(huawei_convs.date, huawei_cost.date) AS date,
        COALESCE(huawei_convs.campaign_name, huawei_cost.campaign_name) AS campaign_name,
        COALESCE(huawei_convs.platform, huawei_cost.platform) AS platform,
        COALESCE(huawei_convs.promo_type, huawei_cost.promo_type) AS promo_type,
        COALESCE(huawei_convs.geo, huawei_cost.geo) AS geo,
        COALESCE(huawei_convs.campaign_type, huawei_cost.campaign_type) AS campaign_type,
        COALESCE(huawei_convs.promo_search, huawei_cost.promo_search) AS promo_search,
        COALESCE(impressions,0) AS impressions,
        COALESCE(clicks,0) AS clicks,
        COALESCE(installs,0) AS installs,
        COALESCE(revenue,0) AS revenue,
        COALESCE(purchase,0) AS purchase,
        COALESCE(uniq_purchase,0) AS uniq_purchase,
        COALESCE(first_purchase_revenue,0) AS first_purchase_revenue,
        COALESCE(first_purchase,0) AS first_purchase,
        COALESCE(uniq_first_purchase,0) AS uniq_first_purchase,
        COALESCE(spend,0) AS spend,
        'Huawei' AS source,
        'context' AS adv_type
    FROM huawei_convs
    FULL OUTER JOIN huawei_cost
    ON huawei_convs.date = huawei_cost.date 
    AND huawei_convs.campaign_name = huawei_cost.campaign_name
    AND huawei_convs.promo_type = huawei_cost.promo_type
    AND huawei_convs.geo = huawei_cost.geo
    AND huawei_convs.promo_search = huawei_cost.promo_search
    WHERE 
        COALESCE(installs,0) + 
        COALESCE(revenue,0) + 
        COALESCE(purchase,0) + 
        COALESCE(uniq_purchase,0) +
        COALESCE(first_purchase_revenue,0) +
        COALESCE(first_purchase,0) + 
        COALESCE(uniq_first_purchase,0) +
        COALESCE(spend,0) > 0
    AND COALESCE(huawei_convs.campaign_name, huawei_cost.campaign_name) != 'None'
),

----------------------- vk -------------------------

vk_cost_pre AS (
    SELECT
        date,
        campaign_name,
        
    CASE
        WHEN REGEXP_CONTAINS(LOWER(campaign_name), r'\[p:ios\]|_ios_|p02') THEN 'ios'
        WHEN REGEXP_CONTAINS(LOWER(campaign_name), r'\[p:and\]|_and_|android|p01') THEN 'android'
    ELSE 'no_platform' END
 as platform,
        
    CASE
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-"],'')), r'promo.*regular') THEN 'promo regular'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-"],'')), r'promo.*global') THEN 'promo global'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-"],'')), r'promo.*feed') THEN 'promo feed'
    ELSE '-' END
 as promo_type,
        
    CASE
          WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-"], ' ')), r'msk+spb|mskspb|msk_spb') THEN 'МСК, СПб'
          WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-"], ' ')), r'spb') THEN 'СПб'
          WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-"], ' ')), r'msk') THEN 'МСК'
          WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-"], ' ')), r'_nn_|g:nn|\[nn\]') THEN 'НН'
          WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-"], ' ')), r'reg1') THEN 'Регионы с доставкой'
          WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-"], ' ')), r'reg2|rostov|kzn|g_all')THEN 'Регионы без доставки'
        ELSE 'Россия' END
 AS geo,
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
        campaign_type,
        SUM(impressions) AS impressions,
        SUM(clicks) AS clicks,
        SUM(spend) AS spend
    FROM `perekrestokvprok-bq`.`dbt_production`.`int_vk_cab_meta`
    WHERE campaign_type = 'UA'
    GROUP BY 1,2,3,4,5,6,7
),

vk_cost AS (
    SELECT * FROM vk_cost_pre
    UNION ALL
    SELECT * FROM `perekrestokvprok-bq`.`agg_data`.`vk_manual_cost` -- ручные данные
),

vk_convs_pre AS (
    SELECT 
        date,
        campaign_name,
        platform,
        promo_type,
        geo,
        'UA' as campaign_type,
        promo_search,
        SUM(IF(event_name = 'install', event_count,0)) AS installs,
        SUM(IF(event_name = 'first_purchase', event_revenue,0)) AS first_purchase_revenue,
        SUM(IF(event_name = 'first_purchase',event_count, 0)) AS first_purchase,
        SUM(IF(event_name = 'first_purchase', uniq_event_count, 0)) AS uniq_first_purchase,
        SUM(IF(event_name = "af_purchase", event_revenue, 0)) AS revenue,
        SUM(IF(event_name = "af_purchase", event_count, 0)) AS purchase,
        SUM(IF(event_name = "af_purchase", uniq_event_count, 0)) AS uniq_purchase,
    FROM af_conversions
    WHERE is_retargeting = FALSE
    AND REGEXP_CONTAINS(campaign_name, r'realweb_vk')
    AND REGEXP_CONTAINS(campaign_name, r'new')
    GROUP BY 1,2,3,4,5,6,7
),

vk_convs AS (
    SELECT * FROM vk_convs_pre
    UNION ALL 
    SELECT * FROM `perekrestokvprok-bq`.`agg_data`.`vk_manual_data` -- ручные данные
),

vk AS (
    SELECT
        COALESCE(vk_convs.date, vk_cost.date) AS date,
        COALESCE(vk_convs.campaign_name, vk_cost.campaign_name) AS campaign_name,
        COALESCE(vk_convs.platform, vk_cost.platform) AS platform,
        COALESCE(vk_convs.promo_type, vk_cost.promo_type) AS promo_type,
        COALESCE(vk_convs.geo, vk_cost.geo) AS geo,
        COALESCE(vk_convs.campaign_type, vk_cost.campaign_type) AS campaign_type,
        COALESCE(vk_convs.promo_search, vk_cost.promo_search) AS promo_search,
        COALESCE(impressions,0) AS impressions,
        COALESCE(clicks,0) AS clicks,
        COALESCE(installs,0) AS installs,
        COALESCE(revenue,0) AS revenue,
        COALESCE(purchase,0) AS purchase,
        COALESCE(uniq_purchase,0) AS uniq_purchase,
        COALESCE(first_purchase_revenue,0) AS first_purchase_revenue,
        COALESCE(first_purchase,0) AS first_purchase,
        COALESCE(uniq_first_purchase,0) AS uniq_first_purchase,
        COALESCE(spend,0) AS spend,
        'ВК' AS source,
        'social' AS adv_type
    FROM vk_convs
    FULL OUTER JOIN vk_cost
    ON vk_convs.date = vk_cost.date 
    AND vk_convs.campaign_name = vk_cost.campaign_name
    AND vk_convs.promo_type = vk_cost.promo_type
    AND vk_convs.geo = vk_cost.geo
    AND vk_convs.promo_search = vk_cost.promo_search
    WHERE 
        COALESCE(installs,0) + 
        COALESCE(revenue,0) + 
        COALESCE(purchase,0) + 
        COALESCE(uniq_purchase,0) +
        COALESCE(first_purchase_revenue,0) +
        COALESCE(first_purchase,0) + 
        COALESCE(uniq_first_purchase,0) +
        COALESCE(spend,0) > 0
    AND COALESCE(vk_convs.campaign_name, vk_cost.campaign_name) != 'None'
),

----------------------- zen -------------------------

zen_cost AS (
    SELECT
        date,
        campaign_name,
        
    CASE
        WHEN REGEXP_CONTAINS(LOWER(campaign_name), r'\[p:ios\]|_ios_|p02') THEN 'ios'
        WHEN REGEXP_CONTAINS(LOWER(campaign_name), r'\[p:and\]|_and_|android|p01') THEN 'android'
    ELSE 'no_platform' END
 as platform,
        
    CASE
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-"],'')), r'promo.*regular') THEN 'promo regular'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-"],'')), r'promo.*global') THEN 'promo global'
        WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-"],'')), r'promo.*feed') THEN 'promo feed'
    ELSE '-' END
 as promo_type,
        
    CASE
          WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-"], ' ')), r'msk+spb|mskspb|msk_spb') THEN 'МСК, СПб'
          WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-"], ' ')), r'spb') THEN 'СПб'
          WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-"], ' ')), r'msk') THEN 'МСК'
          WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-"], ' ')), r'_nn_|g:nn|\[nn\]') THEN 'НН'
          WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-"], ' ')), r'reg1') THEN 'Регионы с доставкой'
          WHEN REGEXP_CONTAINS(LOWER(ARRAY_TO_STRING([campaign_name, "-"], ' ')), r'reg2|rostov|kzn|g_all')THEN 'Регионы без доставки'
        ELSE 'Россия' END
 AS geo,
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
        campaign_type,
        --SUM(impressions) AS impressions,
        --SUM(clicks) AS clicks,
        --SUM(spend) AS spend
        0 impressions,
        0 clicks,
        SUM(cost) AS spend
    FROM `perekrestokvprok-bq`.`dbt_production`.`stg_zen_data_sheets`
    WHERE campaign_type = 'UA'
    GROUP BY 1,2,3,4,5,6,7
),

zen_convs AS (
    SELECT 
        date,
        campaign_name,
        platform,
        promo_type,
        geo,
        'UA' as campaign_type,
        promo_search,
        SUM(IF(event_name = 'install', event_count,0)) AS installs,
        SUM(IF(event_name = 'first_purchase', event_revenue,0)) AS first_purchase_revenue,
        SUM(IF(event_name = 'first_purchase',event_count, 0)) AS first_purchase,
        SUM(IF(event_name = 'first_purchase', uniq_event_count, 0)) AS uniq_first_purchase,
        SUM(IF(event_name = "af_purchase", event_revenue, 0)) AS revenue,
        SUM(IF(event_name = "af_purchase", event_count, 0)) AS purchase,
        SUM(IF(event_name = "af_purchase", uniq_event_count, 0)) AS uniq_purchase,
    FROM af_conversions
    WHERE is_retargeting = FALSE
    AND REGEXP_CONTAINS(campaign_name, r'realweb_dz')
    AND REGEXP_CONTAINS(campaign_name, r'_new_')
    GROUP BY 1,2,3,4,5,6,7
),

zen AS (
    SELECT
        COALESCE(zen_convs.date, zen_cost.date) AS date,
        COALESCE(zen_convs.campaign_name, zen_cost.campaign_name) AS campaign_name,
        COALESCE(zen_convs.platform, zen_cost.platform) AS platform,
        COALESCE(zen_convs.promo_type, zen_cost.promo_type) AS promo_type,
        COALESCE(zen_convs.geo, zen_cost.geo) AS geo,
        COALESCE(zen_convs.campaign_type, zen_cost.campaign_type) AS campaign_type,
        COALESCE(zen_convs.promo_search, zen_cost.promo_search) AS promo_search,
        COALESCE(impressions,0) AS impressions,
        COALESCE(clicks,0) AS clicks,
        COALESCE(installs,0) AS installs,
        COALESCE(revenue,0) AS revenue,
        COALESCE(purchase,0) AS purchase,
        COALESCE(uniq_purchase,0) AS uniq_purchase,
        COALESCE(first_purchase_revenue,0) AS first_purchase_revenue,
        COALESCE(first_purchase,0) AS first_purchase,
        COALESCE(uniq_first_purchase,0) AS uniq_first_purchase,
        COALESCE(spend,0) AS spend,
        'Zen' AS source,
        'Яндекс.Дзен' AS adv_type
    FROM zen_convs
    FULL OUTER JOIN zen_cost
    ON zen_convs.date = zen_cost.date 
    AND zen_convs.campaign_name = zen_cost.campaign_name
    AND zen_convs.promo_type = zen_cost.promo_type
    AND zen_convs.geo = zen_cost.geo
    AND zen_convs.promo_search = zen_cost.promo_search
    WHERE 
        COALESCE(installs,0) + 
        COALESCE(revenue,0) + 
        COALESCE(purchase,0) + 
        COALESCE(uniq_purchase,0) +
        COALESCE(first_purchase_revenue,0) +
        COALESCE(first_purchase,0) + 
        COALESCE(uniq_first_purchase,0) +
        COALESCE(spend,0) > 0
    AND COALESCE(zen_convs.campaign_name, zen_cost.campaign_name) != 'None'
),

----------------------inapp----------------------------

rate AS (
    SELECT
        start_date,
        end_date,
        partner,
        platform,
        rate_for_us
    FROM `perekrestokvprok-bq`.`dbt_production`.`stg_rate_info`
    WHERE type = 'UA'
    AND source = 'inapp'
),

limits_table AS (
    SELECT
        start_date,
        end_date,
        partner,
        limits
    FROM `perekrestokvprok-bq`.`dbt_production`.`stg_partner_limits`
    WHERE type = 'UA'
    AND source = 'inapp'
),

inapp_convs_without_cumulation AS (
    SELECT 
        date,
        campaign_name,
        
    CASE 
        WHEN REGEXP_CONTAINS(campaign_name, r'_ms_') THEN 'Mobisharks'
        WHEN REGEXP_CONTAINS(campaign_name, r'_tl_') THEN '2leads'
        WHEN REGEXP_CONTAINS(campaign_name, r'_mx_') THEN 'MobX'
        WHEN REGEXP_CONTAINS(campaign_name, r'_sw_') THEN 'SW'
        WHEN REGEXP_CONTAINS(campaign_name, r'_tm_') THEN 'Think Mobile'
        WHEN REGEXP_CONTAINS(campaign_name, r'_abc[_\s]|_sf_') THEN 'Mediasurfer'
    ELSE '-' END
 AS partner,
        platform,
        promo_type,
        geo,
        'UA' as campaign_type,
        promo_search,
        SUM(IF(event_name = 'install', event_count,0)) AS installs,
        SUM(IF(event_name = 'first_purchase', event_revenue,0)) AS first_purchase_revenue,
        SUM(IF(event_name = 'first_purchase',event_count, 0)) AS first_purchase,
        SUM(IF(event_name = 'first_purchase', uniq_event_count, 0)) AS uniq_first_purchase,
        SUM(IF(event_name = "af_purchase", event_revenue, 0)) AS revenue,
        SUM(IF(event_name = "af_purchase", event_count, 0)) AS purchase,
        SUM(IF(event_name = "af_purchase", uniq_event_count, 0)) AS uniq_purchase,
    FROM af_conversions
    WHERE REGEXP_CONTAINS(campaign_name, r'realweb_inapp')
    AND is_retargeting = FALSE
    GROUP BY 1,2,3,4,5,6,7,8
),

inapp_convs_with_cumulation AS (
    SELECT
        date,
        campaign_name,
        partner,
        platform,
        promo_type,
        geo,
        campaign_type,
        promo_search,
        installs,
        first_purchase_revenue,
        first_purchase,
        uniq_first_purchase,
        revenue,
        purchase,
        uniq_purchase,
        SUM(first_purchase) 
            OVER(PARTITION BY DATE_TRUNC(date, MONTH), partner ORDER BY date, first_purchase_revenue)
            AS cum_event_count_by_prt
    FROM inapp_convs_without_cumulation
),

inapp_convs AS (
    SELECT
        date,
        campaign_name,
        inapp_convs_with_cumulation.partner,
        platform,
        promo_type,
        geo,
        campaign_type,
        promo_search,
        installs,
        IF(cum_event_count_by_prt <= COALESCE(limits, 1000000), first_purchase_revenue, 0) AS first_purchase_revenue,
        IF(cum_event_count_by_prt <= COALESCE(limits, 1000000), first_purchase, 0) AS first_purchase,
        IF(cum_event_count_by_prt <= COALESCE(limits, 1000000), uniq_first_purchase, 0) AS uniq_first_purchase,
        revenue,
        purchase,
        uniq_purchase,
    FROM inapp_convs_with_cumulation
    LEFT JOIN limits_table
    ON inapp_convs_with_cumulation.partner = limits_table.partner 
    AND inapp_convs_with_cumulation.date BETWEEN limits_table.start_date AND limits_table.end_date
),

inapp AS (
    SELECT
        date,
        campaign_name,
        inapp_convs.platform AS platform,
        promo_type,
        geo,
        campaign_type,
        promo_search,
        0 AS impressions,
        0 AS clicks,
        COALESCE(installs,0) AS installs,
        COALESCE(revenue,0) AS revenue,
        COALESCE(purchase,0) AS purchase,
        COALESCE(uniq_purchase,0) AS uniq_purchase,
        COALESCE(first_purchase_revenue,0) AS first_purchase_revenue,
        COALESCE(first_purchase,0) AS first_purchase,
        COALESCE(uniq_first_purchase,0) AS uniq_first_purchase,
        COALESCE(first_purchase * rate_for_us,0)  AS spend,
        'inapp' AS source,
        'inapp' AS adv_type
    FROM inapp_convs
    LEFT JOIN rate
    ON inapp_convs.partner = rate.partner 
    AND inapp_convs.date BETWEEN rate.start_date AND rate.end_date
    AND inapp_convs.platform = rate.platform 
    WHERE 
        COALESCE(installs,0) + 
        COALESCE(revenue,0) + 
        COALESCE(purchase,0) + 
        COALESCE(uniq_purchase,0) +
        COALESCE(first_purchase_revenue,0) +
        COALESCE(first_purchase,0) + 
        COALESCE(uniq_first_purchase,0) +
        COALESCE(first_purchase * rate_for_us,0) > 0
    AND campaign_name != 'None'
),

----------------------Xiaomi----------------------------

x_rate AS (
    SELECT
        start_date,
        end_date,
        partner,
        platform,
        rate_for_us
    FROM `perekrestokvprok-bq`.`dbt_production`.`stg_rate_info`
    WHERE type = 'UA'
    AND source = 'Xiaomi'
),

x_limits_table AS (
    SELECT
        start_date,
        end_date,
        partner,
        limits
    FROM `perekrestokvprok-bq`.`dbt_production`.`stg_partner_limits`
    WHERE type = 'UA'
    AND source = 'Xiaomi'
),

xiaomi_convs_without_cumulation AS (
    SELECT 
        date,
        campaign_name,
        
    CASE 
        WHEN REGEXP_CONTAINS(campaign_name, r'_ms_') THEN 'Mobisharks'
        WHEN REGEXP_CONTAINS(campaign_name, r'_tl_') THEN '2leads'
        WHEN REGEXP_CONTAINS(campaign_name, r'_mx_') THEN 'MobX'
        WHEN REGEXP_CONTAINS(campaign_name, r'_sw_') THEN 'SW'
        WHEN REGEXP_CONTAINS(campaign_name, r'_tm_') THEN 'Think Mobile'
        WHEN REGEXP_CONTAINS(campaign_name, r'_abc[_\s]|_sf_') THEN 'Mediasurfer'
    ELSE '-' END
 AS partner,
        platform,
        promo_type,
        geo,
        'UA' as campaign_type,
        promo_search,
        SUM(IF(event_name = 'install', event_count,0)) AS installs,
        SUM(IF(event_name = 'first_purchase', event_revenue,0)) AS first_purchase_revenue,
        SUM(IF(event_name = 'first_purchase',event_count, 0)) AS first_purchase,
        SUM(IF(event_name = 'first_purchase', uniq_event_count, 0)) AS uniq_first_purchase,
        SUM(IF(event_name = "af_purchase", event_revenue, 0)) AS revenue,
        SUM(IF(event_name = "af_purchase", event_count, 0)) AS purchase,
        SUM(IF(event_name = "af_purchase", uniq_event_count, 0)) AS uniq_purchase,
    FROM af_conversions
    WHERE REGEXP_CONTAINS(campaign_name, r'realweb_xiaomi')
    AND is_retargeting = FALSE
    GROUP BY 1,2,3,4,5,6,7,8
),

xiaomi_convs_with_cumulation AS (
    SELECT
        date,
        campaign_name,
        partner,
        platform,
        promo_type,
        geo,
        campaign_type,
        promo_search,
        installs,
        first_purchase_revenue,
        first_purchase,
        uniq_first_purchase,
        revenue,
        purchase,
        uniq_purchase,
        SUM(installs) 
            OVER(PARTITION BY DATE_TRUNC(date, MONTH), partner ORDER BY date, installs)
            AS cum_event_count_by_prt
    FROM xiaomi_convs_without_cumulation
),

xiaomi_convs AS (
    SELECT
        date,
        campaign_name,
        xiaomi_convs_with_cumulation.partner,
        platform,
        promo_type,
        geo,
        campaign_type,
        promo_search,
        IF(cum_event_count_by_prt <= COALESCE(limits, 1000000), installs, 0) AS installs,
        first_purchase_revenue,
        first_purchase,
        uniq_first_purchase,
        revenue,
        purchase,
        uniq_purchase,
    FROM xiaomi_convs_with_cumulation
    LEFT JOIN x_limits_table
    ON xiaomi_convs_with_cumulation.partner = x_limits_table.partner 
    AND xiaomi_convs_with_cumulation.date BETWEEN x_limits_table.start_date AND x_limits_table.end_date
),

xiaomi AS (
    SELECT
        date,
        campaign_name,
        xiaomi_convs.platform AS platform,
        promo_type,
        geo,
        campaign_type,
        promo_search,
        0 AS impressions,
        0 AS clicks,
        COALESCE(installs,0) AS installs,
        COALESCE(revenue,0) AS revenue,
        COALESCE(purchase,0) AS purchase,
        COALESCE(uniq_purchase,0) AS uniq_purchase,
        COALESCE(first_purchase_revenue,0) AS first_purchase_revenue,
        COALESCE(first_purchase,0) AS first_purchase,
        COALESCE(uniq_first_purchase,0) AS uniq_first_purchase,
        COALESCE(installs * rate_for_us,0)  AS spend,
        'Xiaomi' AS source,
        'Xiaomi' AS adv_type
    FROM xiaomi_convs
    LEFT JOIN x_rate
    ON xiaomi_convs.partner = x_rate.partner 
    AND xiaomi_convs.date BETWEEN x_rate.start_date AND x_rate.end_date
    AND xiaomi_convs.platform = x_rate.platform 
    WHERE 
        COALESCE(installs,0) + 
        COALESCE(revenue,0) + 
        COALESCE(purchase,0) + 
        COALESCE(uniq_purchase,0) +
        COALESCE(first_purchase_revenue,0) +
        COALESCE(first_purchase,0) + 
        COALESCE(uniq_first_purchase,0) +
        COALESCE(installs * rate_for_us,0) > 0
    AND campaign_name != 'None'
),

----------------------Realweb CPA----------------------------

realwebcpa_convs AS (
    SELECT 
        date,
        campaign_name,
        platform,
        promo_type,
        geo,
        'UA' as campaign_type,
        promo_search,
        SUM(IF(event_name = 'install', event_count,0)) AS installs,
        SUM(IF(event_name = 'first_purchase', event_revenue,0)) AS first_purchase_revenue,
        SUM(IF(event_name = 'first_purchase',event_count, 0)) AS first_purchase,
        SUM(IF(event_name = 'first_purchase', uniq_event_count, 0)) AS uniq_first_purchase,
        SUM(IF(event_name = "af_purchase", event_revenue, 0)) AS revenue,
        SUM(IF(event_name = "af_purchase", event_count, 0)) AS purchase,
        SUM(IF(event_name = "af_purchase", uniq_event_count, 0)) AS uniq_purchase,
    FROM af_conversions
    WHERE is_retargeting = FALSE
    AND REGEXP_CONTAINS(campaign_name, r'realwebcpa')
    GROUP BY 1,2,3,4,5,6,7
),

realwebcpa AS (
    SELECT
        date,
        campaign_name,
        platform,
        promo_type,
        geo,
        campaign_type,
        promo_search,
        0 AS impressions,
        0 AS clicks,
        installs,
        revenue,
        purchase,
        uniq_purchase,
        first_purchase_revenue,
        first_purchase,
        uniq_first_purchase,
        first_purchase * 1000 AS spend,
        'Realweb CPA' AS source,
        'Realweb CPA' AS adv_type
    FROM realwebcpa_convs
),

----------------------final----------------------------

unions AS (
    SELECT * FROM yandex
    UNION ALL 
    SELECT * FROM mt
    UNION ALL  
    SELECT * FROM tiktok
    UNION ALL
    SELECT * FROM asa
    UNION ALL
    SELECT * FROM facebook
    UNION ALL
    SELECT * FROM google
    UNION ALL
    SELECT * FROM huawei
    UNION ALL
    SELECT * FROM vk
    UNION ALL
    SELECT * FROM inapp
    UNION ALL
    SELECT * FROM xiaomi
    UNION ALL
    SELECT * FROM zen
    UNION ALL
    SELECT * FROM realwebcpa
),

final AS (
    SELECT 
        date,
        campaign_name,
        platform,
        promo_type,
        geo,
        campaign_type,
        promo_search,
        impressions,
        clicks,
        installs,
        revenue,
        purchase,
        uniq_purchase,
        first_purchase_revenue,
        first_purchase,
        uniq_first_purchase,
        spend,
        source,
        
    CASE 
        WHEN REGEXP_CONTAINS(campaign_name, r'[\[_]cpi[\]_]') OR source = 'Apple Search Ads' THEN 'CPI'
        WHEN REGEXP_CONTAINS(campaign_name, r'[\[_]cpa[\]_]') OR REGEXP_CONTAINS(campaign_name, r'^realwebcpa') THEN 'CPA'
        WHEN REGEXP_CONTAINS(campaign_name, r'[\[_]cpc[\]_]') THEN 'CPC'
        WHEN REGEXP_CONTAINS(campaign_name, r'[\[_]cpm[\]_]') THEN 'CPM'
        WHEN REGEXP_CONTAINS(campaign_name, r'[\[_]cpo[\]_]') THEN 'CPO'
    ELSE 'Не определено' END
 AS conversion_source_type,
        adv_type
    FROM unions
)

SELECT 
    date,
    campaign_name,
    platform,
    promo_type,
    geo,
    campaign_type,
    promo_search,
    impressions,
    clicks,
    installs,
    revenue,
    purchase,
    uniq_purchase,
    first_purchase_revenue,
    first_purchase,
    uniq_first_purchase,
    spend,
    source,
    conversion_source_type,
    adv_type
FROM final