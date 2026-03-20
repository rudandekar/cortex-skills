{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_mt_inv_channel_rev_measure', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_MT_INV_CHANNEL_REV_MEASURE',
        'target_table': 'MT_INV_CHANNEL_REV_MEASURE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.638066+00:00'
    }
) }}

WITH 

source_gcsp_attrib_rev_retro_pos AS (
    SELECT
        revenue_measure_key,
        dv_fiscal_year_mth_number_int,
        rev_measure_trans_type_cd,
        sold_to_customer_key,
        pos_order_dt,
        partner_site_party_key,
        be_id,
        be_geo_id,
        end_customer_gu_id,
        end_customer_gu_name,
        partner_country,
        end_customer_country,
        partner_bill_to_id
    FROM {{ source('raw', 'gcsp_attrib_rev_retro_pos') }}
),

source_gcsp_attrib_rev_retro AS (
    SELECT
        revenue_measure_key,
        dv_fiscal_year_mth_number_int,
        rev_measure_trans_type_cd,
        bill_to_customer_key,
        bk_so_src_crt_datetime,
        partner_site_party_key,
        be_id,
        be_geo_id,
        end_customer_gu_id,
        end_customer_gu_name,
        partner_country,
        end_customer_country,
        partner_bill_to_id
    FROM {{ source('raw', 'gcsp_attrib_rev_retro') }}
),

source_mt_inv_channel_rev_measure AS (
    SELECT
        revenue_measure_key,
        dv_fiscal_year_mth_number_int,
        rev_measure_trans_type_cd,
        ar_trx_key,
        partner_type_cd,
        as_of_fsc_mth_partner_type_cd,
        dv_partner_site_party_key,
        dv_channel_flg,
        dv_channel_drop_ship_flg,
        dv_as_of_fsc_mth_ptr_ste_prty_key,
        dv_as_of_fsc_mth_channel_flg,
        dv_as_of_fsc_mth_chnl_drp_shp_flg,
        dv_route_to_market_cd,
        dv_as_of_fsc_mth_rtm_cd,
        dv_pnl_line_item_name,
        dv_rtm_split_ratio_pct,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        dv_drct_val_added_dsti_ord_flg
    FROM {{ source('raw', 'mt_inv_channel_rev_measure') }}
),

final AS (
    SELECT
        revenue_measure_key,
        dv_fiscal_year_mth_number_int,
        rev_measure_trans_type_cd,
        ar_trx_key,
        partner_type_cd,
        as_of_fsc_mth_partner_type_cd,
        dv_partner_site_party_key,
        dv_channel_flg,
        dv_channel_drop_ship_flg,
        dv_as_of_fsc_mth_ptr_ste_prty_key,
        dv_as_of_fsc_mth_channel_flg,
        dv_as_of_fsc_mth_chnl_drp_shp_flg,
        dv_route_to_market_cd,
        dv_as_of_fsc_mth_rtm_cd,
        dv_pnl_line_item_name,
        dv_rtm_split_ratio_pct,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        dv_drct_val_added_dsti_ord_flg
    FROM source_mt_inv_channel_rev_measure
)

SELECT * FROM final