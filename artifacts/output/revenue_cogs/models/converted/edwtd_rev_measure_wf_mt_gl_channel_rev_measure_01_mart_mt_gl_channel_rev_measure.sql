{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_mt_gl_channel_rev_measure', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_MT_GL_CHANNEL_REV_MEASURE',
        'target_table': 'MT_GL_CHANNEL_REV_MEASURE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.243470+00:00'
    }
) }}

WITH 

source_mt_gl_channel_rev_measure AS (
    SELECT
        revenue_measure_key,
        dv_fiscal_year_mth_number_int,
        rev_measure_trans_type_cd,
        partner_site_party_key,
        channel_flg,
        channel_drop_ship_flg,
        as_of_fsc_mth_ptr_ste_prty_key,
        as_of_fsc_mth_channel_flg,
        as_of_fsc_mth_chnl_drp_shp_flg,
        dv_route_to_market_cd,
        dv_as_of_fsc_mth_rtm_cd,
        partner_type_cd,
        as_of_fsc_mth_partner_type_cd,
        dv_pnl_line_item_name,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        rtm_split_ratio
    FROM {{ source('raw', 'mt_gl_channel_rev_measure') }}
),

final AS (
    SELECT
        revenue_measure_key,
        dv_fiscal_year_mth_number_int,
        rev_measure_trans_type_cd,
        partner_site_party_key,
        channel_flg,
        channel_drop_ship_flg,
        as_of_fsc_mth_ptr_ste_prty_key,
        as_of_fsc_mth_channel_flg,
        as_of_fsc_mth_chnl_drp_shp_flg,
        dv_route_to_market_cd,
        dv_as_of_fsc_mth_rtm_cd,
        partner_type_cd,
        as_of_fsc_mth_partner_type_cd,
        dv_pnl_line_item_name,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        rtm_split_ratio
    FROM source_mt_gl_channel_rev_measure
)

SELECT * FROM final