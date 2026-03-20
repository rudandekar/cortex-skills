{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_ret_bkgs_chnl_measure_pos', 'batch', 'edwtd_bkg_aggrs'],
    meta={
        'source_workflow': 'wf_m_WI_RET_BKGS_CHNL_MEASURE_POS',
        'target_table': 'WI_RET_BKGS_CHNL_MEASURE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.640255+00:00'
    }
) }}

WITH 

source_n_bookings_channel_measure AS (
    SELECT
        bookings_measure_key,
        bookings_process_date,
        bkgs_measure_trans_type_code,
        partner_site_party_key,
        channel_bookings_flg,
        channel_drop_ship_flg,
        edw_update_user,
        edw_update_datetime,
        as_of_fsc_mth_ptr_ste_prty_key,
        as_of_fsc_mth_channel_bkgs_flg,
        as_of_fsc_mth_chnl_drp_shp_flg,
        dv_drct_val_added_dsti_ord_flg,
        dv_route_to_market_cd,
        dv_as_of_fsc_mth_rtm_cd,
        dv_route_to_market_description,
        otm_override_ptnr_site_party_key,
        mnl_ptnr_site_override_flg,
        manual_ptnr_override_reason_cd
    FROM {{ source('raw', 'n_bookings_channel_measure') }}
),

source_wi_ret_pos_bkgs_chnl_measure AS (
    SELECT
        bookings_measure_key,
        bookings_process_date,
        bkgs_measure_trans_type_code,
        partner_site_party_key,
        channel_bookings_flg,
        channel_drop_ship_flg
    FROM {{ source('raw', 'wi_ret_pos_bkgs_chnl_measure') }}
),

final AS (
    SELECT
        bookings_measure_key,
        bookings_process_date,
        bkgs_measure_trans_type_code,
        partner_site_party_key,
        channel_bookings_flg,
        channel_drop_ship_flg,
        dv_drct_val_added_dsti_ord_flg,
        as_of_fsc_mth_ptr_ste_prty_key,
        as_of_fsc_mth_channel_bkgs_flg,
        as_of_fsc_mth_chnl_drp_shp_flg,
        dv_route_to_market_cd,
        dv_as_of_fsc_mth_rtm_cd,
        dv_gcsp_flg,
        bk_be_geo_id_int,
        booking_channel_type,
        as_of_fsc_mth_bkgs_channel_type,
        dv_route_to_market_description,
        otm_override_ptnr_site_party_key,
        mnl_ptnr_site_override_flg,
        manual_ptnr_override_reason_cd
    FROM source_wi_ret_pos_bkgs_chnl_measure
)

SELECT * FROM final