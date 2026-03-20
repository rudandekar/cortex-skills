{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_bookings_channel_measure_pos', 'batch', 'edwtd_bkg_aggrs'],
    meta={
        'source_workflow': 'wf_m_W_BOOKINGS_CHANNEL_MEASURE_POS',
        'target_table': 'W_BOOKINGS_CHANNEL_MEASURE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.217789+00:00'
    }
) }}

WITH 

source_wi_bkgs_chnl_msr_pos AS (
    SELECT
        bookings_measure_key,
        bookings_process_date,
        bkgs_measure_trans_type_code,
        partner_site_party_key,
        channel_bookings_flg,
        channel_drop_ship_flg,
        dv_drct_val_added_dsti_ord_flg,
        edw_update_user,
        edw_update_datetime,
        dv_route_to_market_cd
    FROM {{ source('raw', 'wi_bkgs_chnl_msr_pos') }}
),

source_wi_bkgs_chnl_msr_pos_adj AS (
    SELECT
        bookings_measure_key,
        bookings_process_date,
        bkgs_measure_trans_type_code,
        partner_site_party_key,
        channel_bookings_flg,
        channel_drop_ship_flg,
        dv_drct_val_added_dsti_ord_flg,
        edw_update_user,
        edw_update_datetime,
        dv_route_to_market_cd
    FROM {{ source('raw', 'wi_bkgs_chnl_msr_pos_adj') }}
),

final AS (
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
        booking_channel_type,
        as_of_fsc_mth_bkgs_channel_type,
        manual_ptnr_override_reason_cd,
        otm_override_ptnr_site_party_key,
        mnl_ptnr_site_override_flg,
        action_code,
        dml_type
    FROM source_wi_bkgs_chnl_msr_pos_adj
)

SELECT * FROM final