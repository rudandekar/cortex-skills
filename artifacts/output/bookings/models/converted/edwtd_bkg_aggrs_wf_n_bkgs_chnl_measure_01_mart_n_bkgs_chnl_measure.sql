{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_bkgs_chnl_measure', 'batch', 'edwtd_bkg_aggrs'],
    meta={
        'source_workflow': 'wf_m_N_BKGS_CHNL_MEASURE',
        'target_table': 'N_BKGS_CHNL_MEASURE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.035958+00:00'
    }
) }}

WITH 

source_w_bkgs_chnl_measure AS (
    SELECT
        bookings_measure_key,
        dv_drct_val_added_dsti_ord_flg,
        as_of_fsc_mth_ptr_ste_prty_key,
        as_of_fsc_mth_channel_bkgs_flg,
        as_of_fsc_mth_chnl_drp_shp_flg,
        edw_update_user,
        edw_update_datetime,
        bookings_process_date,
        bkgs_measure_trans_type_code,
        partner_site_party_key,
        channel_bookings_flg,
        channel_drop_ship_flg,
        dv_route_to_market_cd,
        dv_as_of_fsc_mth_rtm_cd,
        dv_dbt_typ,
        magic_key_id,
        net_price_flg,
        action_code,
        dml_type,
        dv_route_to_market_description,
        manual_ptnr_override_reason_cd,
        otm_override_ptnr_site_party_key,
        mnl_ptnr_site_override_flg
    FROM {{ source('raw', 'w_bkgs_chnl_measure') }}
),

final AS (
    SELECT
        bookings_measure_key,
        dv_drct_val_added_dsti_ord_flg,
        as_of_fsc_mth_ptr_ste_prty_key,
        as_of_fsc_mth_channel_bkgs_flg,
        as_of_fsc_mth_chnl_drp_shp_flg,
        edw_update_user,
        edw_update_datetime,
        bookings_process_date,
        bkgs_measure_trans_type_code,
        partner_site_party_key,
        channel_bookings_flg,
        channel_drop_ship_flg,
        dv_route_to_market_cd,
        dv_as_of_fsc_mth_rtm_cd,
        dv_dbt_typ,
        booking_channel_type,
        as_of_fsc_mth_bkgs_channel_type,
        magic_key_id,
        net_price_flg,
        dv_route_to_market_description,
        manual_ptnr_override_reason_cd,
        otm_override_ptnr_site_party_key,
        mnl_ptnr_site_override_flg
    FROM source_w_bkgs_chnl_measure
)

SELECT * FROM final