{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_bkgs_chnl_msr_nbcm_adj', 'batch', 'edwtd_bkg_aggrs'],
    meta={
        'source_workflow': 'wf_m_WI_BKGS_CHNL_MSR_NBCM_ADJ',
        'target_table': 'WI_BKGS_CHNL_MSR_ADJ',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.108463+00:00'
    }
) }}

WITH 

source_wi_bkgs_chnl_msr_adj AS (
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
    FROM {{ source('raw', 'wi_bkgs_chnl_msr_adj') }}
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
        edw_update_user,
        edw_update_datetime,
        dv_route_to_market_cd,
        booking_channel_type
    FROM source_wi_bkgs_chnl_msr_adj
)

SELECT * FROM final