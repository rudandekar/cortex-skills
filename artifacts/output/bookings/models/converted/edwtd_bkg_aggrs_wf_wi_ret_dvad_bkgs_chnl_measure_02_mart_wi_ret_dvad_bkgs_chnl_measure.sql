{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_ret_dvad_bkgs_chnl_measure', 'batch', 'edwtd_bkg_aggrs'],
    meta={
        'source_workflow': 'wf_m_WI_RET_DVAD_BKGS_CHNL_MEASURE',
        'target_table': 'WI_RET_DVAD_BKGS_CHNL_MEASURE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.716729+00:00'
    }
) }}

WITH 

source_wi_ret_dvad_bkgs_chnl_measure AS (
    SELECT
        bookings_measure_key,
        bookings_process_date,
        bkgs_measure_trans_type_code,
        partner_site_party_key,
        channel_drop_ship_flg,
        dv_route_to_market_cd
    FROM {{ source('raw', 'wi_ret_dvad_bkgs_chnl_measure') }}
),

source_wi_chnl_ret_dvad_part_country AS (
    SELECT
        sold_to_customer_key,
        dv_end_customer_key,
        sales_order_key,
        dv_end_cust_party_key,
        deals_ppsk,
        dv_end_cust_country_cd,
        deals_cntry_cd
    FROM {{ source('raw', 'wi_chnl_ret_dvad_part_country') }}
),

final AS (
    SELECT
        bookings_measure_key,
        bookings_process_date,
        bkgs_measure_trans_type_code,
        partner_site_party_key,
        channel_bookings_flg_as,
        channel_drop_ship_flg,
        dv_route_to_market_cd,
        booking_channel_type
    FROM source_wi_chnl_ret_dvad_part_country
)

SELECT * FROM final