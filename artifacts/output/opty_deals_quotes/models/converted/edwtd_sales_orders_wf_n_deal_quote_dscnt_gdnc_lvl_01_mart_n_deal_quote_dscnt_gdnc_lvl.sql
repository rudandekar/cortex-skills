{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_deal_quote_dscnt_gdnc_lvl', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_N_DEAL_QUOTE_DSCNT_GDNC_LVL',
        'target_table': 'N_DEAL_QUOTE_DSCNT_GDNC_LVL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.901858+00:00'
    }
) }}

WITH 

source_n_deal_quote_dscnt_gdnc_lvl AS (
    SELECT
        bk_quote_num,
        bk_guidance_id_int,
        bk_guidance_level_name,
        src_rptd_rqstd_dscnt_usd_amt,
        src_rptd_gdnc_dscnt_usd_amt,
        src_rprtd_dscnt_discretion_pct,
        status_cd,
        approver_role_name,
        band_level_discretion_pct,
        bookings_upside_amt,
        dscnt_gdnc_rcmndtn_pct,
        bookings_upside_pct,
        barometer_needle_pct,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'n_deal_quote_dscnt_gdnc_lvl') }}
),

final AS (
    SELECT
        bk_quote_num,
        bk_guidance_id_int,
        bk_guidance_level_name,
        src_rptd_rqstd_dscnt_usd_amt,
        src_rptd_gdnc_dscnt_usd_amt,
        src_rprtd_dscnt_discretion_pct,
        status_cd,
        approver_role_name,
        band_level_discretion_pct,
        bookings_upside_amt,
        dscnt_gdnc_rcmndtn_pct,
        bookings_upside_pct,
        barometer_needle_pct,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_n_deal_quote_dscnt_gdnc_lvl
)

SELECT * FROM final