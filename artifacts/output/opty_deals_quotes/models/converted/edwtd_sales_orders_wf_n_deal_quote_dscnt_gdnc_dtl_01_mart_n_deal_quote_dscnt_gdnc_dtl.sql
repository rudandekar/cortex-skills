{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_deal_quote_dscnt_gdnc_dtl', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_N_DEAL_QUOTE_DSCNT_GDNC_DTL',
        'target_table': 'N_DEAL_QUOTE_DSCNT_GDNC_DTL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.884411+00:00'
    }
) }}

WITH 

source_w_deal_quote_dscnt_gdnc_dtl AS (
    SELECT
        bk_deal_quote_line_key,
        bk_detail_guidance_id_int,
        guidance_level_cd,
        requested_discount_usd_amt,
        guidance_discount_usd_amt,
        discount_discretion_pct,
        is_finalized_flg,
        in_sales_dashboard_flg,
        src_rprtd_primary_prdt_fmly_id,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        dtl_status_cd,
        dtl_approver_role_name,
        dtl_band_level_dscrtn_pct,
        dtl_bookings_upside_amt,
        dtl_dscnt_gdnc_rcmndtn_pct,
        dtl_bookings_upside_pct,
        cmrs_major_prdt_family_id,
        dtl_barometer_needle_pct,
        sk_request_id_int,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_deal_quote_dscnt_gdnc_dtl') }}
),

final AS (
    SELECT
        bk_deal_quote_line_key,
        bk_detail_guidance_id_int,
        guidance_level_cd,
        requested_discount_usd_amt,
        guidance_discount_usd_amt,
        discount_discretion_pct,
        is_finalized_flg,
        in_sales_dashboard_flg,
        src_rprtd_primary_prdt_fmly_id,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        dtl_status_cd,
        dtl_approver_role_name,
        dtl_band_level_dscrtn_pct,
        dtl_bookings_upside_amt,
        dtl_dscnt_gdnc_rcmndtn_pct,
        dtl_bookings_upside_pct,
        cmrs_major_prdt_family_id,
        dtl_barometer_needle_pct,
        sk_request_id_int
    FROM source_w_deal_quote_dscnt_gdnc_dtl
)

SELECT * FROM final