{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_deal_quote_dscnt_gdnc_dtl', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_W_DEAL_QUOTE_DSCNT_GDNC_DTL',
        'target_table': 'EX_XXSDG_QUOTE_LINE_GUIDANCE_V',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.964435+00:00'
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

source_st_xxsdg_quote_line_guidance_v AS (
    SELECT
        header_request_id,
        header_guidance_id,
        line_request_id,
        line_guidance_id,
        deal_object_id,
        quote_object_id,
        guidance_level,
        primary_product_family,
        original_product_family,
        line_object_id,
        line_type_id,
        part_number,
        requested_discount,
        guidance_discount,
        discount_discretion,
        is_final,
        in_sales_dashboard,
        updated_by,
        updated_on,
        status,
        approver_role,
        band,
        bookings_upside,
        recommendation,
        bookings_upside_pct,
        barometer_needle
    FROM {{ source('raw', 'st_xxsdg_quote_line_guidance_v') }}
),

source_ex_xxsdg_quote_line_guidance_v AS (
    SELECT
        header_request_id,
        header_guidance_id,
        line_request_id,
        line_guidance_id,
        deal_object_id,
        quote_object_id,
        guidance_level,
        primary_product_family,
        original_product_family,
        line_object_id,
        line_type_id,
        part_number,
        requested_discount,
        guidance_discount,
        discount_discretion,
        is_final,
        in_sales_dashboard,
        updated_by,
        updated_on,
        exception_type,
        status,
        approver_role,
        band,
        bookings_upside,
        recommendation,
        bookings_upside_pct,
        barometer_needle
    FROM {{ source('raw', 'ex_xxsdg_quote_line_guidance_v') }}
),

final AS (
    SELECT
        header_request_id,
        header_guidance_id,
        line_request_id,
        line_guidance_id,
        deal_object_id,
        quote_object_id,
        guidance_level,
        primary_product_family,
        original_product_family,
        line_object_id,
        line_type_id,
        part_number,
        requested_discount,
        guidance_discount,
        discount_discretion,
        is_final,
        in_sales_dashboard,
        updated_by,
        updated_on,
        exception_type,
        status,
        approver_role,
        band,
        bookings_upside,
        recommendation,
        bookings_upside_pct,
        barometer_needle
    FROM source_ex_xxsdg_quote_line_guidance_v
)

SELECT * FROM final