{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_deal_quote_discount_guidance', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_W_DEAL_QUOTE_DISCOUNT_GUIDANCE',
        'target_table': 'W_DEAL_QUOTE_DISCOUNT_GUIDANCE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.915216+00:00'
    }
) }}

WITH 

source_ex_xxsdg_quote_hdr_guidance_v AS (
    SELECT
        request_id,
        guidance_id,
        deal_object_id,
        quote_object_id,
        is_final,
        in_sales_dashboard,
        guidance_level,
        requested_discount,
        guidance_discount,
        discount_discretion,
        requested_by,
        requested_on,
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
    FROM {{ source('raw', 'ex_xxsdg_quote_hdr_guidance_v') }}
),

source_st_xxsdg_quote_hdr_guidance_v AS (
    SELECT
        request_id,
        guidance_id,
        deal_object_id,
        quote_object_id,
        is_final,
        in_sales_dashboard,
        guidance_level,
        requested_discount,
        guidance_discount,
        discount_discretion,
        requested_by,
        requested_on,
        updated_by,
        updated_on,
        status,
        approver_role,
        band,
        bookings_upside,
        recommendation,
        bookings_upside_pct,
        barometer_needle
    FROM {{ source('raw', 'st_xxsdg_quote_hdr_guidance_v') }}
),

source_w_deal_quote_discount_guidance AS (
    SELECT
        bk_quote_num,
        bk_guidance_id_int,
        ru_requested_by_user_id,
        is_finalized_flg,
        in_sales_dashboard_flg,
        requested_on_dt,
        requsted_by_user_role,
        sk_request_id_int,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_deal_quote_discount_guidance') }}
),

final AS (
    SELECT
        bk_quote_num,
        bk_guidance_id_int,
        ru_requested_by_user_id,
        is_finalized_flg,
        in_sales_dashboard_flg,
        requested_on_dt,
        requsted_by_user_role,
        sk_request_id_int,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM source_w_deal_quote_discount_guidance
)

SELECT * FROM final