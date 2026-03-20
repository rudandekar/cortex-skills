{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_xxsdg_quote_hdr_guidance_v', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_FF_XXSDG_QUOTE_HDR_GUIDANCE_V',
        'target_table': 'FF_XXSDG_QUOTE_HDR_GUIDANCE_V',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.936026+00:00'
    }
) }}

WITH 

source_xxsdg_quote_header_guidance_v AS (
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
    FROM {{ source('raw', 'xxsdg_quote_header_guidance_v') }}
),

final AS (
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
    FROM source_xxsdg_quote_header_guidance_v
)

SELECT * FROM final