{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_xxsdg_quote_line_guidance_v', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_FF_XXSDG_QUOTE_LINE_GUIDANCE_V',
        'target_table': 'FF_XXSDG_QUOTE_LINE_GUIDANCE_V',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.921708+00:00'
    }
) }}

WITH 

source_xxsdg_quote_line_guidance_v AS (
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
    FROM {{ source('raw', 'xxsdg_quote_line_guidance_v') }}
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
        status,
        approver_role,
        band,
        bookings_upside,
        recommendation,
        bookings_upside_pct,
        barometer_needle
    FROM source_xxsdg_quote_line_guidance_v
)

SELECT * FROM final