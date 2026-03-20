{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_csf_oe_order_headers_all', 'batch', 'edwtd_migration'],
    meta={
        'source_workflow': 'wf_m_FF_CSF_OE_ORDER_HEADERS_ALL',
        'target_table': 'FF_CSF_OE_ORDER_HEADERS_ALL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.182711+00:00'
    }
) }}

WITH 

source_ff_csf_oe_order_headers_all AS (
    SELECT
        header_id,
        order_number,
        orig_sys_document_ref,
        creation_date,
        context,
        order_type,
        edw_create_dtm
    FROM {{ source('raw', 'ff_csf_oe_order_headers_all') }}
),

source_csf_oe_order_headers_all AS (
    SELECT
        header_id,
        order_number,
        orig_sys_document_ref,
        creation_date,
        context,
        order_type
    FROM {{ source('raw', 'csf_oe_order_headers_all') }}
),

final AS (
    SELECT
        header_id,
        order_number,
        orig_sys_document_ref,
        creation_date,
        context,
        order_type,
        edw_create_dtm
    FROM source_csf_oe_order_headers_all
)

SELECT * FROM final