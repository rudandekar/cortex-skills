{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_opl_xxccp_oe_order_header', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_ST_OPL_XXCCP_OE_ORDER_HEADER',
        'target_table': 'ST_OPL_XXCCP_OE_ORDER_HEADER',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.464096+00:00'
    }
) }}

WITH 

source_st_opl_xxccp_oe_order_header AS (
    SELECT
        xxccp_header_id,
        quote_number,
        erp_header_id,
        orig_sys_document_ref,
        global_name,
        operation,
        creation_date,
        last_update_date,
        source_commit_time
    FROM {{ source('raw', 'st_opl_xxccp_oe_order_header') }}
),

final AS (
    SELECT
        xxccp_header_id,
        quote_number,
        erp_header_id,
        orig_sys_document_ref,
        global_name,
        operation,
        creation_date,
        last_update_date,
        source_commit_time
    FROM source_st_opl_xxccp_oe_order_header
)

SELECT * FROM final