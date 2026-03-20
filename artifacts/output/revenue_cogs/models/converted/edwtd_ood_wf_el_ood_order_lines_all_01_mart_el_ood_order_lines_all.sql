{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_ood_order_lines_all', 'batch', 'edwtd_ood'],
    meta={
        'source_workflow': 'wf_m_EL_OOD_ORDER_LINES_ALL',
        'target_table': 'EL_OOD_ORDER_LINES_ALL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.726440+00:00'
    }
) }}

WITH 

source_st_ood_order_lines_all AS (
    SELECT
        line_id,
        line_number,
        ordered_item,
        orig_sys_document_ref,
        orig_sys_line_ref,
        creation_date,
        last_update_date,
        action_code,
        create_datetime
    FROM {{ source('raw', 'st_ood_order_lines_all') }}
),

final AS (
    SELECT
        line_id,
        line_number,
        ordered_item,
        orig_sys_document_ref,
        orig_sys_line_ref,
        creation_date,
        last_update_date,
        action_code,
        edw_update_dtm,
        ss_code,
        identifier
    FROM source_st_ood_order_lines_all
)

SELECT * FROM final