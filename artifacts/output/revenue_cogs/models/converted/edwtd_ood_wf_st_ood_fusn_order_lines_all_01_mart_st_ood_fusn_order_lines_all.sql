{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_ood_fusn_order_lines_all', 'batch', 'edwtd_ood'],
    meta={
        'source_workflow': 'wf_m_ST_OOD_FUSN_ORDER_LINES_ALL',
        'target_table': 'ST_OOD_FUSN_ORDER_LINES_ALL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.067072+00:00'
    }
) }}

WITH 

source_ff_ood_fusn_order_lines_all AS (
    SELECT
        line_id,
        line_number,
        ordered_item,
        orig_sys_document_ref,
        orig_sys_line_ref,
        creation_date,
        last_update_date,
        split_key,
        action_code,
        create_datetime
    FROM {{ source('raw', 'ff_ood_fusn_order_lines_all') }}
),

filtered_fil_ood_fusn_order_lines_all AS (
    SELECT *
    FROM source_ff_ood_fusn_order_lines_all
    WHERE SUBSTR(ORIG_SYS_DOCUMENT_REF,1,16)!='OE_ORDER_HEADERS' AND SUBSTR(ORIG_SYS_LINE_REF,1,14)!='OE_ORDER_LINES'
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
        split_key,
        action_code,
        create_datetime
    FROM filtered_fil_ood_fusn_order_lines_all
)

SELECT * FROM final