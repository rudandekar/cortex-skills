{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_ood_fusn_order_lines_all', 'batch', 'edwtd_ood'],
    meta={
        'source_workflow': 'wf_m_FF_OOD_FUSN_ORDER_LINES_ALL',
        'target_table': 'FF_OOD_FUSN_ORDER_LINES_ALL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.062838+00:00'
    }
) }}

WITH 

source_saas_oe_order_lines_all AS (
    SELECT
        xpk_root,
        xpk_oe_lines,
        fk_root,
        line_id,
        line_number,
        ordered_item,
        orig_sys_document_ref,
        orig_sys_line_ref,
        creation_date,
        last_update_date,
        split_key
    FROM {{ source('raw', 'saas_oe_order_lines_all') }}
),

xml_parsed_xmldsq_saas_oe_order_lines_all AS (
    SELECT
        src.*,
        xml_record.VALUE AS xml_content
    FROM source_saas_oe_order_lines_all src,
    LATERAL FLATTEN(INPUT => PARSE_XML(src.xml_data):"root") xml_record
),

transformed_exp_ood_fusn_order_lines_all AS (
    SELECT
    line_id,
    line_number,
    ordered_item,
    orig_sys_document_ref,
    orig_sys_line_ref,
    creation_date,
    last_update_date,
    split_key,
    CURRENT_TIMESTAMP() AS create_datetime,
    'I' AS action_code
    FROM xml_parsed_xmldsq_saas_oe_order_lines_all
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
    FROM transformed_exp_ood_fusn_order_lines_all
)

SELECT * FROM final