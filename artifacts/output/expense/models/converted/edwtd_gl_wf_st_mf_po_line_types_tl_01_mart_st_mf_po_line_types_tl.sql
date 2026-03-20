{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_mf_po_line_types_tl', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_ST_MF_PO_LINE_TYPES_TL',
        'target_table': 'ST_MF_PO_LINE_TYPES_TL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.602564+00:00'
    }
) }}

WITH 

source_ff_mf_po_line_types_tl AS (
    SELECT
        batch_id,
        created_by,
        creation_date,
        description,
        language_code,
        last_update_date,
        last_updated_by,
        line_type,
        line_type_id,
        source_lang,
        ges_update_date,
        global_name,
        create_datetime,
        action_code
    FROM {{ source('raw', 'ff_mf_po_line_types_tl') }}
),

final AS (
    SELECT
        batch_id,
        created_by,
        creation_date,
        description,
        language_code,
        last_update_date,
        last_updated_by,
        line_type,
        line_type_id,
        source_lang,
        ges_update_date,
        global_name,
        create_datetime,
        action_code
    FROM source_ff_mf_po_line_types_tl
)

SELECT * FROM final