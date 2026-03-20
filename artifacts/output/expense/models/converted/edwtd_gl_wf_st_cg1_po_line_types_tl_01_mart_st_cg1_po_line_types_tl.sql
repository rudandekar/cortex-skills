{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_cg1_po_line_types_tl', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_ST_CG1_PO_LINE_TYPES_TL',
        'target_table': 'ST_CG1_PO_LINE_TYPES_TL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.687310+00:00'
    }
) }}

WITH 

source_st_cg1_po_line_types_tl AS (
    SELECT
        batch_id,
        action_code,
        line_type_id,
        language_code,
        source_lang,
        description,
        line_type,
        last_update_date,
        last_updated_by,
        last_update_login,
        creation_date,
        created_by,
        source_commit_time,
        refresh_datetime,
        global_name,
        create_datetime
    FROM {{ source('raw', 'st_cg1_po_line_types_tl') }}
),

final AS (
    SELECT
        batch_id,
        action_code,
        line_type_id,
        language_code,
        source_lang,
        description,
        line_type,
        last_update_date,
        last_updated_by,
        last_update_login,
        creation_date,
        created_by,
        source_commit_time,
        refresh_datetime,
        global_name,
        create_datetime
    FROM source_st_cg1_po_line_types_tl
)

SELECT * FROM final