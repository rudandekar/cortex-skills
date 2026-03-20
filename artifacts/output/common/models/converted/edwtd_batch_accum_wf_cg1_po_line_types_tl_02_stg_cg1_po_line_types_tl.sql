{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_cg1_po_line_types_tl', 'batch', 'edwtd_batch_accum'],
    meta={
        'source_workflow': 'wf_m_CG1_PO_LINE_TYPES_TL',
        'target_table': 'STG_CG1_PO_LINE_TYPES_TL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:22:00.828817+00:00'
    }
) }}

WITH 

source_stg_cg1_po_line_types_tl AS (
    SELECT
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
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        processed_flag
    FROM {{ source('raw', 'stg_cg1_po_line_types_tl') }}
),

source_cg1_po_line_types_tl AS (
    SELECT
        source_dml_type,
        fully_qualified_table_name,
        source_commit_time,
        refresh_datetime,
        trail_position,
        token,
        line_type_id,
        language,
        source_lang,
        description,
        line_type,
        last_update_date,
        last_updated_by,
        last_update_login,
        creation_date,
        created_by
    FROM {{ source('raw', 'cg1_po_line_types_tl') }}
),

transformed_exp_cg1_po_line_types_tl AS (
    SELECT
    source_dml_type,
    source_commit_time,
    refresh_datetime,
    line_type_id,
    language,
    source_lang,
    description,
    line_type,
    last_update_date,
    last_updated_by,
    last_update_login,
    creation_date,
    created_by
    FROM source_cg1_po_line_types_tl
),

final AS (
    SELECT
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
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        processed_flag
    FROM transformed_exp_cg1_po_line_types_tl
)

SELECT * FROM final