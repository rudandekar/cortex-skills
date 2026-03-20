{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_cg1_fnd_doc_long_text', 'batch', 'edwtd_batch_accum'],
    meta={
        'source_workflow': 'wf_m_CG1_FND_DOC_LONG_TEXT',
        'target_table': 'STG_CG1_FND_DOC_LONG_TEXT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:22:00.649097+00:00'
    }
) }}

WITH 

source_stg_cg1_fnd_doc_long_text AS (
    SELECT
        media_id,
        long_text,
        app_source_version,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        processed_flag
    FROM {{ source('raw', 'stg_cg1_fnd_doc_long_text') }}
),

source_cg1_fnd_documents_long_text AS (
    SELECT
        source_dml_type,
        fully_qualified_table_name,
        source_commit_time,
        refresh_datetime,
        trail_position,
        token,
        refresh_day,
        media_id,
        app_source_version
    FROM {{ source('raw', 'cg1_fnd_documents_long_text') }}
),

transformed_exp_cg1_fnd_doc_long_text AS (
    SELECT
    source_dml_type,
    source_commit_time,
    refresh_datetime,
    media_id,
    app_source_version
    FROM source_cg1_fnd_documents_long_text
),

final AS (
    SELECT
        media_id,
        long_text,
        app_source_version,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        processed_flag
    FROM transformed_exp_cg1_fnd_doc_long_text
)

SELECT * FROM final