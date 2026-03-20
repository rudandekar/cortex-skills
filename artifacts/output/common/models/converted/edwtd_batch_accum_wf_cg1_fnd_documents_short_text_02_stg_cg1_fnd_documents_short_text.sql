{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_cg1_fnd_documents_short_text', 'batch', 'edwtd_batch_accum'],
    meta={
        'source_workflow': 'wf_m_CG1_FND_DOCUMENTS_SHORT_TEXT',
        'target_table': 'STG_CG1_FND_DOCUMENTS_SHORT_TEXT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:22:00.990338+00:00'
    }
) }}

WITH 

source_cg1_fnd_documents_short_text AS (
    SELECT
        source_dml_type,
        fully_qualified_table_name,
        source_commit_time,
        refresh_datetime,
        trail_position,
        token,
        refresh_day,
        media_id,
        short_text,
        app_source_version
    FROM {{ source('raw', 'cg1_fnd_documents_short_text') }}
),

source_stg_cg1_fnd_documents_short_text AS (
    SELECT
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        media_id,
        short_text,
        app_source_version
    FROM {{ source('raw', 'stg_cg1_fnd_documents_short_text') }}
),

transformed_exptrans AS (
    SELECT
    source_dml_type,
    source_commit_time,
    refresh_datetime,
    media_id,
    short_text,
    app_source_version
    FROM source_stg_cg1_fnd_documents_short_text
),

final AS (
    SELECT
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        media_id,
        short_text,
        app_source_version
    FROM transformed_exptrans
)

SELECT * FROM final