{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_csf_fnd_descr_flex_contexts_tl', 'batch', 'edwtd_batch_accum'],
    meta={
        'source_workflow': 'wf_m_CSF_FND_DESCR_FLEX_CONTEXTS_TL',
        'target_table': 'CSF_FND_DESCR_FLEX_CONTEXTS_TL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:22:01.086966+00:00'
    }
) }}

WITH 

source_stg_csf_fnd_descr_flex_contexts_tl AS (
    SELECT
        application_id,
        descriptive_flexfield_name,
        descriptive_flex_context_code,
        language_,
        last_update_date,
        last_updated_by,
        creation_date,
        created_by,
        last_update_login,
        description,
        descriptive_flex_context_name,
        source_lang,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime
    FROM {{ source('raw', 'stg_csf_fnd_descr_flex_contexts_tl') }}
),

source_csf_fnd_descr_flex_contexts_tl AS (
    SELECT
        source_dml_type,
        fully_qualified_table_name,
        source_commit_time,
        refresh_datetime,
        trail_position,
        token,
        refresh_day,
        application_id,
        descriptive_flexfield_name,
        descriptive_flex_context_code,
        language,
        last_update_date,
        last_updated_by,
        creation_date,
        created_by,
        last_update_login,
        description,
        descriptive_flex_context_name,
        source_lang,
        zd_edition_name,
        zd_sync
    FROM {{ source('raw', 'csf_fnd_descr_flex_contexts_tl') }}
),

transformed_exp_csf_fnd_descr_flex_contexts_tl AS (
    SELECT
    source_dml_type,
    fully_qualified_table_name,
    source_commit_time,
    refresh_datetime,
    trail_position,
    token,
    refresh_day,
    application_id,
    descriptive_flexfield_name,
    descriptive_flex_context_code,
    language,
    last_update_date,
    last_updated_by,
    creation_date,
    created_by,
    last_update_login,
    description,
    descriptive_flex_context_name,
    source_lang,
    zd_edition_name,
    zd_sync
    FROM source_csf_fnd_descr_flex_contexts_tl
),

final AS (
    SELECT
        application_id,
        descriptive_flexfield_name,
        descriptive_flex_context_code,
        language_,
        last_update_date,
        last_updated_by,
        creation_date,
        created_by,
        last_update_login,
        description,
        descriptive_flex_context_name,
        source_lang,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime
    FROM transformed_exp_csf_fnd_descr_flex_contexts_tl
)

SELECT * FROM final