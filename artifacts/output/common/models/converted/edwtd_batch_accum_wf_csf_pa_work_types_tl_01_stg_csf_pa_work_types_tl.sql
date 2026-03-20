{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_csf_pa_work_types_tl', 'batch', 'edwtd_batch_accum'],
    meta={
        'source_workflow': 'wf_m_CSF_PA_WORK_TYPES_TL',
        'target_table': 'STG_CSF_PA_WORK_TYPES_TL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:22:00.578877+00:00'
    }
) }}

WITH 

source_csf_pa_work_types_tl AS (
    SELECT
        source_dml_type,
        fully_qualified_table_name,
        source_commit_time,
        refresh_datetime,
        trail_position,
        token,
        refresh_day,
        work_type_id,
        language,
        source_lang,
        name,
        description,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by,
        last_update_login
    FROM {{ source('raw', 'csf_pa_work_types_tl') }}
),

source_stg_csf_pa_work_types_tl AS (
    SELECT
        work_type_id,
        language_,
        source_lang,
        name,
        description,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by,
        last_update_login,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime
    FROM {{ source('raw', 'stg_csf_pa_work_types_tl') }}
),

transformed_exp_csf_pa_work_types_tl AS (
    SELECT
    source_dml_type,
    fully_qualified_table_name,
    source_commit_time,
    refresh_datetime,
    trail_position,
    token,
    refresh_day,
    work_type_id,
    language,
    source_lang,
    name,
    description,
    creation_date,
    created_by,
    last_update_date,
    last_updated_by,
    last_update_login
    FROM source_stg_csf_pa_work_types_tl
),

final AS (
    SELECT
        work_type_id,
        language_,
        source_lang,
        name,
        description,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by,
        last_update_login,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime
    FROM transformed_exp_csf_pa_work_types_tl
)

SELECT * FROM final