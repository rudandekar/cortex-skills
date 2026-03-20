{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_cg1_fnd_languages', 'batch', 'edwtd_batch_accum'],
    meta={
        'source_workflow': 'wf_m_CG1_FND_LANGUAGES',
        'target_table': 'STG_CG1_FND_LANGUAGES',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:22:00.795943+00:00'
    }
) }}

WITH 

source_cg1_fnd_languages AS (
    SELECT
        source_dml_type,
        fully_qualified_table_name,
        source_commit_time,
        refresh_datetime,
        trail_position,
        token,
        language_code,
        language_id,
        nls_language,
        nls_territory,
        iso_language,
        iso_territory,
        nls_codeset,
        installed_flag,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        last_update_login,
        local_date_language,
        utf8_date_language,
        iso_language_3
    FROM {{ source('raw', 'cg1_fnd_languages') }}
),

source_stg_cg1_fnd_languages AS (
    SELECT
        language_code,
        language_id,
        nls_language,
        nls_territory,
        iso_language,
        iso_territory,
        nls_codeset,
        installed_flag,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        last_update_login,
        local_date_language,
        utf8_date_language,
        iso_language_3,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        processed_flag
    FROM {{ source('raw', 'stg_cg1_fnd_languages') }}
),

transformed_exp_cg1_fnd_languages AS (
    SELECT
    source_dml_type,
    source_commit_time,
    refresh_datetime,
    language_code,
    language_id,
    nls_language,
    nls_territory,
    iso_language,
    iso_territory,
    nls_codeset,
    installed_flag,
    created_by,
    creation_date,
    last_updated_by,
    last_update_date,
    last_update_login,
    local_date_language,
    utf8_date_language,
    iso_language_3
    FROM source_stg_cg1_fnd_languages
),

final AS (
    SELECT
        language_code,
        language_id,
        nls_language,
        nls_territory,
        iso_language,
        iso_territory,
        nls_codeset,
        installed_flag,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        last_update_login,
        local_date_language,
        utf8_date_language,
        iso_language_3,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        processed_flag
    FROM transformed_exp_cg1_fnd_languages
)

SELECT * FROM final