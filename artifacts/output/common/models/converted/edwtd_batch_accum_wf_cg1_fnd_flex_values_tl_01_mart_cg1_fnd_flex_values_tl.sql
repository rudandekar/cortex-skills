{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_cg1_fnd_flex_values_tl', 'batch', 'edwtd_batch_accum'],
    meta={
        'source_workflow': 'wf_m_CG1_FND_FLEX_VALUES_TL',
        'target_table': 'CG1_FND_FLEX_VALUES_TL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:22:00.862657+00:00'
    }
) }}

WITH 

source_stg_cg1_fnd_flex_values_tl AS (
    SELECT
        flex_value_id,
        language_cd,
        last_update_date,
        last_updated_by,
        creation_date,
        created_by,
        last_update_login,
        description,
        source_lang,
        flex_value_meaning,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        processed_flag
    FROM {{ source('raw', 'stg_cg1_fnd_flex_values_tl') }}
),

source_cg1_fnd_flex_values_tl AS (
    SELECT
        source_dml_type,
        fully_qualified_table_name,
        source_commit_time,
        refresh_datetime,
        trail_position,
        token,
        flex_value_id,
        language,
        last_update_date,
        last_updated_by,
        creation_date,
        created_by,
        last_update_login,
        description,
        source_lang,
        flex_value_meaning
    FROM {{ source('raw', 'cg1_fnd_flex_values_tl') }}
),

transformed_exp_cg1_fnd_flex_values_tl AS (
    SELECT
    source_dml_type,
    source_commit_time,
    refresh_datetime,
    flex_value_id,
    language,
    last_update_date,
    last_updated_by,
    creation_date,
    created_by,
    last_update_login,
    description,
    source_lang,
    flex_value_meaning
    FROM source_cg1_fnd_flex_values_tl
),

final AS (
    SELECT
        flex_value_id,
        language_cd,
        last_update_date,
        last_updated_by,
        creation_date,
        created_by,
        last_update_login,
        description,
        source_lang,
        flex_value_meaning,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        processed_flag
    FROM transformed_exp_cg1_fnd_flex_values_tl
)

SELECT * FROM final