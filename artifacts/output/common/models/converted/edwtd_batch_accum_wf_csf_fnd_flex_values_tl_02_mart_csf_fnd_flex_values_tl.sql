{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_csf_fnd_flex_values_tl', 'batch', 'edwtd_batch_accum'],
    meta={
        'source_workflow': 'wf_m_CSF_FND_FLEX_VALUES_TL',
        'target_table': 'CSF_FND_FLEX_VALUES_TL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:22:00.726038+00:00'
    }
) }}

WITH 

source_stg_csf_fnd_flex_values_tl AS (
    SELECT
        flex_value_id,
        language_1,
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
        refresh_datetime
    FROM {{ source('raw', 'stg_csf_fnd_flex_values_tl') }}
),

source_csf_fnd_flex_values_tl AS (
    SELECT
        source_dml_type,
        fully_qualified_table_name,
        source_commit_time,
        refresh_datetime,
        trail_position,
        token,
        refresh_day,
        flex_value_id,
        language,
        last_update_date,
        last_updated_by,
        creation_date,
        created_by,
        last_update_login,
        description,
        source_lang,
        flex_value_meaning,
        zd_edition_name,
        zd_sync
    FROM {{ source('raw', 'csf_fnd_flex_values_tl') }}
),

transformed_exp_csf_fnd_flex_values_tl AS (
    SELECT
    source_dml_type,
    fully_qualified_table_name,
    source_commit_time,
    refresh_datetime,
    trail_position,
    token,
    refresh_day,
    flex_value_id,
    language,
    last_update_date,
    last_updated_by,
    creation_date,
    created_by,
    last_update_login,
    description,
    source_lang,
    flex_value_meaning,
    zd_edition_name,
    zd_sync
    FROM source_csf_fnd_flex_values_tl
),

final AS (
    SELECT
        flex_value_id,
        language_1,
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
        refresh_datetime
    FROM transformed_exp_csf_fnd_flex_values_tl
)

SELECT * FROM final