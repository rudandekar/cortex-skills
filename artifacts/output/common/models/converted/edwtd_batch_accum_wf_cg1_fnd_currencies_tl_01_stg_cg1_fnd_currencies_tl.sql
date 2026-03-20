{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_cg1_fnd_currencies_tl', 'batch', 'edwtd_batch_accum'],
    meta={
        'source_workflow': 'wf_m_CG1_FND_CURRENCIES_TL',
        'target_table': 'STG_CG1_FND_CURRENCIES_TL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:22:00.648790+00:00'
    }
) }}

WITH 

source_cg1_fnd_currencies_tl AS (
    SELECT
        source_dml_type,
        fully_qualified_table_name,
        source_commit_time,
        refresh_datetime,
        trail_position,
        token,
        currency_code,
        language,
        name,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        last_update_login,
        description,
        source_lang
    FROM {{ source('raw', 'cg1_fnd_currencies_tl') }}
),

source_stg_cg1_fnd_currencies_tl AS (
    SELECT
        currency_code,
        name,
        language_code,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        last_update_login,
        description,
        source_lang,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        processed_flag
    FROM {{ source('raw', 'stg_cg1_fnd_currencies_tl') }}
),

transformed_exp_cg1_fnd_currencies_tl AS (
    SELECT
    source_dml_type,
    source_commit_time,
    refresh_datetime,
    currency_code,
    language,
    name,
    created_by,
    creation_date,
    last_updated_by,
    last_update_date,
    last_update_login,
    description,
    source_lang
    FROM source_stg_cg1_fnd_currencies_tl
),

final AS (
    SELECT
        currency_code,
        name,
        language_code,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        last_update_login,
        description,
        source_lang,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        processed_flag
    FROM transformed_exp_cg1_fnd_currencies_tl
)

SELECT * FROM final