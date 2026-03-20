{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_cg1_fnd_currencies_tl', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_ST_CG1_FND_CURRENCIES_TL',
        'target_table': 'ST_CG1_FND_CURRENCIES_TL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.729172+00:00'
    }
) }}

WITH 

source_cg1_fnd_currencies_tl AS (
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
    FROM {{ source('raw', 'cg1_fnd_currencies_tl') }}
),

final AS (
    SELECT
        batch_id,
        action_code,
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
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        global_name,
        create_datetime
    FROM source_cg1_fnd_currencies_tl
)

SELECT * FROM final