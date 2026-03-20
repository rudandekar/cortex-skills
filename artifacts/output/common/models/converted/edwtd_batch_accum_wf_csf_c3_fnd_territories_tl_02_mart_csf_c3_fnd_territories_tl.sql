{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_csf_c3_fnd_territories_tl', 'batch', 'edwtd_batch_accum'],
    meta={
        'source_workflow': 'wf_m_CSF_C3_FND_TERRITORIES_TL',
        'target_table': 'CSF_C3_FND_TERRITORIES_TL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:22:00.794406+00:00'
    }
) }}

WITH 

source_stg_c3_fnd_territories_tl AS (
    SELECT
        territory_code,
        lnguage,
        territory_short_name,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        last_update_login,
        description,
        source_lang,
        source_dml_type,
        source_commit_time,
        refresh_datetime
    FROM {{ source('raw', 'stg_c3_fnd_territories_tl') }}
),

source_csf_fnd_territories_tl AS (
    SELECT
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        territory_code,
        language,
        territory_short_name,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        last_update_login,
        description,
        source_lang
    FROM {{ source('raw', 'csf_fnd_territories_tl') }}
),

transformed_exp_csf_fnd_territories_tl AS (
    SELECT
    source_dml_type,
    source_commit_time,
    refresh_datetime,
    territory_code,
    language,
    territory_short_name,
    created_by,
    creation_date,
    last_updated_by,
    last_update_date,
    last_update_login,
    description,
    source_lang
    FROM source_csf_fnd_territories_tl
),

final AS (
    SELECT
        territory_code,
        lnguage,
        territory_short_name,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        last_update_login,
        description,
        source_lang,
        source_dml_type,
        source_commit_time,
        refresh_datetime
    FROM transformed_exp_csf_fnd_territories_tl
)

SELECT * FROM final