{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_cdc_fnd_application_tl', 'batch', 'edwtd_batch_accum'],
    meta={
        'source_workflow': 'wf_m_CDC_FND_APPLICATION_TL',
        'target_table': 'STG_CDC_FND_APPLICATION_TL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:22:00.746494+00:00'
    }
) }}

WITH 

source_cdc_fnd_application_tl AS (
    SELECT
        source_dml_type,
        fully_qualified_table_name,
        source_commit_time,
        refresh_datetime,
        trail_position,
        token,
        refresh_day,
        application_id,
        language,
        application_name,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        last_update_login,
        description,
        source_lang,
        zd_edition_name,
        zd_sync
    FROM {{ source('raw', 'cdc_fnd_application_tl') }}
),

source_stg_cdc_fnd_application_tl AS (
    SELECT
        application_id,
        lang,
        application_name,
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
    FROM {{ source('raw', 'stg_cdc_fnd_application_tl') }}
),

transformed_exp_cdc_fnd_application_tl AS (
    SELECT
    source_dml_type,
    source_commit_time,
    refresh_datetime,
    application_id,
    language,
    application_name,
    created_by,
    creation_date,
    last_updated_by,
    last_update_date,
    last_update_login,
    description,
    source_lang,
    processed_flag,
    trail_file_name
    FROM source_stg_cdc_fnd_application_tl
),

final AS (
    SELECT
        application_id,
        lang,
        application_name,
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
    FROM transformed_exp_cdc_fnd_application_tl
)

SELECT * FROM final