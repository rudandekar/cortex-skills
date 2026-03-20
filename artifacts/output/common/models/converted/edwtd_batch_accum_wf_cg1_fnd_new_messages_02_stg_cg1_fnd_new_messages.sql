{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_cg1_fnd_new_messages', 'batch', 'edwtd_batch_accum'],
    meta={
        'source_workflow': 'wf_m_CG1_FND_NEW_MESSAGES',
        'target_table': 'STG_CG1_FND_NEW_MESSAGES',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:22:00.633814+00:00'
    }
) }}

WITH 

source_stg_cg1_fnd_new_messages AS (
    SELECT
        application_id,
        language_code,
        message_number,
        message_name,
        message_text,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by,
        last_update_login,
        description,
        type1,
        max_length,
        category,
        severity,
        fnd_log_severity,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        processed_flag
    FROM {{ source('raw', 'stg_cg1_fnd_new_messages') }}
),

source_cg1_fnd_new_messages AS (
    SELECT
        source_dml_type,
        fully_qualified_table_name,
        source_commit_time,
        refresh_datetime,
        trail_position,
        token,
        application_id,
        language_code,
        message_number,
        message_name,
        message_text,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by,
        last_update_login,
        description,
        type,
        max_length,
        category,
        severity,
        fnd_log_severity
    FROM {{ source('raw', 'cg1_fnd_new_messages') }}
),

transformed_exp_cg1_fnd_new_messages AS (
    SELECT
    source_dml_type,
    source_commit_time,
    refresh_datetime,
    application_id,
    language_code,
    message_number,
    message_name,
    message_text,
    creation_date,
    created_by,
    last_update_date,
    last_updated_by,
    last_update_login,
    description,
    type,
    max_length,
    category,
    severity,
    fnd_log_severity
    FROM source_cg1_fnd_new_messages
),

final AS (
    SELECT
        application_id,
        language_code,
        message_number,
        message_name,
        message_text,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by,
        last_update_login,
        description,
        type1,
        max_length,
        category,
        severity,
        fnd_log_severity,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        processed_flag
    FROM transformed_exp_cg1_fnd_new_messages
)

SELECT * FROM final