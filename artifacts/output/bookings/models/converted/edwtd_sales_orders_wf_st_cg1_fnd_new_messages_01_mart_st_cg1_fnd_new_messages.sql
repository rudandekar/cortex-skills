{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_cg1_fnd_new_messages', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_ST_CG1_FND_NEW_MESSAGES',
        'target_table': 'ST_CG1_FND_NEW_MESSAGES',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.371369+00:00'
    }
) }}

WITH 

source_cg1_fnd_new_messages AS (
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
    FROM {{ source('raw', 'cg1_fnd_new_messages') }}
),

final AS (
    SELECT
        application_id,
        language_code,
        message_number,
        message_name,
        message_text,
        global_name,
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
        source_commit_time
    FROM source_cg1_fnd_new_messages
)

SELECT * FROM final