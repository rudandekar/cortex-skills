{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_csf_xxcas_prj_asf_ca_status', 'batch', 'edwtd_batch_accum'],
    meta={
        'source_workflow': 'wf_m_CSF_XXCAS_PRJ_ASF_CA_STATUS',
        'target_table': 'STG_CSF_XXCAS_PRJ_ASF_CA_STATUS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:22:01.027554+00:00'
    }
) }}

WITH 

source_csf_xxcas_prj_asf_ca_status AS (
    SELECT
        source_dml_type,
        fully_qualified_table_name,
        source_commit_time,
        refresh_datetime,
        trail_position,
        token,
        refresh_day,
        project_id,
        cust_email_address,
        ca_request_id,
        request_status,
        rejection_reason,
        acted_by,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        last_updated_login,
        reminder_sent_flag,
        ogg_key_id
    FROM {{ source('raw', 'csf_xxcas_prj_asf_ca_status') }}
),

source_stg_csf_xxcas_prj_asf_ca_status AS (
    SELECT
        project_id,
        cust_email_address,
        ca_request_id,
        request_status,
        rejection_reason,
        acted_by,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        last_updated_login,
        reminder_sent_flag,
        ogg_key_id,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime
    FROM {{ source('raw', 'stg_csf_xxcas_prj_asf_ca_status') }}
),

transformed_exp_csf_xxcas_prj_asf_ca_status AS (
    SELECT
    source_dml_type,
    fully_qualified_table_name,
    source_commit_time,
    refresh_datetime,
    trail_position,
    token,
    refresh_day,
    project_id,
    cust_email_address,
    ca_request_id,
    request_status,
    rejection_reason,
    acted_by,
    created_by,
    creation_date,
    last_updated_by,
    last_update_date,
    last_updated_login,
    reminder_sent_flag,
    ogg_key_id
    FROM source_stg_csf_xxcas_prj_asf_ca_status
),

final AS (
    SELECT
        project_id,
        cust_email_address,
        ca_request_id,
        request_status,
        rejection_reason,
        acted_by,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        last_updated_login,
        reminder_sent_flag,
        ogg_key_id,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime
    FROM transformed_exp_csf_xxcas_prj_asf_ca_status
)

SELECT * FROM final