{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_cg1_xxgco_svc_errors_in_intf', 'batch', 'edwtd_batch_accum'],
    meta={
        'source_workflow': 'wf_m_CG1_XXGCO_SVC_ERRORS_IN_INTF',
        'target_table': 'STG_CG1_XXGCO_SVC_ERR_IN_INTF',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:22:00.584302+00:00'
    }
) }}

WITH 

source_stg_cg1_xxgco_svc_err_in_intf AS (
    SELECT
        line_int_sequence_id,
        header_id,
        line_id,
        error_code,
        error_message,
        status,
        referenceid,
        source_control_id,
        sourcesystem,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by,
        request_id,
        program_application_id,
        program_id,
        program_update_date,
        batch_id,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        processed_flag
    FROM {{ source('raw', 'stg_cg1_xxgco_svc_err_in_intf') }}
),

source_cg1_xxgco_svc_errors_in_intf AS (
    SELECT
        source_dml_type,
        fully_qualified_table_name,
        source_commit_time,
        refresh_datetime,
        trail_position,
        token,
        line_int_sequence_id,
        header_id,
        line_id,
        error_code,
        error_message,
        status,
        referenceid,
        source_control_id,
        sourcesystem,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by,
        request_id,
        program_application_id,
        program_id,
        program_update_date,
        batch_id
    FROM {{ source('raw', 'cg1_xxgco_svc_errors_in_intf') }}
),

transformed_exp_cg1_xxgco_svc_errors_in_intf AS (
    SELECT
    source_dml_type,
    source_commit_time,
    refresh_datetime,
    line_int_sequence_id,
    header_id,
    line_id,
    error_code,
    error_message,
    status,
    referenceid,
    source_control_id,
    sourcesystem,
    creation_date,
    created_by,
    last_update_date,
    last_updated_by,
    request_id,
    program_application_id,
    program_id,
    program_update_date,
    batch_id
    FROM source_cg1_xxgco_svc_errors_in_intf
),

final AS (
    SELECT
        line_int_sequence_id,
        header_id,
        line_id,
        error_code,
        error_message,
        status,
        referenceid,
        source_control_id,
        sourcesystem,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by,
        request_id,
        program_application_id,
        program_id,
        program_update_date,
        batch_id,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        processed_flag
    FROM transformed_exp_cg1_xxgco_svc_errors_in_intf
)

SELECT * FROM final