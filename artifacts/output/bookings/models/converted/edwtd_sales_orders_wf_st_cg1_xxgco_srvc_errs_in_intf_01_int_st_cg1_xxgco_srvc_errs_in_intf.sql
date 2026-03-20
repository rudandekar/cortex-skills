{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_st_cg1_xxgco_srvc_errs_in_intf', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_ST_CG1_XXGCO_SRVC_ERRS_IN_INTF',
        'target_table': 'ST_CG1_XXGCO_SRVC_ERRS_IN_INTF',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.187784+00:00'
    }
) }}

WITH 

source_cg1_xxgco_svc_errors_in_intf AS (
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
    FROM {{ source('raw', 'cg1_xxgco_svc_errors_in_intf') }}
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
        src_batch_id,
        batch_id,
        create_datetime,
        action_code,
        global_name,
        source_commit_time,
        refresh_datetime
    FROM source_cg1_xxgco_svc_errors_in_intf
)

SELECT * FROM final