{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_om_xxcca_svc_ctrt_line_errs', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_WI_OM_XXCCA_SVC_CTRT_LINE_ERRS',
        'target_table': 'WI_OM_XXCCA_SVC_CTRT_LINE_ERRS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.901129+00:00'
    }
) }}

WITH 

source_st_uo_xxcca_svc_ctrt_line_errs AS (
    SELECT
        line_error_id,
        line_id,
        error_message_code,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by,
        last_update_login,
        request_id,
        program_application_id,
        program_id,
        program_update_date,
        valid_flag,
        batch_id,
        create_datetime,
        action_code,
        global_name,
        source_commit_time,
        refresh_datetime
    FROM {{ source('raw', 'st_uo_xxcca_svc_ctrt_line_errs') }}
),

final AS (
    SELECT
        line_error_id,
        line_id,
        error_message_code,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by,
        last_update_login,
        request_id,
        program_application_id,
        program_id,
        program_update_date,
        valid_flag,
        batch_id,
        create_datetime,
        action_code,
        global_name,
        source_commit_time,
        refresh_datetime
    FROM source_st_uo_xxcca_svc_ctrt_line_errs
)

SELECT * FROM final