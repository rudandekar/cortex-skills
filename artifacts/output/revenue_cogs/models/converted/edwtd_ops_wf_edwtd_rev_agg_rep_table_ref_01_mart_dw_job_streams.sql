{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_rep_table_refresh', 'batch', 'edwtd_ops'],
    meta={
        'source_workflow': 'wf_m_REP_TABLE_REFRESH',
        'target_table': 'DW_JOB_STREAMS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.710397+00:00'
    }
) }}

WITH 

source_dw_job_streams AS (
    SELECT
        job_stream_id,
        job_stream_name,
        job_group_id,
        job_stream_seq_num,
        active_ind,
        job_stream_status_code,
        descn,
        system_call,
        run_frequency,
        dollaru_uproc_name,
        last_extract_date,
        batch_id,
        estimate_start_date,
        estimate_end_date,
        average_run_time,
        weightage_percent,
        edw_create_datetime,
        edw_create_user,
        edw_update_datetime,
        edw_update_user,
        source_table_name,
        source_extract_date_col_name,
        target_table_name,
        last_extract_id
    FROM {{ source('raw', 'dw_job_streams') }}
),

final AS (
    SELECT
        job_stream_id,
        job_stream_name,
        job_group_id,
        job_stream_seq_num,
        active_ind,
        job_stream_status_code,
        descn,
        system_call,
        run_frequency,
        dollaru_uproc_name,
        last_extract_date,
        batch_id,
        estimate_start_date,
        estimate_end_date,
        average_run_time,
        weightage_percent,
        edw_create_datetime,
        edw_create_user,
        edw_update_datetime,
        edw_update_user,
        source_table_name,
        source_extract_date_col_name,
        target_table_name
    FROM source_dw_job_streams
)

SELECT * FROM final