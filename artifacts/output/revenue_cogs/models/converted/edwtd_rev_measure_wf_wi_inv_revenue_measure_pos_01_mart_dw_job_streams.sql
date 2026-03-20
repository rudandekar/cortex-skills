{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_edwtd_src_extract_id_details_load', 'batch', 'templates_guidelines'],
    meta={
        'source_workflow': 'wf_m_EDWTD_Src_Extract_Id_Details_LOAD',
        'target_table': 'DW_JOB_STREAMS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.418871+00:00'
    }
) }}

WITH 

source_dual_id AS (
    SELECT
        dummy
    FROM {{ source('raw', 'dual_id') }}
),

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

transformed_exp_dw_job_streams AS (
    SELECT
    last_extract_id,
    'job_stream_id' AS job_stream_id,
    JOB_STREAM_ID AS job_stream_id_out,
    'BatchId' AS batch_id,
    BATCH_ID AS batch_id_out,
    IFF( ISNULL(LAST_EXTRACT_ID), 'LastExtractId', LAST_EXTRACT_ID) AS last_extract_id_out
    FROM source_dw_job_streams
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
        target_table_name,
        last_extract_id
    FROM transformed_exp_dw_job_streams
)

SELECT * FROM final