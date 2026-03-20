{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_edwtd_src_extract_details_load', 'batch', 'templates_guidelines'],
    meta={
        'source_workflow': 'wf_m_EDWTD_Src_Extract_Details_LOAD',
        'target_table': 'Parameter_File',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.723304+00:00'
    }
) }}

WITH 

source_dual_date AS (
    SELECT
        dummy
    FROM {{ source('raw', 'dual_date') }}
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
    last_extract_date,
    IFF( ISNULL(LAST_EXTRACT_DATE), 'LastExtractDate', LAST_EXTRACT_DATE) AS last_extract_date_out,
    'job_stream_id' AS job_stream_id,
    JOB_STREAM_ID AS job_stream_id_out,
    'BatchId' AS batch_id,
    BATCH_ID AS batch_id_out
    FROM source_dw_job_streams
),

final AS (
    SELECT
        parm_string
    FROM transformed_exp_dw_job_streams
)

SELECT * FROM final