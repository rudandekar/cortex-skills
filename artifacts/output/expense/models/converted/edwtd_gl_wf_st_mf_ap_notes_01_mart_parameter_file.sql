{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_edwtd_build_parm_file', 'batch', 'templates_guidelines'],
    meta={
        'source_workflow': 'wf_m_EDWTD_Build_Parm_File',
        'target_table': 'Parameter_File',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.912703+00:00'
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

source_dual_date AS (
    SELECT
        dummy
    FROM {{ source('raw', 'dual_date') }}
),

transformed_exp_dw_job_streams AS (
    SELECT
    job_stream_id,
    job_group_id,
    last_extract_date,
    batch_id,
    target_extract_date_col_name,
    target_table_name,
    last_extract_id,
    BATCH_ID+1 AS batch_id_new,
    'parm_heading'||chr(10)|| ''LastExtractDate'='||to_char(LAST_EXTRACT_DATE,'MM/DD/YYYY HH24:MI:SS')||chr(10)|| ''LastExtractId'='||to_char(LAST_EXTRACT_ID)||chr(10)|| ''BatchId'='||to_char(BATCH_ID_new)||chr(10)|| ''target_table_name'='||target_TABLE_NAME||chr(10)|| ''target_extract_date_col_name'='||to_char(target_EXTRACT_DATE_COL_NAME)||chr(10)|| ''source_file_name'='||target_TABLE_NAME||'.out'||chr(10)|| ''job_stream_id'='||'job_stream_id'||chr(10) -- ''source_file_name'='||to_char(LAST_EXTRAC /* TRUNCATED - review original */ AS vparmstring,
    RTRIM(vParmString) AS parmstring_out
    FROM source_dual_date
),

final AS (
    SELECT
        parm_string
    FROM transformed_exp_dw_job_streams
)

SELECT * FROM final