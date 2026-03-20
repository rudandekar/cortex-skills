{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_recur_ofr_rev_ctrl_dtm', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_RECUR_OFR_REV_CTRL_DTM',
        'target_table': 'WI_REC_OFR_REV_LAST_EXTACT_DTM',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.038748+00:00'
    }
) }}

WITH 

source_dw_job_stream_runs AS (
    SELECT
        job_stream_id,
        start_extract_date,
        end_extract_date,
        start_extract_id,
        end_extract_id,
        job_stream_start_time,
        job_stream_end_time,
        source_flat_file_name,
        flat_file_size,
        flat_file_row_count,
        uproc_name,
        run_skipped,
        edw_create_datetime,
        edw_update_datetime,
        auto_restarted_cnt
    FROM {{ source('raw', 'dw_job_stream_runs') }}
),

final AS (
    SELECT
        last_extact_dtm
    FROM source_dw_job_stream_runs
)

SELECT * FROM final