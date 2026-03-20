{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_cg1_xxgco_exp_request_lines', 'batch', 'edwtd_batch_accum'],
    meta={
        'source_workflow': 'wf_m_CG1_XXGCO_EXP_REQUEST_LINES',
        'target_table': 'CG1_XXGCO_EXP_REQUEST_LINES',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:22:01.086233+00:00'
    }
) }}

WITH 

source_stg_cg1_xxgco_exp_rqst_lines AS (
    SELECT
        request_line_id,
        request_id,
        exp_status,
        shipset_number,
        new_request_date,
        latest_acceptable_date,
        new_promised_date,
        creation_date,
        created_by,
        last_updated_date,
        last_updated_by,
        promised_date,
        dollar_amount,
        reqd_line_ids,
        exp_priority,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        processed_flag
    FROM {{ source('raw', 'stg_cg1_xxgco_exp_rqst_lines') }}
),

source_cg1_xxgco_exp_request_lines AS (
    SELECT
        source_dml_type,
        fully_qualified_table_name,
        source_commit_time,
        refresh_datetime,
        trail_position,
        token,
        request_line_id,
        request_id,
        exp_status,
        shipset_number,
        new_request_date,
        latest_acceptable_date,
        new_promised_date,
        creation_date,
        created_by,
        last_updated_date,
        last_updated_by,
        promised_date,
        dollar_amount,
        reqd_line_ids,
        exp_priority,
        error_message
    FROM {{ source('raw', 'cg1_xxgco_exp_request_lines') }}
),

transformed_exp_cg1_xxgco_exp_request_lines AS (
    SELECT
    source_dml_type,
    source_commit_time,
    refresh_datetime,
    request_line_id,
    request_id,
    exp_status,
    shipset_number,
    new_request_date,
    latest_acceptable_date,
    new_promised_date,
    creation_date,
    created_by,
    last_updated_date,
    last_updated_by,
    promised_date,
    dollar_amount,
    reqd_line_ids,
    exp_priority
    FROM source_cg1_xxgco_exp_request_lines
),

final AS (
    SELECT
        request_line_id,
        request_id,
        exp_status,
        shipset_number,
        new_request_date,
        latest_acceptable_date,
        new_promised_date,
        creation_date,
        created_by,
        last_updated_date,
        last_updated_by,
        promised_date,
        dollar_amount,
        reqd_line_ids,
        exp_priority,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        processed_flag
    FROM transformed_exp_cg1_xxgco_exp_request_lines
)

SELECT * FROM final