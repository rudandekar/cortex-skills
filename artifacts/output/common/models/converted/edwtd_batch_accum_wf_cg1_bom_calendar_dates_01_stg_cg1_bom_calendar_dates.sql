{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_cg1_bom_calendar_dates', 'batch', 'edwtd_batch_accum'],
    meta={
        'source_workflow': 'wf_m_CG1_BOM_CALENDAR_DATES',
        'target_table': 'STG_CG1_BOM_CALENDAR_DATES',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:22:00.733807+00:00'
    }
) }}

WITH 

source_cg1_bom_calendar_dates AS (
    SELECT
        source_dml_type,
        fully_qualified_table_name,
        source_commit_time,
        refresh_datetime,
        trail_position,
        token,
        calendar_code,
        exception_set_id,
        calendar_date,
        last_updated_by,
        creation_date,
        created_by,
        last_update_login,
        last_update_date,
        seq_num,
        next_seq_num,
        prior_seq_num,
        next_date,
        prior_date,
        request_id,
        program_application_id,
        program_id,
        program_update_date
    FROM {{ source('raw', 'cg1_bom_calendar_dates') }}
),

source_stg_cg1_bom_calendar_dates AS (
    SELECT
        calendar_code,
        exception_set_id,
        calendar_date,
        last_updated_by,
        creation_date,
        created_by,
        last_update_login,
        last_update_date,
        seq_num,
        next_seq_num,
        prior_seq_num,
        next_date,
        prior_date,
        request_id,
        program_application_id,
        program_id,
        program_update_date,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        processed_flag
    FROM {{ source('raw', 'stg_cg1_bom_calendar_dates') }}
),

transformed_exp_cg1_bom_calendar_dates AS (
    SELECT
    source_dml_type,
    source_commit_time,
    refresh_datetime,
    calendar_code,
    exception_set_id,
    calendar_date,
    last_updated_by,
    creation_date,
    created_by,
    last_update_login,
    last_update_date,
    seq_num,
    next_seq_num,
    prior_seq_num,
    next_date,
    prior_date,
    request_id,
    program_application_id,
    program_id,
    program_update_date
    FROM source_stg_cg1_bom_calendar_dates
),

final AS (
    SELECT
        calendar_code,
        exception_set_id,
        calendar_date,
        last_updated_by,
        creation_date,
        created_by,
        last_update_login,
        last_update_date,
        seq_num,
        next_seq_num,
        prior_seq_num,
        next_date,
        prior_date,
        request_id,
        program_application_id,
        program_id,
        program_update_date,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        processed_flag
    FROM transformed_exp_cg1_bom_calendar_dates
)

SELECT * FROM final