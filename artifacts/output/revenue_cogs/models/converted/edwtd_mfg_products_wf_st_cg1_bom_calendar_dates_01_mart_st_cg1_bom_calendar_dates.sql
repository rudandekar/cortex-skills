{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_cg1_bom_calendar_dates', 'batch', 'edwtd_mfg_products'],
    meta={
        'source_workflow': 'wf_m_ST_CG1_BOM_CALENDAR_DATES',
        'target_table': 'ST_CG1_BOM_CALENDAR_DATES',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.945847+00:00'
    }
) }}

WITH 

source_cg1_bom_calendar_dates AS (
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
    FROM {{ source('raw', 'cg1_bom_calendar_dates') }}
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
        batch_id,
        action_cd,
        global_name,
        create_datetime,
        source_commit_time,
        refresh_datetime
    FROM source_cg1_bom_calendar_dates
)

SELECT * FROM final