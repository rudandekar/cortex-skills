{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_csf_pa_periods_all', 'batch', 'edwtd_batch_accum'],
    meta={
        'source_workflow': 'wf_m_CSF_PA_PERIODS_ALL',
        'target_table': 'STG_CSF_PA_PERIODS_ALL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:22:00.955280+00:00'
    }
) }}

WITH 

source_stg_csf_pa_periods_all AS (
    SELECT
        period_name,
        last_update_date,
        last_updated_by,
        creation_date,
        created_by,
        last_update_login,
        start_date,
        end_date,
        status,
        gl_period_name,
        current_pa_period_flag,
        org_id,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime
    FROM {{ source('raw', 'stg_csf_pa_periods_all') }}
),

source_csf_pa_periods_all AS (
    SELECT
        source_dml_type,
        fully_qualified_table_name,
        source_commit_time,
        refresh_datetime,
        trail_position,
        token,
        refresh_day,
        period_name,
        last_update_date,
        last_updated_by,
        creation_date,
        created_by,
        last_update_login,
        start_date,
        end_date,
        status,
        gl_period_name,
        current_pa_period_flag,
        org_id
    FROM {{ source('raw', 'csf_pa_periods_all') }}
),

transformed_exp_csf_pa_periods_all AS (
    SELECT
    source_dml_type,
    fully_qualified_table_name,
    source_commit_time,
    refresh_datetime,
    trail_position,
    token,
    refresh_day,
    period_name,
    last_update_date,
    last_updated_by,
    creation_date,
    created_by,
    last_update_login,
    start_date,
    end_date,
    status,
    gl_period_name,
    current_pa_period_flag,
    org_id
    FROM source_csf_pa_periods_all
),

final AS (
    SELECT
        period_name,
        last_update_date,
        last_updated_by,
        creation_date,
        created_by,
        last_update_login,
        start_date,
        end_date,
        status,
        gl_period_name,
        current_pa_period_flag,
        org_id,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime
    FROM transformed_exp_csf_pa_periods_all
)

SELECT * FROM final