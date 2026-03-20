{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_mf_gl_period_statuses', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_ST_MF_GL_PERIOD_STATUSES',
        'target_table': 'ST_MF_GL_PERIOD_STATUSES',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.849558+00:00'
    }
) }}

WITH 

source_ff_mf_gl_period_statuses AS (
    SELECT
        batch_id,
        adjustment_period_flag,
        application_id,
        attribute1,
        attribute2,
        attribute3,
        attribute4,
        attribute5,
        chronological_seq_status_code,
        closing_status,
        context,
        created_by,
        creation_date,
        effective_period_num,
        elimination_confirmed_flag,
        end_date,
        ges_update_date,
        global_name,
        last_updated_by,
        last_update_date,
        last_update_login,
        period_name,
        period_num,
        period_type,
        period_year,
        quarter_num,
        quarter_start_date,
        set_of_books_id,
        start_date,
        year_start_date,
        create_datetime,
        action_code
    FROM {{ source('raw', 'ff_mf_gl_period_statuses') }}
),

final AS (
    SELECT
        batch_id,
        adjustment_period_flag,
        application_id,
        attribute1,
        attribute2,
        attribute3,
        attribute4,
        attribute5,
        chronological_seq_status_code,
        closing_status,
        context,
        created_by,
        creation_date,
        effective_period_num,
        elimination_confirmed_flag,
        end_date,
        ges_update_date,
        global_name,
        last_updated_by,
        last_update_date,
        last_update_login,
        period_name,
        period_num,
        period_type,
        period_year,
        quarter_num,
        quarter_start_date,
        set_of_books_id,
        start_date,
        year_start_date,
        create_datetime,
        action_code
    FROM source_ff_mf_gl_period_statuses
)

SELECT * FROM final