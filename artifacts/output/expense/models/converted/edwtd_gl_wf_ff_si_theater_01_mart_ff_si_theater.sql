{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_si_theater', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_FF_SI_THEATER',
        'target_table': 'FF_SI_THEATER',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:29.077550+00:00'
    }
) }}

WITH 

source_si_theater AS (
    SELECT
        theater_id,
        theater_name,
        create_date,
        created_by,
        last_update_date,
        last_updated_by,
        enabled_flag,
        company_min_range,
        company_max_range
    FROM {{ source('raw', 'si_theater') }}
),

transformed_exp_si_theater AS (
    SELECT
    theater_id,
    theater_name,
    last_update_date,
    'BatchId' AS batch_id,
    CURRENT_TIMESTAMP() AS create_date,
    'I' AS action_code
    FROM source_si_theater
),

final AS (
    SELECT
        batch_id,
        theater_id,
        theater_name,
        last_update_date,
        create_datetime,
        action_code
    FROM transformed_exp_si_theater
)

SELECT * FROM final