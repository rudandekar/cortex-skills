{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_si_theater', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_ST_SI_THEATER',
        'target_table': 'ST_SI_THEATER',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:29.041763+00:00'
    }
) }}

WITH 

source_ff_si_theater AS (
    SELECT
        batch_id,
        theater_id,
        theater_name,
        last_update_date,
        create_datetime,
        action_code
    FROM {{ source('raw', 'ff_si_theater') }}
),

final AS (
    SELECT
        batch_id,
        theater_id,
        theater_name,
        last_update_date,
        create_datetime,
        action_code
    FROM source_ff_si_theater
)

SELECT * FROM final