{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_csone_acts', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_ST_CSONE_ACTS',
        'target_table': 'ST_CSONE_ACTS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.504748+00:00'
    }
) }}

WITH 

source_ff_csone_acts AS (
    SELECT
        user_id,
        user_name,
        login_time,
        login_status
    FROM {{ source('raw', 'ff_csone_acts') }}
),

final AS (
    SELECT
        user_id,
        user_name,
        login_time,
        login_status
    FROM source_ff_csone_acts
)

SELECT * FROM final