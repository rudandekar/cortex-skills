{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_si_ariba_req_threshold', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_ST_SI_ARIBA_REQ_THRESHOLD',
        'target_table': 'ST_SI_ARIBA_REQ_THRESHOLD',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:29.113100+00:00'
    }
) }}

WITH 

source_ff_si_ariba_req_threshold AS (
    SELECT
        batch_id,
        ariba_req_threshold_id,
        ariba_req_threshold_value,
        ariba_req_threshold_desc,
        enabled_flag,
        last_update_date,
        create_datetime,
        action_code
    FROM {{ source('raw', 'ff_si_ariba_req_threshold') }}
),

final AS (
    SELECT
        batch_id,
        ariba_req_threshold_id,
        ariba_req_threshold_value,
        ariba_req_threshold_desc,
        enabled_flag,
        last_update_date,
        create_datetime,
        action_code
    FROM source_ff_si_ariba_req_threshold
)

SELECT * FROM final