{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_si_ariba_req_threshold', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_FF_SI_ARIBA_REQ_THRESHOLD',
        'target_table': 'FF_SI_ARIBA_REQ_THRESHOLD',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.612864+00:00'
    }
) }}

WITH 

source_si_ariba_req_threshold AS (
    SELECT
        ariba_req_threshold_id,
        ariba_req_threshold_value,
        ariba_req_threshold_desc,
        created_by,
        create_date,
        last_updated_by,
        last_update_date,
        enabled_flag
    FROM {{ source('raw', 'si_ariba_req_threshold') }}
),

transformed_exp_si_ariba_req_threshold AS (
    SELECT
    ariba_req_threshold_id,
    ariba_req_threshold_value,
    ariba_req_threshold_desc,
    enabled_flag,
    last_update_date,
    'BatchId' AS batch_id,
    CURRENT_TIMESTAMP() AS create_datetime,
    'I' AS action_code
    FROM source_si_ariba_req_threshold
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
    FROM transformed_exp_si_ariba_req_threshold
)

SELECT * FROM final