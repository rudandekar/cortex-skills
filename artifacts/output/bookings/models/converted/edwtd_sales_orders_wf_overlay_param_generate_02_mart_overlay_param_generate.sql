{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_overlay_process_control', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_OVERLAY_PROCESS_CONTROL',
        'target_table': 'OVERLAY_PARAM_GENERATE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.301840+00:00'
    }
) }}

WITH 

source_wi_overlay_process_control AS (
    SELECT
        active_ind,
        process_type,
        process_date
    FROM {{ source('raw', 'wi_overlay_process_control') }}
),

final AS (
    SELECT
        parameter_value
    FROM source_wi_overlay_process_control
)

SELECT * FROM final