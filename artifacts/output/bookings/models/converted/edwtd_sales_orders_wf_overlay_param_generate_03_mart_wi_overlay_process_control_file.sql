{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_overlay_param_generate', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_OVERLAY_PARAM_GENERATE',
        'target_table': 'WI_OVERLAY_PROCESS_CONTROL_FILE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.247630+00:00'
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
        process_flag
    FROM source_wi_overlay_process_control
)

SELECT * FROM final