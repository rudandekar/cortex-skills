{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_rtm_month_control', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_RTM_MONTH_CONTROL',
        'target_table': 'WI_RTM_MONTH_CONTROL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.067773+00:00'
    }
) }}

WITH 

source_wi_rtm_month_control AS (
    SELECT
        fiscal_year_quarter_number_int,
        fiscal_year_month_int,
        run_batch_id,
        inc_flag
    FROM {{ source('raw', 'wi_rtm_month_control') }}
),

final AS (
    SELECT
        fiscal_year_quarter_number_int,
        fiscal_year_month_int,
        run_batch_id,
        inc_flag
    FROM source_wi_rtm_month_control
)

SELECT * FROM final