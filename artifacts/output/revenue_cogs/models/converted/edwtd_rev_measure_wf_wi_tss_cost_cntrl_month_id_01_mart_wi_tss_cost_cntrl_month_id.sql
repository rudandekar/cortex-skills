{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_tss_cost_cntrl_month_id', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_TSS_COST_CNTRL_MONTH_ID',
        'target_table': 'WI_TSS_COST_CNTRL_MONTH_ID',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.552443+00:00'
    }
) }}

WITH 

source_wi_tss_cost_cntrl_month_id AS (
    SELECT
        fiscal_month_id,
        fiscal_year_month_int,
        fiscal_quarter_name,
        fiscal_month_name,
        month_rank
    FROM {{ source('raw', 'wi_tss_cost_cntrl_month_id') }}
),

final AS (
    SELECT
        fiscal_month_id,
        fiscal_year_month_int,
        fiscal_quarter_name,
        fiscal_month_name,
        month_rank
    FROM source_wi_tss_cost_cntrl_month_id
)

SELECT * FROM final