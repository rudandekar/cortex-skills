{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_ccm_ssc_input_dollar', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_CCM_SSC_INPUT_DOLLAR',
        'target_table': 'WI_CCM_SSC_INPUT_DOLLAR',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.713920+00:00'
    }
) }}

WITH 

source_wi_ccm_ssc_input_dollar AS (
    SELECT
        fiscal_month_id,
        pl_node_value,
        account_number,
        sub_account_number,
        functional_cost,
        amount
    FROM {{ source('raw', 'wi_ccm_ssc_input_dollar') }}
),

final AS (
    SELECT
        fiscal_month_id,
        pl_node_value,
        account_number,
        sub_account_number,
        functional_cost,
        amount
    FROM source_wi_ccm_ssc_input_dollar
)

SELECT * FROM final