{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_input_amount', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_INPUT_AMOUNT',
        'target_table': 'WI_SSC_ALLOC_ER',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.228841+00:00'
    }
) }}

WITH 

source_wi_ssc_alloc_er AS (
    SELECT
        fiscal_month_id,
        theater,
        cost_type,
        sub_cost_type,
        cost
    FROM {{ source('raw', 'wi_ssc_alloc_er') }}
),

source_wi_input_amount AS (
    SELECT
        pl_node_value,
        account_number,
        allocation_method,
        sub_account_number,
        functional_cost,
        cost_type,
        input_amount
    FROM {{ source('raw', 'wi_input_amount') }}
),

final AS (
    SELECT
        fiscal_month_id,
        theater,
        cost_type,
        sub_cost_type,
        cost
    FROM source_wi_input_amount
)

SELECT * FROM final