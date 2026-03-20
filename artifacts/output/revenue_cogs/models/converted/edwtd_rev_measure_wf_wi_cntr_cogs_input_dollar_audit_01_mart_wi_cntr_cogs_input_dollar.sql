{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_cntr_cogs_input_dollar', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_CNTR_COGS_INPUT_DOLLAR',
        'target_table': 'WI_CNTR_COGS_INPUT_DOLLAR',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.177481+00:00'
    }
) }}

WITH 

source_wi_cntr_cogs_input_dollar AS (
    SELECT
        fiscal_year_month_num_int,
        cost_type,
        direct_flag,
        as_tss_flag,
        pl_node_value,
        pl_node_name,
        pl_node_level,
        dept_number,
        amount,
        internal_l2_theater
    FROM {{ source('raw', 'wi_cntr_cogs_input_dollar') }}
),

final AS (
    SELECT
        fiscal_year_month_num_int,
        cost_type,
        direct_flag,
        as_tss_flag,
        pl_node_value,
        pl_node_name,
        pl_node_level,
        dept_number,
        amount,
        internal_l2_theater
    FROM source_wi_cntr_cogs_input_dollar
)

SELECT * FROM final