{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_ssc_alloc_er', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_SSC_ALLOC_ER',
        'target_table': 'WI_SSC_ALLOC_ER',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.318724+00:00'
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

final AS (
    SELECT
        fiscal_month_id,
        theater,
        cost_type,
        sub_cost_type,
        cost
    FROM source_wi_ssc_alloc_er
)

SELECT * FROM final