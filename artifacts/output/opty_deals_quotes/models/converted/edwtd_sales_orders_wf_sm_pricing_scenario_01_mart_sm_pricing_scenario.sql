{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_sm_pricing_scenario', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_SM_PRICING_SCENARIO',
        'target_table': 'SM_PRICING_SCENARIO',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.955186+00:00'
    }
) }}

WITH 

source_sm_pricing_scenario AS (
    SELECT
        pricing_scenario_key,
        sk_scenario_id_int,
        edw_create_dtm,
        edw_create_user
    FROM {{ source('raw', 'sm_pricing_scenario') }}
),

final AS (
    SELECT
        pricing_scenario_key,
        sk_scenario_id_int,
        edw_create_dtm,
        edw_create_user
    FROM source_sm_pricing_scenario
)

SELECT * FROM final