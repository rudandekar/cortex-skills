{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_n_financial_sales_map', 'batch', 'edwtd_fnh'],
    meta={
        'source_workflow': 'wf_m_FF_N_FINANCIAL_SALES_MAP',
        'target_table': 'FF_N_FINANCIAL_SALES_MAP',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.740189+00:00'
    }
) }}

WITH 

source_vw_n_financial_sales_map AS (
    SELECT
        parent_node,
        child_node,
        child_node_desc,
        refresh_date
    FROM {{ source('raw', 'vw_n_financial_sales_map') }}
),

transformed_ex_ff_n_financial_sales_map AS (
    SELECT
    parent_node,
    child_node,
    child_node_desc,
    refresh_date,
    'BatchId' AS batch_id,
    'I' AS action_cd,
    CURRENT_TIMESTAMP() AS create_datetime
    FROM source_vw_n_financial_sales_map
),

final AS (
    SELECT
        parent_node,
        child_node,
        child_node_desc,
        refresh_date,
        batch_id,
        action_cd,
        create_datetime
    FROM transformed_ex_ff_n_financial_sales_map
)

SELECT * FROM final