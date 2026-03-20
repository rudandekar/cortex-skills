{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_n_financial_sales_map', 'batch', 'edwtd_fnh'],
    meta={
        'source_workflow': 'wf_m_ST_N_FINANCIAL_SALES_MAP',
        'target_table': 'ST_N_FINANCIAL_SALES_MAP',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:29.129595+00:00'
    }
) }}

WITH 

source_ff_n_financial_sales_map AS (
    SELECT
        parent_node,
        child_node,
        child_node_desc,
        refresh_date,
        batch_id,
        action_cd,
        create_datetime
    FROM {{ source('raw', 'ff_n_financial_sales_map') }}
),

final AS (
    SELECT
        parent_node,
        child_node,
        child_node_desc,
        refresh_date,
        create_datetime,
        action_code,
        batch_id
    FROM source_ff_n_financial_sales_map
)

SELECT * FROM final