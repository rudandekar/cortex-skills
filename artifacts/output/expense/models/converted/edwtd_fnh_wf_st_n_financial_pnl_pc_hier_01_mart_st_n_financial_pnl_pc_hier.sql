{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_n_financial_pnl_pc_hier', 'batch', 'edwtd_fnh'],
    meta={
        'source_workflow': 'wf_m_ST_N_FINANCIAL_PNL_PC_HIER',
        'target_table': 'ST_N_FINANCIAL_PNL_PC_HIER',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:29.032834+00:00'
    }
) }}

WITH 

source_ff_n_financial_pnl_pc_hier AS (
    SELECT
        batch_id,
        child_node,
        ifp_pnl_parent_lvl,
        mra_alias,
        parent_node,
        refresh_date,
        create_datetime,
        action_code
    FROM {{ source('raw', 'ff_n_financial_pnl_pc_hier') }}
),

final AS (
    SELECT
        batch_id,
        child_node,
        ifp_pnl_parent_lvl,
        mra_alias,
        parent_node,
        refresh_date,
        create_datetime,
        action_code
    FROM source_ff_n_financial_pnl_pc_hier
)

SELECT * FROM final