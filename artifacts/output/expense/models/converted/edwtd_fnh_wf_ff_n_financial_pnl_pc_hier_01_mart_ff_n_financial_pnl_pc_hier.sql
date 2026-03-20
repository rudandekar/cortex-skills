{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_n_financial_pnl_pc_hier', 'batch', 'edwtd_fnh'],
    meta={
        'source_workflow': 'wf_m_FF_N_FINANCIAL_PNL_PC_HIER',
        'target_table': 'FF_N_FINANCIAL_PNL_PC_HIER',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.615358+00:00'
    }
) }}

WITH 

source_vw_n_financial_pnl_pc_hier AS (
    SELECT
        parent_node,
        child_node,
        mra_alias,
        ifp_pnl_parent_lvl,
        refresh_date
    FROM {{ source('raw', 'vw_n_financial_pnl_pc_hier') }}
),

transformed_exptrans AS (
    SELECT
    parent_node,
    child_node,
    mra_alias,
    ifp_pnl_parent_lvl,
    refresh_date,
    'BatchId' AS batch_id,
    CURRENT_TIMESTAMP() AS create_datetime,
    'I' AS action_code
    FROM source_vw_n_financial_pnl_pc_hier
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
    FROM transformed_exptrans
)

SELECT * FROM final