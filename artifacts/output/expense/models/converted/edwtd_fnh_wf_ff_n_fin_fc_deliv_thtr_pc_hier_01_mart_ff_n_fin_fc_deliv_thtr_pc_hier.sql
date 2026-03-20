{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_n_fin_fc_deliv_thtr_pc_hier', 'batch', 'edwtd_fnh'],
    meta={
        'source_workflow': 'wf_m_FF_N_FIN_FC_DELIV_THTR_PC_HIER',
        'target_table': 'FF_N_FIN_FC_DELIV_THTR_PC_HIER',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.983088+00:00'
    }
) }}

WITH 

source_vw_n_fin_fc_deliv_thtr_pc_hier AS (
    SELECT
        parent_node,
        child_node,
        child_node_desc,
        refresh_date
    FROM {{ source('raw', 'vw_n_fin_fc_deliv_thtr_pc_hier') }}
),

transformed_exp_ff_n_fin_fc_deliv_thtr_pc_hier AS (
    SELECT
    parent_node,
    child_node,
    child_node_desc,
    refresh_date,
    CURRENT_TIMESTAMP() AS create_datetime,
    'I' AS action_code,
    'BatchId' AS batch_id
    FROM source_vw_n_fin_fc_deliv_thtr_pc_hier
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
    FROM transformed_exp_ff_n_fin_fc_deliv_thtr_pc_hier
)

SELECT * FROM final