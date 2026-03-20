{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_n_fin_fc_deliv_thtr_pc_hier', 'batch', 'edwtd_fnh'],
    meta={
        'source_workflow': 'wf_m_ST_N_FIN_FC_DELIV_THTR_PC_HIER',
        'target_table': 'ST_N_FIN_FC_DELIV_THTR_PC_HIER',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.929330+00:00'
    }
) }}

WITH 

source_ff_n_fin_fc_deliv_thtr_pc_hier AS (
    SELECT
        parent_node,
        child_node,
        child_node_desc,
        refresh_date,
        create_datetime,
        action_code,
        batch_id
    FROM {{ source('raw', 'ff_n_fin_fc_deliv_thtr_pc_hier') }}
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
    FROM source_ff_n_fin_fc_deliv_thtr_pc_hier
)

SELECT * FROM final