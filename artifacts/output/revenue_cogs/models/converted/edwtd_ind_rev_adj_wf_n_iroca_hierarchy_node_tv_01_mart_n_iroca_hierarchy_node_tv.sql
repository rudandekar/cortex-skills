{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_iroca_hierarchy_node_tv', 'batch', 'edwtd_ind_rev_adj'],
    meta={
        'source_workflow': 'wf_m_N_IROCA_HIERARCHY_NODE_TV',
        'target_table': 'N_IROCA_HIERARCHY_NODE_TV',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.419167+00:00'
    }
) }}

WITH 

source_w_iroca_hierarchy_node AS (
    SELECT
        bk_iroca_hierarchy_node_id,
        start_tv_dt,
        node_type,
        end_tv_dt,
        ru_parent_hierarchy_node_id,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_iroca_hierarchy_node') }}
),

final AS (
    SELECT
        bk_iroca_hierarchy_node_id,
        start_tv_dt,
        node_type,
        end_tv_dt,
        ru_parent_hierarchy_node_id,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm
    FROM source_w_iroca_hierarchy_node
)

SELECT * FROM final