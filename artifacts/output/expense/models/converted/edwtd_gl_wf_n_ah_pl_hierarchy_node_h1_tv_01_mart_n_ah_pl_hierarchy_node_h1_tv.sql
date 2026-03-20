{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_ah_pl_hierarchy_node_h1_tv', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_N_AH_PL_HIERARCHY_NODE_H1_TV',
        'target_table': 'N_AH_PL_HIERARCHY_NODE_H1_TV',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:29.174091+00:00'
    }
) }}

WITH 

source_n_ah_pl_hierarchy_node_h1_tv AS (
    SELECT
        pl_hierarchy_node_key,
        pl_hier_node_level_number_int,
        parent_pl_hier_node_key,
        pl_hierarchy_node_nm,
        node_value_cd,
        node_type,
        node_id_int,
        bk_profit_center_name,
        bk_fd_alternate_hierarchy_id,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        start_tv_dt,
        end_tv_dt
    FROM {{ source('raw', 'n_ah_pl_hierarchy_node_h1_tv') }}
),

source_w_ah_pl_hierarchy_node_h1 AS (
    SELECT
        pl_hierarchy_node_key,
        pl_hier_node_level_number_int,
        parent_pl_hier_node_key,
        pl_hierarchy_node_nm,
        node_value_cd,
        node_type,
        node_id_int,
        bk_profit_center_name,
        bk_fd_alternate_hierarchy_id,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        start_tv_dt,
        end_tv_dt,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_ah_pl_hierarchy_node_h1') }}
),

final AS (
    SELECT
        pl_hierarchy_node_key,
        pl_hier_node_level_number_int,
        parent_pl_hier_node_key,
        pl_hierarchy_node_nm,
        node_value_cd,
        node_type,
        node_id_int,
        bk_profit_center_name,
        bk_fd_alternate_hierarchy_id,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        start_tv_dt,
        end_tv_dt
    FROM source_w_ah_pl_hierarchy_node_h1
)

SELECT * FROM final