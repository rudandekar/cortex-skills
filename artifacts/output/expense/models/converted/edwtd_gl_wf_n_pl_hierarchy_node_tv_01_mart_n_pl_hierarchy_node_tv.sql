{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_pl_hierarchy_node_tv', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_N_PL_HIERARCHY_NODE_TV',
        'target_table': 'N_PL_HIERARCHY_NODE_TV',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.641845+00:00'
    }
) }}

WITH 

source_w_pl_hierarchy_node AS (
    SELECT
        pl_hierarchy_node_key,
        start_tv_dt,
        end_tv_dt,
        node_type,
        node_value_cd,
        parent_pl_hier_node_key,
        pl_hier_node_level_number_int,
        pl_hierarchy_node_nm,
        edw_create_user,
        edw_update_user,
        edw_create_dtm,
        edw_update_dtm,
        action_code,
        dml_type,
        node_id_int
    FROM {{ source('raw', 'w_pl_hierarchy_node') }}
),

source_n_pl_hierarchy_node_tv AS (
    SELECT
        pl_hierarchy_node_key,
        start_tv_dt,
        end_tv_dt,
        node_type,
        node_value_cd,
        parent_pl_hier_node_key,
        pl_hier_node_level_number_int,
        pl_hierarchy_node_nm,
        edw_create_user,
        edw_update_user,
        edw_create_dtm,
        edw_update_dtm,
        node_id_int
    FROM {{ source('raw', 'n_pl_hierarchy_node_tv') }}
),

final AS (
    SELECT
        pl_hierarchy_node_key,
        start_tv_dt,
        end_tv_dt,
        node_type,
        node_value_cd,
        parent_pl_hier_node_key,
        pl_hier_node_level_number_int,
        pl_hierarchy_node_nm,
        edw_create_user,
        edw_update_user,
        edw_create_dtm,
        edw_update_dtm,
        node_id_int
    FROM source_n_pl_hierarchy_node_tv
)

SELECT * FROM final