{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_ah_mgmt_hier_node_h1_tv', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_N_AH_MGMT_HIER_NODE_H1_TV',
        'target_table': 'N_AH_MGMT_HIER_NODE_H1',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:29.177541+00:00'
    }
) }}

WITH 

source_w_ah_mgmt_hier_node_h1 AS (
    SELECT
        management_hier_node_nm_key,
        management_hierarchy_node_nm,
        department_manager_party_key,
        node_value_cd,
        mgmt_node_level_number_int,
        node_type,
        node_id_int,
        parent_mgmt_hier_node_nm_key,
        bk_fd_alternate_hierarchy_id,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        start_tv_dt,
        end_tv_dt,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_ah_mgmt_hier_node_h1') }}
),

source_n_ah_mgmt_hier_node_h1_tv AS (
    SELECT
        management_hier_node_nm_key,
        start_tv_dt,
        end_tv_dt,
        department_manager_party_key,
        management_hierarchy_node_nm,
        node_value_cd,
        mgmt_node_level_number_int,
        node_type,
        node_id_int,
        edw_create_user,
        edw_update_user,
        parent_mgmt_hier_node_nm_key,
        bk_fd_alternate_hierarchy_id,
        edw_create_dtm,
        edw_update_dtm
    FROM {{ source('raw', 'n_ah_mgmt_hier_node_h1_tv') }}
),

final AS (
    SELECT
        management_hier_node_nm_key,
        management_hierarchy_node_nm,
        department_manager_party_key,
        node_value_cd,
        mgmt_node_level_number_int,
        node_type,
        node_id_int,
        parent_mgmt_hier_node_nm_key,
        bk_fd_alternate_hierarchy_id,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_n_ah_mgmt_hier_node_h1_tv
)

SELECT * FROM final