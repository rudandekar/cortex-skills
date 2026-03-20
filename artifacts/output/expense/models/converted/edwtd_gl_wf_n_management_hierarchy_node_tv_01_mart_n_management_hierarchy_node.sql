{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_management_hierarchy_node_tv', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_N_MANAGEMENT_HIERARCHY_NODE_TV',
        'target_table': 'N_MANAGEMENT_HIERARCHY_NODE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.910328+00:00'
    }
) }}

WITH 

source_n_management_hierarchy_node_tv AS (
    SELECT
        management_hier_node_nm_key,
        start_tv_dt,
        end_tv_dt,
        department_manager_party_key,
        management_hierarchy_node_nm,
        mgmt_node_level_number_int,
        node_type,
        parent_mgmt_hier_node_nm_key,
        node_value_cd,
        edw_create_user,
        edw_update_user,
        edw_create_dtm,
        edw_update_dtm,
        node_id_int,
        rprtd_functional_org_name
    FROM {{ source('raw', 'n_management_hierarchy_node_tv') }}
),

source_w_management_hierarchy_node AS (
    SELECT
        management_hier_node_nm_key,
        start_tv_dt,
        end_tv_dt,
        department_manager_party_key,
        management_hierarchy_node_nm,
        mgmt_node_level_number_int,
        node_type,
        parent_mgmt_hier_node_nm_key,
        node_value_cd,
        edw_create_user,
        edw_update_user,
        edw_create_dtm,
        edw_update_dtm,
        action_code,
        dml_type,
        node_id_int
    FROM {{ source('raw', 'w_management_hierarchy_node') }}
),

final AS (
    SELECT
        management_hier_node_nm_key,
        department_manager_party_key,
        management_hierarchy_node_nm,
        mgmt_node_level_number_int,
        node_type,
        parent_mgmt_hier_node_nm_key,
        node_value_cd,
        edw_create_user,
        edw_update_user,
        edw_create_dtm,
        edw_update_dtm,
        node_id_int,
        rprtd_functional_org_name
    FROM source_w_management_hierarchy_node
)

SELECT * FROM final