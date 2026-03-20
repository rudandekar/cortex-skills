{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wk_management_hierarchy_node', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_WK_MANAGEMENT_HIERARCHY_NODE',
        'target_table': 'EX_ODS_SI_HIER_PUBLISHED',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.670166+00:00'
    }
) }}

WITH 

source_sm_management_hierarchy_node AS (
    SELECT
        management_hier_node_nm_key,
        department_manager_party_key,
        management_hierarchy_node_nm,
        node_value_cd,
        edw_create_user,
        edw_create_dtm
    FROM {{ source('raw', 'sm_management_hierarchy_node') }}
),

source_wi_ods_si_hier_published_mgmt AS (
    SELECT
        node_id,
        node_value,
        node_desc,
        parent_node_id,
        level_no,
        node_type
    FROM {{ source('raw', 'wi_ods_si_hier_published_mgmt') }}
),

source_st_ods_si_hier_published AS (
    SELECT
        batch_id,
        created_by,
        creation_date,
        ges_global_name,
        ges_update_date,
        global_name,
        hierarchy_type,
        level_no,
        node_desc,
        node_id,
        node_type,
        node_value,
        parent_desc,
        parent_node_id,
        published_date,
        published_status,
        updated_by,
        update_date,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_ods_si_hier_published') }}
),

transformed_exp_w_management_hierarchy_node AS (
    SELECT
    management_hier_node_nm_key,
    batch_id,
    start_tv_dt,
    end_tv_dt,
    department_manager_party_key,
    management_hierarchy_node_nm,
    mgmt_node_level_number_int,
    node_type,
    parent_mgmt_hier_node_nm_key,
    action_code,
    dml_type,
    rank_index,
    node_value_cd,
    node_id_int
    FROM source_st_ods_si_hier_published
),

final AS (
    SELECT
        batch_id,
        created_by,
        creation_date,
        ges_global_name,
        ges_update_date,
        global_name,
        hierarchy_type,
        level_no,
        node_desc,
        node_id,
        node_type,
        node_value,
        parent_desc,
        parent_node_id,
        published_date,
        published_status,
        updated_by,
        update_date,
        create_datetime,
        action_code,
        exception_type
    FROM transformed_exp_w_management_hierarchy_node
)

SELECT * FROM final