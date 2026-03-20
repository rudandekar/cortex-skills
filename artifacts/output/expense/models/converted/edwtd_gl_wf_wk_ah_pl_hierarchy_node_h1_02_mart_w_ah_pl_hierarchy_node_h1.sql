{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wk_ah_pl_hierarchy_node_h1', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_WK_AH_PL_HIERARCHY_NODE_H1',
        'target_table': 'W_AH_PL_HIERARCHY_NODE_H1',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.645839+00:00'
    }
) }}

WITH 

source_wi_si_hierarchy_ah_published AS (
    SELECT
        node_id,
        node_value,
        node_desc,
        parent_node_id,
        level_no,
        node_type
    FROM {{ source('raw', 'wi_si_hierarchy_ah_published') }}
),

source_st_si_hierarchy_ah_published AS (
    SELECT
        batch_id,
        node_id,
        global_name,
        level_no,
        node_type,
        node_value,
        node_desc,
        parent_desc,
        parent_node_id,
        hierarchy_type,
        creation_date,
        created_by,
        update_date,
        updated_by,
        reference_id,
        published_date,
        published_status,
        change_type,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_si_hierarchy_ah_published') }}
),

transformed_exp_w_ah_pl_hierarchy_node_h1_for_work AS (
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
    start_tv_dt,
    end_tv_dt,
    action_code,
    dml_type
    FROM source_st_si_hierarchy_ah_published
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
        end_tv_dt,
        action_code,
        dml_type
    FROM transformed_exp_w_ah_pl_hierarchy_node_h1_for_work
)

SELECT * FROM final