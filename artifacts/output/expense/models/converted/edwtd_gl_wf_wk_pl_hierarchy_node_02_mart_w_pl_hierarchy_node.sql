{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wk_pl_hierarchy_node', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_WK_PL_HIERARCHY_NODE',
        'target_table': 'W_PL_HIERARCHY_NODE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.897612+00:00'
    }
) }}

WITH 

source_wi_ods_si_hier_published_pl AS (
    SELECT
        node_id,
        node_value,
        node_desc,
        parent_node_id,
        level_no,
        node_type
    FROM {{ source('raw', 'wi_ods_si_hier_published_pl') }}
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

transformed_exp_pl_hierarchy_node AS (
    SELECT
    pl_hierarchy_node_key,
    start_tv_dt,
    end_tv_dt,
    node_type,
    node_value_cd,
    parent_pl_hier_node_key,
    pl_hier_node_level_number_int,
    pl_hierarchy_node_nm,
    sk_node_id_int,
    action_code,
    rank_index,
    dml_type
    FROM source_st_ods_si_hier_published
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
        action_code,
        dml_type,
        node_id_int
    FROM transformed_exp_pl_hierarchy_node
)

SELECT * FROM final