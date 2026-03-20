{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_apsp_gprv_fin_acct_hier_node', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_W_APSP_GPRV_FIN_ACCT_HIER_NODE',
        'target_table': 'W_APSP_GPRV_FIN_ACCT_HIER_NODE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.958026+00:00'
    }
) }}

WITH 

source_st_rv_account_hierarchy AS (
    SELECT
        hierarchy_type,
        category_id,
        category,
        category_level1,
        category_level2,
        account_number,
        action_code,
        batch_id,
        create_datetime
    FROM {{ source('raw', 'st_rv_account_hierarchy') }}
),

source_w_apsp_gprv_fin_acct_hier_node AS (
    SELECT
        bk_gprv_account_group_id,
        node_type,
        ru_parent_grp_acct_grp_id,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        dv_account_node_level_num_int,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_apsp_gprv_fin_acct_hier_node') }}
),

final AS (
    SELECT
        bk_gprv_account_group_id,
        node_type,
        ru_parent_grp_acct_grp_id,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        dv_account_node_level_num_int,
        action_code,
        dml_type
    FROM source_w_apsp_gprv_fin_acct_hier_node
)

SELECT * FROM final