{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_iam_pl_hierarchy_link', 'batch', 'edwtd_iam'],
    meta={
        'source_workflow': 'wf_m_N_IAM_PL_HIERARCHY_LINK',
        'target_table': 'N_IAM_PL_HIERARCHY_NODE_LINK',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:25:46.754605+00:00'
    }
) }}

WITH 

source_w_iam_pl_hierarchy_node_link AS (
    SELECT
        bk_iam_role_name,
        iam_user_key,
        iam_application_key,
        pl_hierarchy_node_key,
        dd_node_id_int,
        assignment_type,
        standard_or_exception_flg,
        exclusive_or_restrictive_flg,
        source_deleted_flg,
        iam_level_num_int,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_iam_pl_hierarchy_node_link') }}
),

final AS (
    SELECT
        bk_iam_role_name,
        iam_user_key,
        iam_application_key,
        pl_hierarchy_node_key,
        dd_node_id_int,
        assignment_type,
        standard_or_exception_flg,
        exclusive_or_restrictive_flg,
        source_deleted_flg,
        iam_level_num_int,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_iam_pl_hierarchy_node_link
)

SELECT * FROM final