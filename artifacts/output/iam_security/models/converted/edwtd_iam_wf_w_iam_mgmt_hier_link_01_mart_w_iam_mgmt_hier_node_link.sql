{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_iam_mgmt_hier_link', 'batch', 'edwtd_iam'],
    meta={
        'source_workflow': 'wf_m_W_IAM_MGMT_HIER_LINK',
        'target_table': 'W_IAM_MGMT_HIER_NODE_LINK',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:25:46.722135+00:00'
    }
) }}

WITH 

source_st_iam_edwtd_usr_role_pl_hier AS (
    SELECT
        iam_user_id,
        role_id,
        role_name,
        tool_id,
        data_struct_type,
        lvl_number,
        erp_segment1,
        erp_segment_id,
        assignment_type,
        data_res_expt_flag,
        excl_restr_flag,
        restriction_flag,
        proxy_flag,
        grantor_universal_id,
        grantor_cec_id,
        grantor_cco_id,
        status,
        tr_flag,
        last_action,
        created_by,
        create_date,
        updated_by,
        update_date,
        action_code,
        batch_id,
        create_datetime
    FROM {{ source('raw', 'st_iam_edwtd_usr_role_pl_hier') }}
),

final AS (
    SELECT
        bk_iam_role_name,
        iam_user_key,
        iam_application_key,
        mgmt_hierarchy_node_key,
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
    FROM source_st_iam_edwtd_usr_role_pl_hier
)

SELECT * FROM final