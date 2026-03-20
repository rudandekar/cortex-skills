{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_iam_cisco_wrkr_supv_hier_lnk', 'batch', 'edwtd_iam'],
    meta={
        'source_workflow': 'wf_m_N_IAM_CISCO_WRKR_SUPV_HIER_LNK',
        'target_table': 'N_IAM_CISCO_WRKR_SUPV_HIER_LNK',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:25:46.734420+00:00'
    }
) }}

WITH 

source_w_iam_cisco_wrkr_supv_hier_lnk AS (
    SELECT
        iam_role_name,
        iam_user_key,
        iam_application_key,
        cisco_worker_party_key,
        assignment_type,
        standard_or_exception_flg,
        exclusive_or_restrictive_flg,
        source_deleted_flg,
        iam_level_num_int,
        ss_bk_employee_id,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_iam_cisco_wrkr_supv_hier_lnk') }}
),

final AS (
    SELECT
        iam_role_name,
        iam_user_key,
        iam_application_key,
        cisco_worker_party_key,
        assignment_type,
        standard_or_exception_flg,
        exclusive_or_restrictive_flg,
        source_deleted_flg,
        iam_level_num_int,
        ss_bk_employee_id,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_iam_cisco_wrkr_supv_hier_lnk
)

SELECT * FROM final