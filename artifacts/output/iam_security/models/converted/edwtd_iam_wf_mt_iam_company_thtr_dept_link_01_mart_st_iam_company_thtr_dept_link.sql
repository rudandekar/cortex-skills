{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_mt_iam_company_thtr_dept_link', 'batch', 'edwtd_iam'],
    meta={
        'source_workflow': 'wf_m_MT_IAM_COMPANY_THTR_DEPT_LINK',
        'target_table': 'ST_IAM_COMPANY_THTR_DEPT_LINK',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:25:46.765201+00:00'
    }
) }}

WITH 

source_st_iam_company_thtr_dept_link AS (
    SELECT
        iam_user_key,
        source_deleted_flg,
        iam_application_key,
        bk_company_cd,
        bk_department_cd,
        assignment_type,
        standard_or_exception_flg,
        exclusive_or_restrictive_flg,
        iam_level_num_int,
        application_name,
        bk_cpr_universal_id_int,
        internal_role,
        external_role,
        cco_id,
        worker_type_cd,
        cec_id,
        external_user_email_addr,
        external_flg,
        internal_flg,
        iam_dummy_user_name,
        cisco_worker_party_key,
        bk_iam_role_name
    FROM {{ source('raw', 'st_iam_company_thtr_dept_link') }}
),

final AS (
    SELECT
        iam_user_key,
        source_deleted_flg,
        iam_application_key,
        bk_company_cd,
        bk_department_cd,
        assignment_type,
        standard_or_exception_flg,
        exclusive_or_restrictive_flg,
        iam_level_num_int,
        application_name,
        bk_cpr_universal_id_int,
        internal_role,
        external_role,
        cco_id,
        worker_type_cd,
        cec_id,
        external_user_email_addr,
        external_flg,
        internal_flg,
        iam_dummy_user_name,
        cisco_worker_party_key,
        bk_iam_role_name
    FROM source_st_iam_company_thtr_dept_link
)

SELECT * FROM final