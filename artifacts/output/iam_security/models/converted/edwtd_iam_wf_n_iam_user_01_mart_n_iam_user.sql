{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_iam_user', 'batch', 'edwtd_iam'],
    meta={
        'source_workflow': 'wf_m_N_IAM_USER',
        'target_table': 'N_IAM_USER',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:25:46.745076+00:00'
    }
) }}

WITH 

source_w_iam_user AS (
    SELECT
        iam_user_key,
        bk_cpr_universal_id_int,
        internal_role,
        cco_id,
        external_role,
        source_deleted_flg,
        ru_external_user_email_addr,
        ru_cisco_worker_party_key,
        dd_cec_id,
        dd_bk_worker_type_cd,
        sk_iam_user_id_int,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm,
        dummy_user_role,
        ru_external_flg,
        ru_internal_flg,
        ru_iam_dummy_user_name,
        logging_level_cd,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_iam_user') }}
),

final AS (
    SELECT
        iam_user_key,
        bk_cpr_universal_id_int,
        internal_role,
        external_role,
        ru_cisco_worker_party_key,
        cco_id,
        source_deleted_flg,
        sk_iam_user_id_int,
        dd_bk_worker_type_cd,
        dd_cec_id,
        ru_external_flg,
        ru_internal_flg,
        ru_iam_dummy_user_name,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        dummy_user_role,
        logging_level_cd,
        ldap_user_id
    FROM source_w_iam_user
)

SELECT * FROM final