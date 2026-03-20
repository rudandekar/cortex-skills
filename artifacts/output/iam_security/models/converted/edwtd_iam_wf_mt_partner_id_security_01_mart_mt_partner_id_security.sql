{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_mt_partner_id_security', 'batch', 'edwtd_iam'],
    meta={
        'source_workflow': 'wf_m_MT_PARTNER_ID_SECURITY',
        'target_table': 'MT_PARTNER_ID_SECURITY',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:25:46.782156+00:00'
    }
) }}

WITH 

source_n_iam_user AS (
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
        logging_level_cd
    FROM {{ source('raw', 'n_iam_user') }}
),

final AS (
    SELECT
        iam_user_key,
        bk_cpr_universal_id_int,
        internal_role,
        cco_id,
        external_role,
        sk_iam_user_id_int,
        ru_external_user_email_addr,
        ru_cisco_worker_party_key,
        dd_bk_worker_type_cd,
        dd_cec_id,
        dummy_user_role,
        ru_external_flg,
        ru_internal_flg,
        ru_iam_dummy_user_name,
        iam_application_key,
        application_name,
        bk_iam_role_name,
        assignment_type,
        standard_or_exception_flg,
        exclusive_or_restrictive_flg,
        iam_level_num_int,
        partner_id_int,
        partner_party_key
    FROM source_n_iam_user
)

SELECT * FROM final