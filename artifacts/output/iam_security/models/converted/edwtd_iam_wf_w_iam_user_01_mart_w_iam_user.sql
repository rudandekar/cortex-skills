{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_iam_user', 'batch', 'edwtd_iam'],
    meta={
        'source_workflow': 'wf_m_W_IAM_USER',
        'target_table': 'W_IAM_USER',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:25:46.738181+00:00'
    }
) }}

WITH 

source_st_iam_edwtd_user AS (
    SELECT
        iam_user_id,
        universal_id,
        cco_id,
        emplid,
        cpr_access_level,
        iam_status,
        created_by,
        create_date,
        updated_by,
        update_date,
        cec_id,
        iam_user_type,
        log_level,
        action_code,
        batch_id,
        create_datetime
    FROM {{ source('raw', 'st_iam_edwtd_user') }}
),

final AS (
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
        dml_type,
        ldap_user_id
    FROM source_st_iam_edwtd_user
)

SELECT * FROM final