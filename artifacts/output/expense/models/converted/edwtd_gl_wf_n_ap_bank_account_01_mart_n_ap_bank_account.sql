{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_ap_bank_account', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_N_AP_BANK_ACCOUNT',
        'target_table': 'N_AP_BANK_ACCOUNT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.744484+00:00'
    }
) }}

WITH 

source_w_ap_bank_account AS (
    SELECT
        ap_bank_account_key,
        ap_bank_branch_key,
        bk_bank_account_num,
        bank_account_type_cd,
        bank_account_name,
        cisco_bank_account_type_cd,
        source_deleted_flg,
        sk_bank_account_id_int,
        ss_cd,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm,
        action_code,
        dml_type,
        ss_table_cd
    FROM {{ source('raw', 'w_ap_bank_account') }}
),

final AS (
    SELECT
        ap_bank_account_key,
        ap_bank_branch_key,
        bk_bank_account_num,
        bank_account_type_cd,
        bank_account_name,
        cisco_bank_account_type_cd,
        source_deleted_flg,
        sk_bank_account_id_int,
        ss_cd,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm,
        ss_table_cd
    FROM source_w_ap_bank_account
)

SELECT * FROM final