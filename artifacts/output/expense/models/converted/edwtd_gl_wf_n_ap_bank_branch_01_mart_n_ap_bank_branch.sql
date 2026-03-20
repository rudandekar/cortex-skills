{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_ap_bank_branch', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_N_AP_BANK_BRANCH',
        'target_table': 'N_AP_BANK_BRANCH',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.661460+00:00'
    }
) }}

WITH 

source_w_ap_bank_branch AS (
    SELECT
        ap_bank_branch_key,
        bk_bank_branch_name,
        bk_bank_name,
        sk_bank_branch_id_int,
        bank_num,
        ss_cd,
        source_deleted_flg,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_ap_bank_branch') }}
),

final AS (
    SELECT
        ap_bank_branch_key,
        bk_bank_branch_name,
        bk_bank_name,
        sk_bank_branch_id_int,
        bank_num,
        ss_cd,
        source_deleted_flg,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm
    FROM source_w_ap_bank_branch
)

SELECT * FROM final