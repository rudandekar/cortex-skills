{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_dstrbtr_claim_ln_vldty_chk', 'batch', 'edwtd_pos'],
    meta={
        'source_workflow': 'wf_m_N_DSTRBTR_CLAIM_LN_VLDTY_CHK',
        'target_table': 'N_DSTRBTR_CLAIM_LN_VLDTY_CHK',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.287774+00:00'
    }
) }}

WITH 

source_w_dstrbtr_claim_ln_vldty_chk AS (
    SELECT
        bk_claim_detail_id_int,
        bk_claim_id_int,
        approval_rsn_txt,
        sk_result_id_int,
        bk_claim_rejection_rsn_cd,
        validity_check_status_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type,
        corrective_action_cd,
        validation_reason_cd,
        active_flg
    FROM {{ source('raw', 'w_dstrbtr_claim_ln_vldty_chk') }}
),

final AS (
    SELECT
        bk_claim_detail_id_int,
        bk_claim_id_int,
        approval_rsn_txt,
        sk_result_id_int,
        bk_claim_rejection_rsn_cd,
        validity_check_status_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        corrective_action_cd,
        validation_reason_cd,
        active_flg
    FROM source_w_dstrbtr_claim_ln_vldty_chk
)

SELECT * FROM final