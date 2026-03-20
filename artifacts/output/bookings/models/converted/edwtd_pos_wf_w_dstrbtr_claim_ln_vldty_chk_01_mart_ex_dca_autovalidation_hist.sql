{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_dstrbtr_claim_ln_vldty_chk', 'batch', 'edwtd_pos'],
    meta={
        'source_workflow': 'wf_m_W_DSTRBTR_CLAIM_LN_VLDTY_CHK',
        'target_table': 'EX_DCA_AUTOVALIDATION_HIST',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.061406+00:00'
    }
) }}

WITH 

source_ex_dca_autovalidation_hist AS (
    SELECT
        claim_detail_id,
        claim_id,
        check_status,
        comments_rsn_txt,
        check_code,
        result_id,
        last_update_date,
        validation_reason_code,
        correction_action,
        active_flag,
        create_datetime,
        batch_id,
        action_cd,
        exception_type
    FROM {{ source('raw', 'ex_dca_autovalidation_hist') }}
),

source_st_dca_autovalidation_hist AS (
    SELECT
        claim_detail_id,
        claim_id,
        check_status,
        comments_rsn_txt,
        check_code,
        result_id,
        last_update_date,
        validation_reason_code,
        correction_action,
        active_flag,
        create_datetime,
        batch_id,
        action_cd
    FROM {{ source('raw', 'st_dca_autovalidation_hist') }}
),

final AS (
    SELECT
        claim_detail_id,
        claim_id,
        check_status,
        comments_rsn_txt,
        check_code,
        result_id,
        last_update_date,
        validation_reason_code,
        correction_action,
        active_flag,
        create_datetime,
        batch_id,
        action_cd,
        exception_type
    FROM source_st_dca_autovalidation_hist
)

SELECT * FROM final