{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_item_clsfctn_audit_result', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_N_ITEM_CLSFCTN_AUDIT_RESULT',
        'target_table': 'N_ITEM_CLSFTN_AUDIT_RESULT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.753604+00:00'
    }
) }}

WITH 

source_w_item_clsftn_audit_result AS (
    SELECT
        bk_audit_name,
        bk_world_cg_cd,
        product_key,
        cg_duty_rate,
        product_source_cd,
        requestor_trained_flg,
        commit_status_flg,
        audit_status_cd,
        pool_target_type,
        src_created_dtm,
        src_updated_dtm,
        src_crtd_by_csco_wrkr_prty_key,
        src_updt_by_csco_wrkr_prty_key,
        requestor_csco_wrkr_prty_key,
        us_itm_cg_hts_clsfctn_key,
        item_cg_hts_clsfctn_key,
        ru_ccap_is_correct_flg,
        ru_us_hts_impacted_flg,
        ru_cg_hts_impacted_flg,
        ru_mis_clsfctn_reason_cmt,
        erp_item_specific_key,
        customs_item_specific_key,
        ru_new_item_specific_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_item_clsftn_audit_result') }}
),

final AS (
    SELECT
        bk_audit_name,
        bk_world_cg_cd,
        product_key,
        cg_duty_rate,
        product_source_cd,
        requestor_trained_flg,
        commit_status_flg,
        audit_status_cd,
        pool_target_type,
        src_created_dtm,
        src_updated_dtm,
        src_crtd_by_csco_wrkr_prty_key,
        src_updt_by_csco_wrkr_prty_key,
        requestor_csco_wrkr_prty_key,
        us_itm_cg_hts_clsfctn_key,
        item_cg_hts_clsfctn_key,
        ru_ccap_is_correct_flg,
        ru_us_hts_impacted_flg,
        ru_cg_hts_impacted_flg,
        ru_mis_clsfctn_reason_cmt,
        erp_item_specific_key,
        customs_item_specific_key,
        ru_new_item_specific_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_item_clsftn_audit_result
)

SELECT * FROM final