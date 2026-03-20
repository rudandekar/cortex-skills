{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_ccrm_manual_adjustment', 'batch', 'edwtd_ngccrm'],
    meta={
        'source_workflow': 'wf_m_N_CCRM_MANUAL_ADJUSTMENT',
        'target_table': 'N_CCRM_MANUAL_ADJUSTMENT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.562141+00:00'
    }
) }}

WITH 

source_w_ccrm_manual_adjustment AS (
    SELECT
        ccrm_manual_adjustment_key,
        bk_adjustment_category_cd,
        bk_ccrm_profile_id_int,
        bk_ccrm_element_type_cd,
        bk_ccrm_adjustment_reason_cd,
        bk_ccrm_adj_reason_ver_num_int,
        bk_ccrm_adj_subreason_cd,
        bk_ccrm_adj_subrsn_ver_num_int,
        bk_accounting_scope_summary_cd,
        gl_eligible_flg,
        adjustment_type,
        sk_transaction_id_int,
        ru_source_created_dtm,
        ru_deferral_usd_amt,
        ru_deferral_rel_usd_amt,
        ru_accural_usd_amt,
        ru_accural_rel_usd_amt,
        ru_provision_channel_flg,
        ru_provision_usd_amt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        source_created_dtm,
        rae_adjustment_key,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_ccrm_manual_adjustment') }}
),

final AS (
    SELECT
        ccrm_manual_adjustment_key,
        bk_adjustment_category_cd,
        bk_ccrm_profile_id_int,
        bk_ccrm_element_type_cd,
        bk_ccrm_adjustment_reason_cd,
        bk_ccrm_adj_reason_ver_num_int,
        bk_ccrm_adj_subreason_cd,
        bk_ccrm_adj_subrsn_ver_num_int,
        bk_accounting_scope_summary_cd,
        gl_eligible_flg,
        adjustment_type,
        sk_transaction_id_int,
        ru_source_created_dtm,
        ru_deferral_usd_amt,
        ru_deferral_rel_usd_amt,
        ru_accural_usd_amt,
        ru_accural_rel_usd_amt,
        ru_provision_channel_flg,
        ru_provision_usd_amt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        source_created_dtm,
        rae_adjustment_key
    FROM source_w_ccrm_manual_adjustment
)

SELECT * FROM final