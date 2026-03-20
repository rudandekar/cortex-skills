{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_eac_rule', 'batch', 'edwtd_party_sav'],
    meta={
        'source_workflow': 'wf_m_N_EAC_RULE',
        'target_table': 'N_EAC_RULE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.649318+00:00'
    }
) }}

WITH 

source_w_eac_rule AS (
    SELECT
        bk_eac_rule_id,
        crtd_by_csco_wrkr_prty_key,
        lst_uptd_by_csco_wrkr_prty_key,
        bk_eac_group_id,
        rule_status_name,
        rule_start_dt,
        rule_end_dt,
        src_created_dtm,
        dv_src_created_dt,
        src_last_uptd_dtm,
        dv_src_last_uptd_dt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        parent_eac_rule_id,
        parent_eac_rule_name,
        rule_usage_application_code,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_eac_rule') }}
),

final AS (
    SELECT
        bk_eac_rule_id,
        crtd_by_csco_wrkr_prty_key,
        lst_uptd_by_csco_wrkr_prty_key,
        bk_eac_group_id,
        rule_status_name,
        rule_start_dt,
        rule_end_dt,
        src_created_dtm,
        dv_src_created_dt,
        src_last_uptd_dtm,
        dv_src_last_uptd_dt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        parent_eac_rule_id,
        parent_eac_rule_name,
        rule_usage_application_code
    FROM source_w_eac_rule
)

SELECT * FROM final