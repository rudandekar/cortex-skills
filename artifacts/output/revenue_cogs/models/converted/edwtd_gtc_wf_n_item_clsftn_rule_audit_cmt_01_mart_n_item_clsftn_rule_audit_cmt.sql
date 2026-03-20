{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_item_clsftn_rule_audit_cmt', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_N_ITEM_CLSFTN_RULE_AUDIT_CMT',
        'target_table': 'N_ITEM_CLSFTN_RULE_AUDIT_CMT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.740796+00:00'
    }
) }}

WITH 

source_w_item_clsftn_rule_audit_cmt AS (
    SELECT
        bk_audit_name,
        bk_world_cg_cd,
        bk_src_created_dtm,
        rule_id_int,
        src_updated_dtm,
        audit_cmt,
        src_crtd_by_csco_wrkr_prty_key,
        src_updt_by_csco_wrkr_prty_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_item_clsftn_rule_audit_cmt') }}
),

final AS (
    SELECT
        bk_audit_name,
        bk_world_cg_cd,
        bk_src_created_dtm,
        rule_id_int,
        src_updated_dtm,
        audit_cmt,
        src_crtd_by_csco_wrkr_prty_key,
        src_updt_by_csco_wrkr_prty_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_item_clsftn_rule_audit_cmt
)

SELECT * FROM final