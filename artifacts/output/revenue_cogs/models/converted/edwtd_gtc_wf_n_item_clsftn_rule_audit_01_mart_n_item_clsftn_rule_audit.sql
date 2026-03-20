{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_item_clsftn_rule_audit', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_N_ITEM_CLSFTN_RULE_AUDIT',
        'target_table': 'N_ITEM_CLSFTN_RULE_AUDIT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.343683+00:00'
    }
) }}

WITH 

source_w_item_clsftn_rule_audit AS (
    SELECT
        bk_audit_name,
        audit_type_cd,
        audit_status_cd,
        src_creation_dtm,
        src_updated_dtm,
        audit_owner_csco_wrkr_prty_key,
        bk_calendar_year_int,
        bk_calendar_mth_num_int,
        src_crtd_by_csco_wrkr_prty_key,
        src_updt_by_csco_wrkr_prty_key,
        audit_record_count_type,
        sk_audit_query_id,
        ru_audit_record_cnt,
        ru_audit_record_pct,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_item_clsftn_rule_audit') }}
),

final AS (
    SELECT
        bk_audit_name,
        audit_type_cd,
        audit_status_cd,
        src_creation_dtm,
        src_updated_dtm,
        audit_owner_csco_wrkr_prty_key,
        bk_calendar_year_int,
        bk_calendar_mth_num_int,
        src_crtd_by_csco_wrkr_prty_key,
        src_updt_by_csco_wrkr_prty_key,
        audit_record_count_type,
        sk_audit_query_id,
        ru_audit_record_cnt,
        ru_audit_record_pct,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_item_clsftn_rule_audit
)

SELECT * FROM final