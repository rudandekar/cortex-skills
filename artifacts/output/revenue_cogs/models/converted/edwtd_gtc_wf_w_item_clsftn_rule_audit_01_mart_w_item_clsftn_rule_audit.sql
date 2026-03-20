{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_item_clsftn_rule_audit', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_W_ITEM_CLSFTN_RULE_AUDIT',
        'target_table': 'W_ITEM_CLSFTN_RULE_AUDIT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:43.969438+00:00'
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

source_ex_xxcfi_cb_audit_query AS (
    SELECT
        audit_comments,
        audit_commited,
        audit_country_group,
        audit_fiscal_year,
        audit_month,
        audit_name,
        audit_owner,
        audit_query_id,
        audit_record_count,
        audit_record_count_type,
        audit_status,
        audit_type,
        audit_year,
        created_by,
        creation_date,
        last_updated_by,
        last_updated_date,
        create_datetime,
        action_code,
        exception_type
    FROM {{ source('raw', 'ex_xxcfi_cb_audit_query') }}
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
        edw_update_user,
        action_code,
        dml_type
    FROM source_ex_xxcfi_cb_audit_query
)

SELECT * FROM final