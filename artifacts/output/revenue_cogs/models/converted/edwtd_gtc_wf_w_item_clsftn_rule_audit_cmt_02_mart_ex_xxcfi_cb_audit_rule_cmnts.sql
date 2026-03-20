{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_item_clsftn_rule_audit_cmt', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_W_ITEM_CLSFTN_RULE_AUDIT_CMT',
        'target_table': 'EX_XXCFI_CB_AUDIT_RULE_CMNTS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.120757+00:00'
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

source_ex_xxcfi_cb_audit_rule_cmnts AS (
    SELECT
        audit_comment_id,
        audit_query_id,
        rule_id,
        country_group_code,
        auditor_comments,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        create_datetime,
        action_code,
        exception_type
    FROM {{ source('raw', 'ex_xxcfi_cb_audit_rule_cmnts') }}
),

final AS (
    SELECT
        audit_comment_id,
        audit_query_id,
        rule_id,
        country_group_code,
        auditor_comments,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        create_datetime,
        action_code,
        exception_type
    FROM source_ex_xxcfi_cb_audit_rule_cmnts
)

SELECT * FROM final