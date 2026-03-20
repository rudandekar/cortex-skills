{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_item_clsftn_adt_rslt_tgt_cmt', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_W_ITEM_CLSFTN_ADT_RSLT_TGT_CMT',
        'target_table': 'EX_XXCFI_CB_AUDIT_CLSFN_CMNTS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.073975+00:00'
    }
) }}

WITH 

source_ex_xxcfi_cb_audit_clsfn_cmnts AS (
    SELECT
        audit_comment_id,
        audit_query_id,
        pid,
        auditor_comments,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        create_datetime,
        action_code,
        exception_type
    FROM {{ source('raw', 'ex_xxcfi_cb_audit_clsfn_cmnts') }}
),

source_w_item_clsftn_adt_rslt_tgt_cmt AS (
    SELECT
        bk_audit_name,
        product_key,
        bk_src_created_dtm,
        src_updated_dtm,
        auditor_cmt,
        src_crtd_by_csco_wrkr_prty_key,
        src_updt_by_csco_wrkr_prty_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_item_clsftn_adt_rslt_tgt_cmt') }}
),

final AS (
    SELECT
        audit_comment_id,
        audit_query_id,
        pid,
        auditor_comments,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        create_datetime,
        action_code,
        exception_type
    FROM source_w_item_clsftn_adt_rslt_tgt_cmt
)

SELECT * FROM final