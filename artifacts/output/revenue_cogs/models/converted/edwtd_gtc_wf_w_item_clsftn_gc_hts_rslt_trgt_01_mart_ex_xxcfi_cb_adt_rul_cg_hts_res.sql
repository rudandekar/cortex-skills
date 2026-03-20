{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_item_clsftn_gc_hts_rslt_trgt', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_W_ITEM_CLSFTN_GC_HTS_RSLT_TRGT',
        'target_table': 'EX_XXCFI_CB_ADT_RUL_CG_HTS_RES',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.939063+00:00'
    }
) }}

WITH 

source_ex_xxcfi_cb_adt_rul_cg_hts_res AS (
    SELECT
        audit_result_id,
        audit_query_id,
        audit_month,
        audit_year,
        audit_fiscal_year,
        rule_id,
        specific_name,
        pid_count,
        country_group_code,
        hts_code,
        duty_rate,
        score,
        rule_cg_comment,
        pool_target_flag,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        new_hts_code,
        audit_performed,
        specific_id,
        create_datetime,
        action_code,
        exception_type
    FROM {{ source('raw', 'ex_xxcfi_cb_adt_rul_cg_hts_res') }}
),

source_w_item_clsftn_gc_hts_rslt_trgt AS (
    SELECT
        bk_audit_name,
        bk_rule_id_int,
        bk_world_cg_cd,
        product_cnt,
        src_created_dtm,
        src_updated_dtm,
        audit_rule_cg_cmt,
        pool_target_cd,
        cg_duty_rt,
        src_crtd_by_csco_wrkr_prty_key,
        src_updt_by_csco_wrkr_prty_key,
        hts_clsfctn_rule_key,
        mdfd_hts_clsfctn_rule_key,
        hts_mapping_to_cg_score_cd,
        customs_item_specific_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        src_deleted_flg,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_item_clsftn_gc_hts_rslt_trgt') }}
),

final AS (
    SELECT
        audit_result_id,
        audit_query_id,
        audit_month,
        audit_year,
        audit_fiscal_year,
        rule_id,
        specific_name,
        pid_count,
        country_group_code,
        hts_code,
        duty_rate,
        score,
        rule_cg_comment,
        pool_target_flag,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        new_hts_code,
        audit_performed,
        specific_id,
        create_datetime,
        action_code,
        exception_type
    FROM source_w_item_clsftn_gc_hts_rslt_trgt
)

SELECT * FROM final