{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_item_clsftn_gc_hts_rslt_pool', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_N_ITEM_CLSFTN_GC_HTS_RSLT_POOL',
        'target_table': 'N_ITEM_CLSFTN_GC_HTS_RSLT_POOL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.061847+00:00'
    }
) }}

WITH 

source_w_item_clsftn_gc_hts_rslt_pool AS (
    SELECT
        bk_audit_name,
        bk_rule_id_int,
        commit_status_flg,
        rule_cmt,
        rule_rank_int,
        product_cnt,
        src_created_dtm,
        src_updated_dtm,
        src_crtd_by_csco_wrkr_prty_key,
        src_updt_by_csco_wrkr_prty_key,
        pool_target_cd,
        customs_item_specific_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_item_clsftn_gc_hts_rslt_pool') }}
),

final AS (
    SELECT
        bk_audit_name,
        bk_rule_id_int,
        commit_status_flg,
        rule_cmt,
        rule_rank_int,
        product_cnt,
        src_created_dtm,
        src_updated_dtm,
        src_crtd_by_csco_wrkr_prty_key,
        src_updt_by_csco_wrkr_prty_key,
        pool_target_cd,
        customs_item_specific_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_item_clsftn_gc_hts_rslt_pool
)

SELECT * FROM final