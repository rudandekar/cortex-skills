{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_xaas_sls_crdt_asgnmt_sls_trx', 'batch', 'edwtd_xaas_bkgs'],
    meta={
        'source_workflow': 'wf_m_W_XAAS_SLS_CRDT_ASGNMT_SLS_TRX',
        'target_table': 'W_XAAS_SLS_CRDT_ASGNMT_SLS_TRX',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.700161+00:00'
    }
) }}

WITH 

source_w_xaas_sls_crdt_asgnmt_sls_trx AS (
    SELECT
        sls_credit_asgnmt_sls_trx_key,
        sales_credit_usage_cd,
        sales_credit_unallocated_flg,
        sls_credit_asgnmt_mode_cd,
        sk_trx_sc_id_int,
        overlay_type,
        bk_sales_rep_num,
        bk_sls_terr_asgnmt_type_cd,
        so_sbscrptn_itm_sls_trx_key,
        sales_territory_key,
        src_rptd_rbk_otm_terr_id_int,
        src_rptd_rbk_otm_terr_name,
        src_rptd_rbk_otm_terr_type_cd,
        bk_sales_credit_type_code,
        bk_created_by_erp_user_name,
        bk_sls_crdt_asgnmt_rsn_cd,
        sca_create_dtm,
        dv_sca_create_dt,
        sca_last_update_dtm,
        dv_sca_last_update_dt,
        split_pct,
        effective_start_dtm,
        dv_effective_start_dt,
        effective_end_dtm,
        dv_effective_end_dt,
        ru_rebok_channel_flg,
        source_commit_dtm,
        dv_source_commit_dt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        ucrm_case_num,
        bk_offer_attribution_id_int,
        ep_trx_split_sc_id,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_xaas_sls_crdt_asgnmt_sls_trx') }}
),

final AS (
    SELECT
        sls_credit_asgnmt_sls_trx_key,
        sales_credit_usage_cd,
        sales_credit_unallocated_flg,
        sls_credit_asgnmt_mode_cd,
        sk_trx_sc_id_int,
        overlay_type,
        bk_sales_rep_num,
        bk_sls_terr_asgnmt_type_cd,
        so_sbscrptn_itm_sls_trx_key,
        sales_territory_key,
        src_rptd_rbk_otm_terr_id_int,
        src_rptd_rbk_otm_terr_name,
        src_rptd_rbk_otm_terr_type_cd,
        bk_sales_credit_type_code,
        bk_created_by_erp_user_name,
        bk_sls_crdt_asgnmt_rsn_cd,
        sca_create_dtm,
        dv_sca_create_dt,
        sca_last_update_dtm,
        dv_sca_last_update_dt,
        split_pct,
        effective_start_dtm,
        dv_effective_start_dt,
        effective_end_dtm,
        dv_effective_end_dt,
        ru_rebok_channel_flg,
        source_commit_dtm,
        dv_source_commit_dt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        ucrm_case_num,
        bk_offer_attribution_id_int,
        ep_trx_split_sc_id,
        action_code,
        dml_type
    FROM source_w_xaas_sls_crdt_asgnmt_sls_trx
)

SELECT * FROM final