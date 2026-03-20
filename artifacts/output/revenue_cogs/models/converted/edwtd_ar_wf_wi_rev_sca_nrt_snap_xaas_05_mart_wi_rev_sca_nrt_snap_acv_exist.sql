{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_rev_sca_nrt_snap_xaas', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_WI_REV_SCA_NRT_SNAP_XAAS',
        'target_table': 'WI_REV_SCA_NRT_SNAP_ACV_EXIST',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.420270+00:00'
    }
) }}

WITH 

source_audit_revenue_post_table AS (
    SELECT
        audit_runid
    FROM {{ source('raw', 'audit_revenue_post_table') }}
),

source_wi_rev_sca_nrt_snap_acv_exist AS (
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
        sca_source_type_cd,
        sbscrptn_item_change_id_int,
        ar_trx_line_key,
        action_cd,
        ar_trx_type_short_code
    FROM {{ source('raw', 'wi_rev_sca_nrt_snap_acv_exist') }}
),

source_wi_rev_sca_nrt_snap_xaas_tv AS (
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
        sca_source_type_cd,
        sbscrptn_item_change_id_int,
        ar_trx_line_key,
        action_cd,
        ar_trx_type_short_code
    FROM {{ source('raw', 'wi_rev_sca_nrt_snap_xaas_tv') }}
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
        sca_source_type_cd,
        sbscrptn_item_change_id_int,
        ar_trx_line_key,
        action_cd,
        ar_trx_type_short_code
    FROM source_wi_rev_sca_nrt_snap_xaas_tv
)

SELECT * FROM final