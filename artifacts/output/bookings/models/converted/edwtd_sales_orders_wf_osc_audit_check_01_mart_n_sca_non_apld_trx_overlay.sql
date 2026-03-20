{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_osc_audit_check', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_OSC_AUDIT_CHECK',
        'target_table': 'N_SCA_NON_APLD_TRX_OVERLAY',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.367879+00:00'
    }
) }}

WITH 

source_n_sca_non_apld_trx_overlay AS (
    SELECT
        bk_sls_terr_assignment_type_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        pd_ar_trx_line_key,
        pd_assignment_mode_cd,
        pd_bk_sales_credit_type_cd,
        pd_created_by_erp_user_name,
        pd_sales_commission_pct,
        pd_sales_rep_num,
        pd_sales_territory_key,
        pd_scan_creation_dt,
        pd_ss_cd,
        sales_cr_assgn_nonappld_key,
        sk_trx_sc_id_int,
        bk_offer_attribution_id_int,
        ep_trx_split_sc_id
    FROM {{ source('raw', 'n_sca_non_apld_trx_overlay') }}
),

transformed_exp_n_sca_non_apld_trx_overlay_dups AS (
    SELECT
    pd_bk_pos_transaction_id_int,
    pd_sales_territory_key,
    pd_sales_rep_num,
    bk_sls_terr_assignment_type_cd,
    bk_offer_attribution_id_int,
    pd_sales_commission_pct,
    ep_trx_split_sc_id,
    count1,
    IFF(NOT ISNULL(PD_BK_POS_TRANSACTION_ID_INT),ABORT('Duplicate records found for Sales Credits tables')) AS dups
    FROM source_n_sca_non_apld_trx_overlay
),

filtered_flt_n_sca_non_apld_trx_overlay_dups AS (
    SELECT *
    FROM transformed_exp_n_sca_non_apld_trx_overlay_dups
    WHERE FALSE
),

filtered_flt_n_sca_non_apld_trx_overlay_sum AS (
    SELECT *
    FROM filtered_flt_n_sca_non_apld_trx_overlay_dups
    WHERE FALSE
),

transformed_exp_n_sca_non_apld_trx_overlay_sum AS (
    SELECT
    pd_bk_pos_transaction_id_int,
    bk_offer_attribution_id_int,
    bk_sls_terr_assignment_type_cd,
    ep_trx_split_sc_id,
    count1,
    sums,
    IFF(NOT ISNULL(PD_BK_POS_TRANSACTION_ID_INT),ABORT('Sum exceeds greater than 100 for Sales Credits tables')) AS sum
    FROM filtered_flt_n_sca_non_apld_trx_overlay_sum
),

final AS (
    SELECT
        bk_sls_terr_assignment_type_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        pd_ar_trx_line_key,
        pd_assignment_mode_cd,
        pd_bk_sales_credit_type_cd,
        pd_created_by_erp_user_name,
        pd_sales_commission_pct,
        pd_sales_rep_num,
        pd_sales_territory_key,
        pd_scan_creation_dt,
        pd_ss_cd,
        sales_cr_assgn_nonappld_key,
        sk_trx_sc_id_int,
        bk_sls_crdt_asgnmnt_reason_cd,
        pd_sales_credit_usage_cd,
        pd_sls_credit_unallocated_flg,
        src_rptd_rbk_otm_terr_id_int,
        src_rptd_rbk_otm_terr_name,
        src_rptd_rbk_otm_terr_type_cd,
        pd_sls_credit_last_update_dtm,
        ucrm_case_num,
        bk_offer_attribution_id_int,
        ep_trx_split_sc_id
    FROM transformed_exp_n_sca_non_apld_trx_overlay_sum
)

SELECT * FROM final