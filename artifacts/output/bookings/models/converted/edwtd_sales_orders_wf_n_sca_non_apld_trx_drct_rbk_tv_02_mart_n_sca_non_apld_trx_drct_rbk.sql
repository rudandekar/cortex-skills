{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_sca_non_apld_trx_drct_rbk_tv', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_N_SCA_NON_APLD_TRX_DRCT_RBK_TV',
        'target_table': 'N_SCA_NON_APLD_TRX_DRCT_RBK',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.529596+00:00'
    }
) }}

WITH 

source_w_sca_non_apld_trx_drct_rbk AS (
    SELECT
        sales_cr_assgn_nonappld_key,
        rebok_channel_flg,
        src_rptd_rebok_otm_terr_id_int,
        src_rptd_rebok_otm_terr_name,
        src_rptd_rebok_otm_terr_typ_cd,
        sk_trx_sc_id_int,
        pd_assignment_mode_cd,
        pd_scan_creation_dt,
        pd_bk_sales_credit_type_cd,
        pd_sales_rep_num,
        sales_territory_key,
        pd_ar_trx_line_key,
        pd_sales_commission_pct,
        pd_ss_cd,
        pd_created_by_erp_user_name,
        bk_sls_crdt_asgnmnt_reason_cd,
        pd_sales_credit_usage_cd,
        pd_sls_credit_unallocated_flg,
        pd_sls_credit_last_update_dtm,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        start_tv_dtm,
        end_tv_dtm,
        ucrm_case_num,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_sca_non_apld_trx_drct_rbk') }}
),

source_n_sca_non_apld_trx_drct_rbk_tv AS (
    SELECT
        sales_cr_assgn_nonappld_key,
        rebok_channel_flg,
        src_rptd_rebok_otm_terr_id_int,
        src_rptd_rebok_otm_terr_name,
        src_rptd_rebok_otm_terr_typ_cd,
        sk_trx_sc_id_int,
        pd_assignment_mode_cd,
        pd_scan_creation_dt,
        pd_bk_sales_credit_type_cd,
        pd_sales_rep_num,
        sales_territory_key,
        pd_ar_trx_line_key,
        pd_sales_commission_pct,
        pd_ss_cd,
        pd_created_by_erp_user_name,
        bk_sls_crdt_asgnmnt_reason_cd,
        pd_sales_credit_usage_cd,
        pd_sls_credit_unallocated_flg,
        pd_sls_credit_last_update_dtm,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        ucrm_case_num,
        start_tv_dtm,
        end_tv_dtm
    FROM {{ source('raw', 'n_sca_non_apld_trx_drct_rbk_tv') }}
),

final AS (
    SELECT
        sales_cr_assgn_nonappld_key,
        rebok_channel_flg,
        src_rptd_rebok_otm_terr_id_int,
        src_rptd_rebok_otm_terr_name,
        src_rptd_rebok_otm_terr_typ_cd,
        sk_trx_sc_id_int,
        pd_assignment_mode_cd,
        pd_scan_creation_dt,
        pd_bk_sales_credit_type_cd,
        pd_sales_rep_num,
        sales_territory_key,
        pd_ar_trx_line_key,
        pd_sales_commission_pct,
        pd_ss_cd,
        pd_created_by_erp_user_name,
        bk_sls_crdt_asgnmnt_reason_cd,
        pd_sales_credit_usage_cd,
        pd_sls_credit_unallocated_flg,
        pd_sls_credit_last_update_dtm,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        ucrm_case_num
    FROM source_n_sca_non_apld_trx_drct_rbk_tv
)

SELECT * FROM final