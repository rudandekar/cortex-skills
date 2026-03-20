{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_sca_apld_trx_direct_rebok_tv', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_N_SCA_APLD_TRX_DIRECT_REBOK_TV',
        'target_table': 'N_SCA_APLD_TRX_DIRECT_REBOK',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.648677+00:00'
    }
) }}

WITH 

source_w_sca_apld_trx_direct_rebok AS (
    SELECT
        sales_credit_assgn_appld_key,
        rebok_channel_flg,
        src_rptd_rebok_otm_terr_id_int,
        src_rptd_rebok_otm_terr_name,
        src_rptd_rebok_otm_terr_typ_cd,
        sk_trx_sc_id_int,
        pd_assignment_mode_cd,
        pd_scaa_creation_dt,
        pd_sales_credit_type_cd,
        pd_sales_rep_num,
        pd_sales_territory_key,
        pd_sales_order_line_key,
        pd_sales_commission_pct,
        pd_sca_source_type_cd,
        pd_ss_cd,
        pd_created_by_erp_user_name,
        bk_sls_crdt_asgnmnt_reason_cd,
        pd_sls_credit_last_update_dtm,
        pd_sls_credit_unallocated_flg,
        pd_sales_credit_usage_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        start_tv_dtm,
        end_tv_dtm,
        ucrm_case_num,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_sca_apld_trx_direct_rebok') }}
),

source_n_sca_apld_trx_direct_rebok_tv AS (
    SELECT
        sales_credit_assgn_appld_key,
        rebok_channel_flg,
        src_rptd_rebok_otm_terr_id_int,
        src_rptd_rebok_otm_terr_name,
        src_rptd_rebok_otm_terr_typ_cd,
        sk_trx_sc_id_int,
        pd_assignment_mode_cd,
        pd_scaa_creation_dt,
        pd_sales_credit_type_cd,
        pd_sales_rep_num,
        pd_sales_territory_key,
        pd_sales_order_line_key,
        pd_sales_commission_pct,
        pd_sca_source_type_cd,
        pd_ss_cd,
        pd_created_by_erp_user_name,
        bk_sls_crdt_asgnmnt_reason_cd,
        pd_sls_credit_last_update_dtm,
        pd_sls_credit_unallocated_flg,
        pd_sales_credit_usage_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        start_tv_dtm,
        end_tv_dtm,
        ucrm_case_num
    FROM {{ source('raw', 'n_sca_apld_trx_direct_rebok_tv') }}
),

final AS (
    SELECT
        sales_credit_assgn_appld_key,
        rebok_channel_flg,
        src_rptd_rebok_otm_terr_id_int,
        src_rptd_rebok_otm_terr_name,
        src_rptd_rebok_otm_terr_typ_cd,
        sk_trx_sc_id_int,
        pd_assignment_mode_cd,
        pd_scaa_creation_dt,
        pd_sales_credit_type_cd,
        pd_sales_rep_num,
        pd_sales_territory_key,
        pd_sales_order_line_key,
        pd_sales_commission_pct,
        pd_sca_source_type_cd,
        pd_ss_cd,
        pd_created_by_erp_user_name,
        bk_sls_crdt_asgnmnt_reason_cd,
        pd_sls_credit_last_update_dtm,
        pd_sls_credit_unallocated_flg,
        pd_sales_credit_usage_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        ucrm_case_num
    FROM source_n_sca_apld_trx_direct_rebok_tv
)

SELECT * FROM final