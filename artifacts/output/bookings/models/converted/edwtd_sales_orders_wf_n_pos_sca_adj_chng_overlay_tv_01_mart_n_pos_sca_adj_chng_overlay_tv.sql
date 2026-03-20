{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_pos_sca_adj_chng_overlay_tv', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_N_POS_SCA_ADJ_CHNG_OVERLAY_TV',
        'target_table': 'N_POS_SCA_ADJ_CHNG_OVERLAY_TV',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.104721+00:00'
    }
) }}

WITH 

source_w_pos_sca_adj_chng_overlay AS (
    SELECT
        bk_sls_terr_assignment_type_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        end_tv_dtm,
        pd_adjustment_dt,
        pd_assignment_mode_cd,
        pd_bk_pos_transaction_id_int,
        pd_bk_sales_credit_type_cd,
        pd_bk_sales_rep_num,
        pd_created_by_erp_user_name,
        pd_sales_commission_pct,
        pd_sales_territory_key,
        pd_ss_cd,
        pd_transaction_dt,
        pos_scaac_key,
        sk_trx_sc_id_int,
        start_tv_dtm,
        action_code,
        dml_type,
        bk_sls_crdt_asgnmnt_reason_cd,
        pd_sales_credit_usage_cd,
        pd_sls_credit_unallocated_flg,
        src_rptd_rebok_otm_terr_id_int,
        src_rptd_rebok_otm_terr_name,
        src_rptd_rebok_otm_terr_typ_cd,
        pd_sls_credit_last_update_dtm,
        ucrm_case_num,
        bk_offer_attribution_id_int,
        ep_trx_split_sc_id
    FROM {{ source('raw', 'w_pos_sca_adj_chng_overlay') }}
),

source_n_pos_sca_adj_chng_overlay_tv AS (
    SELECT
        bk_sls_terr_assignment_type_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        end_tv_dtm,
        pd_adjustment_dt,
        pd_assignment_mode_cd,
        pd_bk_pos_transaction_id_int,
        pd_bk_sales_credit_type_cd,
        pd_bk_sales_rep_num,
        pd_created_by_erp_user_name,
        pd_sales_commission_pct,
        pd_sales_territory_key,
        pd_ss_cd,
        pd_transaction_dt,
        pos_scaac_key,
        sk_trx_sc_id_int,
        start_tv_dtm,
        bk_sls_crdt_asgnmnt_reason_cd,
        pd_sales_credit_usage_cd,
        pd_sls_credit_unallocated_flg,
        src_rptd_rebok_otm_terr_id_int,
        src_rptd_rebok_otm_terr_name,
        src_rptd_rebok_otm_terr_typ_cd,
        pd_sls_credit_last_update_dtm,
        ucrm_case_num,
        bk_offer_attribution_id_int,
        ep_trx_split_sc_id
    FROM {{ source('raw', 'n_pos_sca_adj_chng_overlay_tv') }}
),

final AS (
    SELECT
        bk_sls_terr_assignment_type_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        end_tv_dtm,
        pd_adjustment_dt,
        pd_assignment_mode_cd,
        pd_bk_pos_transaction_id_int,
        pd_bk_sales_credit_type_cd,
        pd_bk_sales_rep_num,
        pd_created_by_erp_user_name,
        pd_sales_commission_pct,
        pd_sales_territory_key,
        pd_ss_cd,
        pd_transaction_dt,
        pos_scaac_key,
        sk_trx_sc_id_int,
        start_tv_dtm,
        bk_sls_crdt_asgnmnt_reason_cd,
        pd_sales_credit_usage_cd,
        pd_sls_credit_unallocated_flg,
        src_rptd_rebok_otm_terr_id_int,
        src_rptd_rebok_otm_terr_name,
        src_rptd_rebok_otm_terr_typ_cd,
        pd_sls_credit_last_update_dtm,
        ucrm_case_num,
        bk_offer_attribution_id_int,
        ep_trx_split_sc_id
    FROM source_n_pos_sca_adj_chng_overlay_tv
)

SELECT * FROM final