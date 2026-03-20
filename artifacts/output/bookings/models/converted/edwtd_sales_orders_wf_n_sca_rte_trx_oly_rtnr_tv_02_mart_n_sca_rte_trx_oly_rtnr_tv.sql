{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_sca_rte_trx_oly_rtnr_tv', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_N_SCA_RTE_TRX_OLY_RTNR_TV',
        'target_table': 'N_SCA_RTE_TRX_OLY_RTNR_TV',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.411587+00:00'
    }
) }}

WITH 

source_n_sca_rte_trx_oly_rtnr_tv AS (
    SELECT
        sales_credit_assgn_rte_oly_key,
        bk_sls_terr_assignment_type_cd,
        sales_territory_key,
        sales_rep_number,
        sales_credit_type_cd,
        ep_transaction_id_int,
        sca_sales_commission_pct,
        sca_source_commit_dtm,
        dv_sca_source_commit_dt,
        sca_source_type_cd,
        sca_source_update_dtm,
        dv_sca_source_update_dt,
        sk_line_id_int,
        sk_line_seq_id_int,
        ss_cd,
        split_1_offer_attrib_pct,
        split_2_new_renew_pct,
        total_split_pct,
        sales_motion_cd,
        split_2_type_cd,
        sk_trx_split_id_int,
        rtnr_unique_id,
        bk_transaction_split_bu_id,
        bk_offer_attribution_id_int,
        sk_rtnr_attribution_key_int,
        ep_trx_split_sc_id_int,
        start_tv_dtm,
        end_tv_dtm,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'n_sca_rte_trx_oly_rtnr_tv') }}
),

source_n_sca_rte_trx_oly_rtnr AS (
    SELECT
        sales_credit_assgn_rte_oly_key,
        bk_sls_terr_assignment_type_cd,
        sales_territory_key,
        sales_rep_number,
        sales_credit_type_cd,
        ep_transaction_id_int,
        sca_sales_commission_pct,
        sca_source_commit_dtm,
        dv_sca_source_commit_dt,
        sca_source_type_cd,
        sca_source_update_dtm,
        dv_sca_source_update_dt,
        sk_line_id_int,
        sk_line_seq_id_int,
        ss_cd,
        split_1_offer_attrib_pct,
        split_2_new_renew_pct,
        total_split_pct,
        sales_motion_cd,
        split_2_type_cd,
        sk_trx_split_id_int,
        rtnr_unique_id,
        bk_transaction_split_bu_id,
        bk_offer_attribution_id_int,
        sk_rtnr_attribution_key_int,
        ep_trx_split_sc_id_int,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'n_sca_rte_trx_oly_rtnr') }}
),

final AS (
    SELECT
        sales_credit_assgn_rte_oly_key,
        bk_sls_terr_assignment_type_cd,
        sales_territory_key,
        sales_rep_number,
        sales_credit_type_cd,
        ep_transaction_id_int,
        sca_sales_commission_pct,
        sca_source_commit_dtm,
        dv_sca_source_commit_dt,
        sca_source_type_cd,
        sca_source_update_dtm,
        dv_sca_source_update_dt,
        sk_line_id_int,
        sk_line_seq_id_int,
        ss_cd,
        split_1_offer_attrib_pct,
        split_2_new_renew_pct,
        total_split_pct,
        sales_motion_cd,
        split_2_type_cd,
        sk_trx_split_id_int,
        rtnr_unique_id,
        bk_transaction_split_bu_id,
        bk_offer_attribution_id_int,
        sk_rtnr_attribution_key_int,
        ep_trx_split_sc_id_int,
        start_tv_dtm,
        end_tv_dtm,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_n_sca_rte_trx_oly_rtnr
)

SELECT * FROM final