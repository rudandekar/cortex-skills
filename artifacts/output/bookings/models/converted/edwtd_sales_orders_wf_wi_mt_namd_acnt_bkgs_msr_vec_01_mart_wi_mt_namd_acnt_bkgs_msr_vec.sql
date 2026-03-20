{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_mt_namd_acnt_bkgs_msr_vec', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_WI_MT_NAMD_ACNT_BKGS_MSR_VEC',
        'target_table': 'WI_MT_NAMD_ACNT_BKGS_MSR_VEC',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.447358+00:00'
    }
) }}

WITH 

source_n_flvdt_endcst_ss_itmslstrx_tv AS (
    SELECT
        bk_sa_member_id_int,
        so_sbscrptn_itm_sls_trx_key,
        novalid_slsacctgrp_ptyrsn_name,
        owned_flg,
        hq_remote_cd,
        enrchmt_cust_type_cd,
        asgnmt_source_system_cd,
        enrchmt_sls_acct_pty_type_cd,
        validation_mode_cd,
        end_cust_ownership_split_pct,
        sk_trx_party_extn_id_int,
        last_updated_dtm,
        validation_status_cd,
        last_updated_by_user_name,
        sales_acct_grp_member_pty_key,
        fld_vldtd_end_cust_pty_key,
        sales_acct_group_pty_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        start_tv_dt,
        end_tv_dt
    FROM {{ source('raw', 'n_flvdt_endcst_ss_itmslstrx_tv') }}
),

source_n_vldtd_end_cust_napld_trx_tv AS (
    SELECT
        ar_transaction_line_key,
        bk_sa_member_id_int,
        start_tv_dtm,
        end_tv_dtm,
        field_vldtd_end_cust_prty_key,
        sls_acct_grp_member_party_key,
        sales_account_group_party_key,
        enrchmnt_sls_acct_prty_typ_cd,
        enrichment_customer_type_cd,
        end_cust_ownership_split_pct,
        no_vld_sls_acct_pty_rsn_name,
        owned_flg,
        hq_remote_cd,
        assignment_source_system_cd,
        validation_mode_cd,
        validation_status_cd,
        last_updated_by_user_name,
        last_updated_dtm,
        sk_trx_party_extension_id_int,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'n_vldtd_end_cust_napld_trx_tv') }}
),

source_n_vldtd_end_cust_pos_trx_ln AS (
    SELECT
        bk_pos_transaction_id_int,
        field_vldtd_end_cust_prty_key,
        sales_account_group_party_key,
        sls_acct_grp_member_party_key,
        no_vld_sls_acct_pty_rsn_name,
        enrchmnt_sls_acct_prty_typ_cd,
        enrichment_customer_type_cd,
        end_cust_ownership_split_pct,
        validation_mode_cd,
        validation_status_cd,
        owned_flg,
        hq_remote_cd,
        assignment_source_system_cd,
        last_updated_dtm,
        last_updated_by_user_name,
        sk_trx_party_extension_id_int,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'n_vldtd_end_cust_pos_trx_ln') }}
),

source_n_vldtd_end_cst_rbk_mnl_trx AS (
    SELECT
        manual_trx_key,
        field_vldtd_end_cust_prty_key,
        sales_account_group_party_key,
        sls_acct_grp_member_party_key,
        no_vld_sls_acct_pty_rsn_name,
        enrchmnt_sls_acct_prty_typ_cd,
        enrichment_customer_type_cd,
        end_cust_ownership_split_pct,
        validation_mode_cd,
        validation_status_cd,
        owned_flg,
        hq_remote_cd,
        assignment_source_system_cd,
        last_updated_dtm,
        last_updated_by_user_name,
        sk_trx_party_extension_id_int,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'n_vldtd_end_cst_rbk_mnl_trx') }}
),

source_n_vldtd_end_cust_for_so_ln AS (
    SELECT
        sales_order_line_key,
        field_vldtd_end_cust_prty_key,
        sales_account_group_party_key,
        sls_acct_grp_member_party_key,
        no_vld_sls_acct_pty_rsn_name,
        enrchmnt_sls_acct_prty_typ_cd,
        enrichment_customer_type_cd,
        end_cust_ownership_split_pct,
        validation_mode_cd,
        validation_status_cd,
        owned_flg,
        hq_remote_cd,
        assignment_source_system_cd,
        last_updated_dtm,
        last_updated_by_user_name,
        sk_trx_party_extension_id_int,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'n_vldtd_end_cust_for_so_ln') }}
),

final AS (
    SELECT
        bookings_measure_key,
        bookings_process_date,
        bk_sa_member_id_int,
        bkgs_measure_trans_type_code,
        assignment_source_system_cd,
        end_cust_ownership_split_pct,
        enrichment_customer_type_cd,
        field_vldtd_end_cust_prty_key,
        hq_remote_cd,
        last_updated_by_user_name,
        last_updated_dtm,
        no_vld_sls_acct_pty_rsn_name,
        owned_flg,
        sales_account_group_party_key,
        sk_trx_party_extension_id_int,
        sls_acct_grp_member_party_key,
        validation_mode_cd,
        validation_status_cd,
        dv_fmv_flg
    FROM source_n_vldtd_end_cust_for_so_ln
)

SELECT * FROM final