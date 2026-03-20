{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_mt_glbl_suprgrp_bkgs_measure', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_MT_GLBL_SUPRGRP_BKGS_MEASURE',
        'target_table': 'MT_GLBL_SUPRGRP_BKGS_MEASURE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.911789+00:00'
    }
) }}

WITH 

source_wi_mt_suprgrup_bkgs_msr_olbm AS (
    SELECT
        overlay_bookings_measure_key,
        bookings_process_date,
        bk_sa_member_id_int,
        ovrly_bkgs_msr_trans_type_code,
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
        dv_fmv_flg,
        bk_sls_terr_assignment_type_cd,
        overlay_bookings_process_dt
    FROM {{ source('raw', 'wi_mt_suprgrup_bkgs_msr_olbm') }}
),

source_wi_mt_suprgrup_bkgs_msr_vec AS (
    SELECT
        overlay_bookings_measure_key,
        bookings_process_date,
        bk_sa_member_id_int,
        ovrly_bkgs_msr_trans_type_code,
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
        dv_fmv_flg,
        bk_sls_terr_assignment_type_cd,
        overlay_bookings_process_dt
    FROM {{ source('raw', 'wi_mt_suprgrup_bkgs_msr_vec') }}
),

final AS (
    SELECT
        overlay_bookings_measure_key,
        bookings_process_date,
        bk_sa_member_id_int,
        ovrly_bkgs_msr_trans_type_code,
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
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        dv_fmv_flg,
        bk_sls_terr_assignment_type_cd,
        overlay_bookings_process_dt
    FROM source_wi_mt_suprgrup_bkgs_msr_vec
)

SELECT * FROM final