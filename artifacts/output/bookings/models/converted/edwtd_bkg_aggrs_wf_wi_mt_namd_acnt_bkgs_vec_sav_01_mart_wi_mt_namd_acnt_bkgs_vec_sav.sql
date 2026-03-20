{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_mt_namd_acnt_bkgs_vec_sav', 'batch', 'edwtd_bkg_aggrs'],
    meta={
        'source_workflow': 'wf_m_WI_MT_NAMD_ACNT_BKGS_VEC_SAV',
        'target_table': 'WI_MT_NAMD_ACNT_BKGS_VEC_SAV',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.188205+00:00'
    }
) }}

WITH 

source_wi_mt_namd_acnt_bkgs_vec_sav AS (
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
        enrchmt_acct_type
    FROM {{ source('raw', 'wi_mt_namd_acnt_bkgs_vec_sav') }}
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
        enrchmt_acct_type
    FROM source_wi_mt_namd_acnt_bkgs_vec_sav
)

SELECT * FROM final