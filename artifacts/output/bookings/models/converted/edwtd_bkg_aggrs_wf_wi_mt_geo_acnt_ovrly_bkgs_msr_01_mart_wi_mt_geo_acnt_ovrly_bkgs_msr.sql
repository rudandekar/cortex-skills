{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_mt_geo_acnt_ovrly_bkgs_msr', 'batch', 'edwtd_bkg_aggrs'],
    meta={
        'source_workflow': 'wf_m_WI_MT_GEO_ACNT_OVRLY_BKGS_MSR',
        'target_table': 'WI_MT_GEO_ACNT_OVRLY_BKGS_MSR',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.031187+00:00'
    }
) }}

WITH 

source_wi_mt_geo_acnt_ovrly_bkgs_msr AS (
    SELECT
        overlay_bookings_measure_key,
        ovrly_bkgs_msr_trans_type_code,
        overlay_bookings_process_date,
        bk_sa_member_id_int,
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
    FROM {{ source('raw', 'wi_mt_geo_acnt_ovrly_bkgs_msr') }}
),

final AS (
    SELECT
        overlay_bookings_measure_key,
        ovrly_bkgs_msr_trans_type_code,
        overlay_bookings_process_date,
        bk_sa_member_id_int,
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
    FROM source_wi_mt_geo_acnt_ovrly_bkgs_msr
)

SELECT * FROM final