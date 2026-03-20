{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_mt_sav_enrch_ovly_bkgs_measure', 'batch', 'edwtd_bkg_aggrs'],
    meta={
        'source_workflow': 'wf_m_MT_SAV_ENRCH_OVLY_BKGS_MEASURE',
        'target_table': 'MT_SAV_ENRCH_OVLY_BKGS_MEASURE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.354824+00:00'
    }
) }}

WITH 

source_mt_sav_enrch_ovly_bkgs_measure AS (
    SELECT
        overlay_bookings_measure_key,
        overlay_bookings_process_date,
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
        dv_end_cust_prty_key,
        enrchmt_acct_type,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'mt_sav_enrch_ovly_bkgs_measure') }}
),

final AS (
    SELECT
        overlay_bookings_measure_key,
        overlay_bookings_process_date,
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
        dv_end_cust_prty_key,
        enrchmt_acct_type,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_mt_sav_enrch_ovly_bkgs_measure
)

SELECT * FROM final