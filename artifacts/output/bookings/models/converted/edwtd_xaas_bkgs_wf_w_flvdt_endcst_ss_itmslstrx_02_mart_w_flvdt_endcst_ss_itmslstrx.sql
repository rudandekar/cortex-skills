{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_flvdt_endcst_ss_itmslstrx', 'batch', 'edwtd_xaas_bkgs'],
    meta={
        'source_workflow': 'wf_m_W_FLVDT_ENDCST_SS_ITMSLSTRX',
        'target_table': 'W_FLVDT_ENDCST_SS_ITMSLSTRX',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.639916+00:00'
    }
) }}

WITH 

source_ex_otm_trx_prts_ext_mnl_xaas AS (
    SELECT
        action_cd,
        assignment_code,
        assignment_source_system,
        batch_id,
        create_datetime,
        creation_date,
        enrichment_code,
        enrichment_party_type,
        expiration_date,
        hz_cr_party_id,
        last_update_date,
        order_line_id,
        reason_code,
        sav_hq_remote_flag,
        sav_id,
        sav_member_id,
        sav_member_party_id,
        sav_owned_flag,
        split_percent,
        start_date,
        status_code,
        trx_orig_code,
        trx_party_ext_id,
        trx_source_type,
        user_name,
        validation_date,
        exception_type,
        ucrm_case_num
    FROM {{ source('raw', 'ex_otm_trx_prts_ext_mnl_xaas') }}
),

source_st_otm_trx_prts_ext_mnl_trx_xaas AS (
    SELECT
        action_cd,
        assignment_code,
        assignment_source_system,
        batch_id,
        create_datetime,
        creation_date,
        enrichment_code,
        enrichment_party_type,
        expiration_date,
        hz_cr_party_id,
        last_update_date,
        order_line_id,
        reason_code,
        sav_hq_remote_flag,
        sav_id,
        sav_member_id,
        sav_member_party_id,
        sav_owned_flag,
        split_percent,
        start_date,
        status_code,
        trx_orig_code,
        trx_party_ext_id,
        trx_source_type,
        user_name,
        validation_date,
        ucrm_case_num
    FROM {{ source('raw', 'st_otm_trx_prts_ext_mnl_trx_xaas') }}
),

source_w_flvdt_endcst_ss_itmslstrx AS (
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
        end_tv_dt,
        ucrm_case_num,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_flvdt_endcst_ss_itmslstrx') }}
),

final AS (
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
        end_tv_dt,
        ucrm_case_num,
        action_code,
        dml_type
    FROM source_w_flvdt_endcst_ss_itmslstrx
)

SELECT * FROM final