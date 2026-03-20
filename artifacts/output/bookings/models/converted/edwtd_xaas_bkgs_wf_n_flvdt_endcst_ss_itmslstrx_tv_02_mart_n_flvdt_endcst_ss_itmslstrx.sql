{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_flvdt_endcst_ss_itmslstrx_tv', 'batch', 'edwtd_xaas_bkgs'],
    meta={
        'source_workflow': 'wf_m_N_FLVDT_ENDCST_SS_ITMSLSTRX_TV',
        'target_table': 'N_FLVDT_ENDCST_SS_ITMSLSTRX',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.278984+00:00'
    }
) }}

WITH 

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
        end_tv_dt,
        ucrm_case_num
    FROM {{ source('raw', 'n_flvdt_endcst_ss_itmslstrx_tv') }}
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
        cust_party_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        ucrm_case_num
    FROM source_n_flvdt_endcst_ss_itmslstrx_tv
)

SELECT * FROM final