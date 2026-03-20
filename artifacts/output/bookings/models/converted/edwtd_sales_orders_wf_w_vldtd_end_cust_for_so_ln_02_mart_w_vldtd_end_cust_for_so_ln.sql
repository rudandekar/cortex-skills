{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_vldtd_end_cust_for_so_ln', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_W_VLDTD_END_CUST_FOR_SO_LN',
        'target_table': 'W_VLDTD_END_CUST_FOR_SO_LN',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.044616+00:00'
    }
) }}

WITH 

source_st_otm_trx_prts_ext_sol AS (
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
        global_name,
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
        trx_party_ext_id,
        user_name,
        validation_date,
        ucrm_case_num
    FROM {{ source('raw', 'st_otm_trx_prts_ext_sol') }}
),

source_ex_otm_trx_prts_ext_sol AS (
    SELECT
        action_cd,
        assignment_code,
        assignment_source_system,
        batch_id,
        create_datetime,
        creation_date,
        enrichment_code,
        enrichment_party_type,
        exception_type,
        expiration_date,
        global_name,
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
        trx_party_ext_id,
        user_name,
        validation_date,
        ucrm_case_num
    FROM {{ source('raw', 'ex_otm_trx_prts_ext_sol') }}
),

final AS (
    SELECT
        sales_order_line_key,
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
        edw_update_user,
        ucrm_case_num,
        action_code,
        dml_type
    FROM source_ex_otm_trx_prts_ext_sol
)

SELECT * FROM final