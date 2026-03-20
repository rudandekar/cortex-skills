{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_mt_sls_acnt_rstd_msr_drr_ng', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_MT_SLS_ACNT_RSTD_MSR_DRR_NG',
        'target_table': 'WI_DRR_SALES_ACNT_MSRE_FNL_DEL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.674534+00:00'
    }
) }}

WITH 

source_wi_drr_sales_account_measure AS (
    SELECT
        bookings_measure_key,
        bookings_process_date,
        bkgs_measure_trans_type_code,
        dv_end_cust_party_key,
        new_sa_member_id_int,
        link_customer_party_key,
        sales_account_group_party_key,
        reason_desc_end_cust,
        reason_descr,
        sales_credit_split_pct,
        sls_acct_group_sales_terr_key,
        sales_rep_num,
        ep_otm_territory_id_int,
        node_enrich_reason,
        dv_sav_rstmt_reason_cd
    FROM {{ source('raw', 'wi_drr_sales_account_measure') }}
),

source_mt_sls_acnt_rstd_msr_drr_ng AS (
    SELECT
        bookings_measure_key,
        bookings_process_dt,
        bkgs_measure_trans_type_cd,
        dv_end_cust_party_key,
        bk_sa_member_id_int,
        link_customer_party_key,
        sales_account_group_party_key,
        dv_end_cust_reason_descr,
        dv_end_cust_ownership_splt_pct,
        last_updated_dtm,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        sales_rep_num,
        sales_territory_key,
        ep_otm_territory_id_int,
        process_type,
        dv_fin_adj_key,
        dv_rstmt_reason_cd,
        sk_sequence_id,
        dv_sales_rep_rstmt_reasn_cd,
        dv_sav_rstmt_reason_cd,
        dv_sales_rep_rst_rule_cd
    FROM {{ source('raw', 'mt_sls_acnt_rstd_msr_drr_ng') }}
),

source_wi_drr_sales_acnt_measure_fnl AS (
    SELECT
        bookings_measure_key,
        bookings_process_date,
        bkgs_measure_trans_type_code,
        dv_end_cust_party_key,
        new_sa_member_id_int,
        link_customer_party_key,
        sales_account_group_party_key,
        reason_desc_end_cust,
        reason_descr,
        sales_credit_split_pct,
        sls_acct_group_sales_terr_key,
        sales_rep_num,
        ep_otm_territory_id_int,
        fin_adj_key,
        process_type
    FROM {{ source('raw', 'wi_drr_sales_acnt_measure_fnl') }}
),

final AS (
    SELECT
        bookings_measure_key,
        bookings_process_date,
        bkgs_measure_trans_type_code,
        dv_end_cust_party_key,
        new_sa_member_id_int,
        link_customer_party_key,
        sales_account_group_party_key,
        reason_desc_end_cust,
        reason_descr,
        sales_credit_split_pct,
        sls_acct_group_sales_terr_key,
        sales_rep_num,
        ep_otm_territory_id_int
    FROM source_wi_drr_sales_acnt_measure_fnl
)

SELECT * FROM final