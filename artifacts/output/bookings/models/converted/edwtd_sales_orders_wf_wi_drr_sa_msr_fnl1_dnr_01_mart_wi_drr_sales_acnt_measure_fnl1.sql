{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_drr_sa_msr_fnl1_dnr', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_WI_DRR_SA_MSR_FNL1_DNR',
        'target_table': 'WI_DRR_SALES_ACNT_MEASURE_FNL1',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.827466+00:00'
    }
) }}

WITH 

source_wi_drr_sales_acnt_measure_fnl1 AS (
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
        process_type,
        original_sales_territory_key,
        original_sales_rep_num,
        node_enrich_reason,
        dv_sav_rstmt_reason_cd
    FROM {{ source('raw', 'wi_drr_sales_acnt_measure_fnl1') }}
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
        ep_otm_territory_id_int,
        fin_adj_key,
        process_type,
        original_sales_territory_key,
        original_sales_rep_num,
        dv_sales_rep_rstmt_reasn_cd,
        node_enrich_reason,
        dv_sav_rstmt_reason_cd
    FROM source_wi_drr_sales_acnt_measure_fnl1
)

SELECT * FROM final