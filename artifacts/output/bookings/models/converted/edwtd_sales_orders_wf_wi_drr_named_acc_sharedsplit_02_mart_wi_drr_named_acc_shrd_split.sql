{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_drr_named_acc_sharedsplit', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_WI_DRR_NAMED_ACC_SHAREDSPLIT',
        'target_table': 'WI_DRR_NAMED_ACC_SHRD_SPLIT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.814637+00:00'
    }
) }}

WITH 

source_wi_drr_named_acc_shrd_split AS (
    SELECT
        bookings_measure_key,
        bookings_process_date,
        bkgs_measure_trans_type_code,
        dv_end_cust_party_key,
        reason_desc_end_cust,
        new_sa_member_id_int,
        link_customer_party_key,
        sales_account_group_party_key,
        reason_descr,
        sales_credit_split_pct,
        sls_acct_group_sales_terr_key,
        sales_rep_num,
        ep_otm_territory_id_int,
        fin_adj_key,
        process_type
    FROM {{ source('raw', 'wi_drr_named_acc_shrd_split') }}
),

source_wi_drr_named_account_round_off AS (
    SELECT
        bookings_measure_key,
        bookings_process_date,
        bkgs_measure_trans_type_code,
        dv_end_cust_party_key,
        reason_desc_end_cust,
        new_sa_member_id_int,
        link_customer_party_key,
        sales_account_group_party_key,
        reason_descr,
        sales_credit_split_pct,
        sls_acct_group_sales_terr_key,
        sales_rep_num,
        ep_otm_territory_id_int,
        fin_adj_key,
        process_type
    FROM {{ source('raw', 'wi_drr_named_account_round_off') }}
),

source_wi_drr_named_account AS (
    SELECT
        bookings_measure_key,
        bookings_process_date,
        bkgs_measure_trans_type_code,
        dv_end_cust_party_key,
        reason_desc_end_cust,
        new_sa_member_id_int,
        link_customer_party_key,
        iso_country_code,
        sales_account_group_party_key,
        sales_credit_split_pct,
        reason_descr,
        sls_acct_group_sales_terr_key,
        sales_rep_num,
        ep_otm_territory_id_int,
        na_processed_flg,
        fin_adj_key,
        process_type,
        country_match
    FROM {{ source('raw', 'wi_drr_named_account') }}
),

final AS (
    SELECT
        bookings_measure_key,
        bookings_process_date,
        bkgs_measure_trans_type_code,
        dv_end_cust_party_key,
        reason_desc_end_cust,
        new_sa_member_id_int,
        link_customer_party_key,
        sales_account_group_party_key,
        reason_descr,
        sales_credit_split_pct,
        sls_acct_group_sales_terr_key,
        sales_rep_num,
        ep_otm_territory_id_int,
        fin_adj_key,
        process_type
    FROM source_wi_drr_named_account
)

SELECT * FROM final