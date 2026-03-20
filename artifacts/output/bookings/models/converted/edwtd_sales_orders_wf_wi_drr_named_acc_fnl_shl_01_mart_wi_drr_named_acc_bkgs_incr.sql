{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_drr_named_acc_fnl_shl', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_WI_DRR_NAMED_ACC_FNL_SHL',
        'target_table': 'WI_DRR_NAMED_ACC_BKGS_INCR',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.750409+00:00'
    }
) }}

WITH 

source_wi_drr_sysview_party AS (
    SELECT
        customer_party_key,
        bk_sa_member_id_int,
        sales_account_group_party_key,
        link_customer_party_key,
        sls_acct_group_sales_terr_key,
        sales_rep_num,
        l6_sales_territory_name_code,
        l5_sales_territory_name_code,
        iso_country_code,
        sales_credit_split_pct,
        sales_account_group_type_cd,
        hibernation_flg,
        process_type
    FROM {{ source('raw', 'wi_drr_sysview_party') }}
),

source_wi_drr_named_account_shl AS (
    SELECT
        bookings_measure_key,
        bookings_process_date,
        bkgs_measure_trans_type_code,
        dv_end_cust_party_key,
        sales_account_group_party_key,
        sales_credit_split_pct,
        bk_sa_member_id_int,
        link_customer_party_key,
        reason_desc_end_cust,
        reason_descr,
        sls_acct_group_sales_terr_key,
        sales_rep_num,
        ep_otm_territory_id_int,
        fin_adj_key,
        process_type
    FROM {{ source('raw', 'wi_drr_named_account_shl') }}
),

source_wi_drr_named_acc_sysparty AS (
    SELECT
        bookings_measure_key,
        bookings_process_date,
        bkgs_measure_trans_type_code,
        dv_end_cust_party_key,
        reason_desc_end_cust,
        sales_account_group_party_key,
        sales_credit_split_pct,
        bk_sa_member_id_int,
        link_customer_party_key,
        sls_acct_group_sales_terr_key,
        sales_rep_num,
        l5_sales_territory_name_code,
        l6_sales_territory_name_code,
        iso_country_code,
        ep_otm_territory_id_int,
        fin_adj_key,
        process_type
    FROM {{ source('raw', 'wi_drr_named_acc_sysparty') }}
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

source_wi_drr_bookings_incrmental AS (
    SELECT
        bookings_measure_key,
        bookings_process_date,
        bkgs_measure_trans_type_code,
        sales_territory_key,
        sales_rep_number,
        end_customer_key,
        ship_to_customer_key,
        sold_to_customer_key,
        dv_deal_id,
        wips_originator_id_int,
        l1_sales_territory_name_code,
        l5_sales_territory_name_code,
        l6_sales_territory_name_code,
        iso_country_code,
        field_vldtd_end_cust_prty_key,
        sales_coverage_code,
        sales_territory_type_code,
        sales_account_group_party_key,
        no_vld_sls_acct_pty_rsn_name,
        fin_adj_key,
        process_type,
        end_customer_process_flag,
        edw_update_datetime,
        bill_to_customer_key,
        party_reason_descr
    FROM {{ source('raw', 'wi_drr_bookings_incrmental') }}
),

final AS (
    SELECT
        bookings_measure_key,
        bookings_process_date,
        bkgs_measure_trans_type_code,
        dv_end_cust_party_key,
        sales_territory_key,
        sales_rep_num,
        l6_sales_territory_name_code,
        l5_sales_territory_name_code,
        iso_country_code,
        fin_adj_key,
        process_type
    FROM source_wi_drr_bookings_incrmental
)

SELECT * FROM final