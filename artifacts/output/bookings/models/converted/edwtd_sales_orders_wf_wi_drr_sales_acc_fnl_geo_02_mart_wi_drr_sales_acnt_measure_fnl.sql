{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_drr_sales_acc_fnl_geo', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_WI_DRR_SALES_ACC_FNL_GEO',
        'target_table': 'WI_DRR_SALES_ACNT_MEASURE_FNL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.057836+00:00'
    }
) }}

WITH 

source_wi_drr_geo_account_match AS (
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
        geo_processed_flg,
        fin_adj_key,
        process_type
    FROM {{ source('raw', 'wi_drr_geo_account_match') }}
),

source_wi_drr_bookings_party_enrich AS (
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
        dv_end_cust_party_key,
        reason_desc,
        fin_adj_key,
        process_type,
        rule_process_flag
    FROM {{ source('raw', 'wi_drr_bookings_party_enrich') }}
),

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
        node_enrich_flag,
        node_enrich_reason
    FROM source_wi_drr_sysview_party
)

SELECT * FROM final