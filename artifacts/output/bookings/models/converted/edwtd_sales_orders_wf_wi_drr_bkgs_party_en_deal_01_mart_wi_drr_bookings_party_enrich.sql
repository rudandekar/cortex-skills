{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_drr_bkgs_party_en_deal', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_WI_DRR_BKGS_PARTY_EN_DEAL',
        'target_table': 'WI_DRR_BOOKINGS_PARTY_ENRICH',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.548401+00:00'
    }
) }}

WITH 

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
        dv_end_cust_party_key,
        reason_desc,
        fin_adj_key,
        process_type,
        rule_process_flag,
        bill_to_customer_key
    FROM source_wi_drr_bookings_incrmental
)

SELECT * FROM final