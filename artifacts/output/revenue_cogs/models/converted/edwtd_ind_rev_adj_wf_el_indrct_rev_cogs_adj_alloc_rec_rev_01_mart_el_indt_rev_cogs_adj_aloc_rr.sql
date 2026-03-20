{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_indrct_rev_cogs_adj_alloc_rec_rev', 'batch', 'edwtd_ind_rev_adj'],
    meta={
        'source_workflow': 'wf_m_EL_INDRCT_REV_COGS_ADJ_ALLOC_REC_REV',
        'target_table': 'EL_INDT_REV_COGS_ADJ_ALOC_RR',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.516812+00:00'
    }
) }}

WITH 

source_el_indt_rev_cogs_adj_aloc_rr AS (
    SELECT
        indirect_adjustment_rec_key,
        bk_fiscal_year_number_int,
        bk_fiscal_month_number_int,
        bk_fiscal_calendar_cd,
        bk_product_key,
        bk_sales_territory_key,
        bk_company_cd,
        bk_set_of_books_key,
        bk_bill_to_erp_cust_acct_key,
        bk_ship_to_erp_cust_acct_key,
        bk_sold_to_cust_acct_key,
        bk_financial_account_cd,
        bk_financial_acct_locality_int,
        bk_subaccount_locality_int,
        bk_subaccount_cd,
        bk_project_locality_int,
        bk_project_cd,
        bk_department_cd,
        bk_financial_location_cd,
        bk_adjustment_type_cd,
        bk_revenue_or_cogs_type,
        bk_deal_id,
        revenue_classification,
        recurring_flag,
        dv_rev_class_rule_name,
        net_price_flg
    FROM {{ source('raw', 'el_indt_rev_cogs_adj_aloc_rr') }}
),

final AS (
    SELECT
        indirect_adjustment_rec_key,
        bk_fiscal_year_number_int,
        bk_fiscal_month_number_int,
        bk_fiscal_calendar_cd,
        bk_product_key,
        bk_sales_territory_key,
        bk_company_cd,
        bk_set_of_books_key,
        bk_bill_to_erp_cust_acct_key,
        bk_ship_to_erp_cust_acct_key,
        bk_sold_to_cust_acct_key,
        bk_financial_account_cd,
        bk_financial_acct_locality_int,
        bk_subaccount_locality_int,
        bk_subaccount_cd,
        bk_project_locality_int,
        bk_project_cd,
        bk_department_cd,
        bk_financial_location_cd,
        bk_adjustment_type_cd,
        bk_revenue_or_cogs_type,
        bk_deal_id,
        revenue_classification,
        recurring_flag,
        dv_rev_class_rule_name,
        net_price_flg
    FROM source_el_indt_rev_cogs_adj_aloc_rr
)

SELECT * FROM final