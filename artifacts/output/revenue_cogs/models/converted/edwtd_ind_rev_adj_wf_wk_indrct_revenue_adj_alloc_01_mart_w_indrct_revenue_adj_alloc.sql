{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wk_indrct_revenue_adj_alloc', 'batch', 'edwtd_ind_rev_adj'],
    meta={
        'source_workflow': 'wf_m_WK_INDRCT_REVENUE_ADJ_ALLOC',
        'target_table': 'W_INDRCT_REVENUE_ADJ_ALLOC',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.292998+00:00'
    }
) }}

WITH 

source_wi_indrct_rev_cogs_adj_alloc AS (
    SELECT
        indirect_adjustment_key,
        start_ssp_dt,
        end_ssp_dt,
        ss_cd,
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
        revenue_allocation_usd_amt,
        cogs_allocation_usd_amt,
        dd_process_dt
    FROM {{ source('raw', 'wi_indrct_rev_cogs_adj_alloc') }}
),

final AS (
    SELECT
        indirect_adjustment_key,
        start_ssp_dt,
        end_ssp_dt,
        revenue_allocation_usd_amt,
        fiscal_year_number_int,
        fiscal_month_number_int,
        fiscal_calendar_cd,
        adjustment_type_cd,
        bk_revenue_or_cogs_type_cd,
        dd_process_dt,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm,
        bk_deal_id,
        action_code,
        dml_type
    FROM source_wi_indrct_rev_cogs_adj_alloc
)

SELECT * FROM final