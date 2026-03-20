{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_rstd_idrt_rev_cogs_adj_alc', 'batch', 'edwtd_ind_rev_adj'],
    meta={
        'source_workflow': 'wf_m_W_RSTD_IDRT_REV_COGS_ADJ_ALC',
        'target_table': 'W_RSTD_IDRT_REV_COGS_ADJ_ALC',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.645922+00:00'
    }
) }}

WITH 

source_w_rstd_idrt_rev_cogs_adj_alc AS (
    SELECT
        rstd_idrt_rev_cogs_adj_alc_key,
        bk_fiscal_month_num_int,
        bk_fiscal_year_num_int,
        bk_restated_sales_terr_key,
        bk_fiscal_calendar_cd,
        bk_subaccount_cd,
        bk_department_cd,
        bk_adjustment_type_cd,
        bk_financial_location_cd,
        bk_product_key,
        bk_bill_erp_cst_act_lc_use_key,
        bk_subaccount_locality_int,
        bk_financial_acct_locality_int,
        bk_sold_to_cust_acct_key,
        bk_company_cd,
        bk_financial_account_cd,
        bk_set_of_books_key,
        ss_cd,
        bk_ship_erp_cst_act_lc_use_key,
        bk_project_locality_int,
        bk_project_cd,
        bk_revenue_or_cogs_type,
        bk_rstd_fiscal_year_num_int,
        bk_rstd_fiscal_month_num_int,
        bk_rstd_fiscal_calender_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        ru_rstd_rev_alloc_usd_amt,
        ru_rstd_cogs_alloc_usd_amt,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_rstd_idrt_rev_cogs_adj_alc') }}
),

final AS (
    SELECT
        rstd_idrt_rev_cogs_adj_alc_key,
        bk_fiscal_month_num_int,
        bk_fiscal_year_num_int,
        bk_restated_sales_terr_key,
        bk_fiscal_calendar_cd,
        bk_subaccount_cd,
        bk_department_cd,
        bk_adjustment_type_cd,
        bk_financial_location_cd,
        bk_product_key,
        bk_bill_erp_cst_act_lc_use_key,
        bk_subaccount_locality_int,
        bk_financial_acct_locality_int,
        bk_sold_to_cust_acct_key,
        bk_company_cd,
        bk_financial_account_cd,
        bk_set_of_books_key,
        ss_cd,
        bk_ship_erp_cst_act_lc_use_key,
        bk_project_locality_int,
        bk_project_cd,
        bk_revenue_or_cogs_type,
        bk_rstd_fiscal_year_num_int,
        bk_rstd_fiscal_month_num_int,
        bk_rstd_fiscal_calender_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        ru_rstd_rev_alloc_usd_amt,
        ru_rstd_cogs_alloc_usd_amt,
        action_code,
        dml_type
    FROM source_w_rstd_idrt_rev_cogs_adj_alc
)

SELECT * FROM final