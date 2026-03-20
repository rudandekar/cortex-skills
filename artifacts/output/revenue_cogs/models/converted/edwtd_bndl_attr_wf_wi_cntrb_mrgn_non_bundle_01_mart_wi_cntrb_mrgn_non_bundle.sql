{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_cntrb_mrgn_non_bundle', 'batch', 'edwtd_bndl_attr'],
    meta={
        'source_workflow': 'wf_m_WI_CNTRB_MRGN_NON_BUNDLE',
        'target_table': 'WI_CNTRB_MRGN_NON_BUNDLE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.704244+00:00'
    }
) }}

WITH 

source_n_cntrb_mrgn_cost_cat_actl_ln AS (
    SELECT
        cntrb_mrgn_cost_actl_ln_key,
        bk_fiscal_year_number_int,
        bk_fiscal_month_number_int,
        bk_fiscal_calendar_cd,
        bk_product_key,
        bk_sales_territory_key,
        bk_subaccount_locality_int,
        bk_financial_acct_locality_int,
        bk_financial_account_cd,
        bk_subaccount_cd,
        bk_company_cd,
        bk_set_of_books_key,
        bk_department_cd,
        bk_project_locality_int,
        bk_project_cd,
        bk_financial_location_cd,
        bk_bill_to_cust_acct_key,
        bk_ship_to_cust_acct_key,
        bk_sold_to_cust_acct_key,
        bk_cntrbtn_mrgn_category_cd,
        cntrb_mrgn_actl_ln_usd_amt,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm
    FROM {{ source('raw', 'n_cntrb_mrgn_cost_cat_actl_ln') }}
),

final AS (
    SELECT
        cntrb_mrgn_cost_actl_ln_key,
        bk_fiscal_year_number_int,
        bk_fiscal_month_number_int,
        bk_product_key,
        cntrb_mrgn_actl_ln_usd_amt,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm
    FROM source_n_cntrb_mrgn_cost_cat_actl_ln
)

SELECT * FROM final