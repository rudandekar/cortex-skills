{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_mt_cm_adjustment', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_MT_CM_ADJUSTMENT',
        'target_table': 'MT_CM_ADJUSTMENT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.711086+00:00'
    }
) }}

WITH 

source_w_cm_adjustment AS (
    SELECT
        cm_adjustment_key,
        functional_currency_cd,
        bk_financial_account_cd,
        bill_to_customer_key,
        bk_ar_trx_num,
        ru_cust_purchase_order_num,
        ar_type_cd,
        dv_ar_adj_trx_functional_amt,
        dv_ar_adj_trx_usd_amt,
        gl_dt,
        gl_posted_dt,
        ar_adj_trx_reason_cd,
        vendor_name,
        operating_unit_name_cd,
        cust_trx_type_category_cd,
        fiscal_year_month_num_int,
        bk_ar_adj_number,
        bk_company_code,
        bk_set_of_books_key,
        edw_create_user,
        edw_create_datetime,
        edw_update_user,
        edw_update_datetime,
        action_code,
        dml_type,
        dv_trx_fsc_yr_mth_wk_num_int
    FROM {{ source('raw', 'w_cm_adjustment') }}
),

final AS (
    SELECT
        cm_adjustment_key,
        functional_currency_cd,
        bk_financial_account_cd,
        bill_to_customer_key,
        bk_ar_trx_num,
        ru_cust_purchase_order_num,
        ar_type_cd,
        dv_ar_adj_trx_functional_amt,
        dv_ar_adj_trx_usd_amt,
        gl_dt,
        gl_posted_dt,
        ar_adj_trx_reason_cd,
        vendor_name,
        operating_unit_name_cd,
        cust_trx_type_category_cd,
        fiscal_year_month_num_int,
        bk_company_code,
        bk_set_of_books_key,
        edw_create_user,
        edw_create_datetime,
        edw_update_user,
        edw_update_datetime,
        dv_trx_fsc_yr_mth_wk_num_int
    FROM source_w_cm_adjustment
)

SELECT * FROM final