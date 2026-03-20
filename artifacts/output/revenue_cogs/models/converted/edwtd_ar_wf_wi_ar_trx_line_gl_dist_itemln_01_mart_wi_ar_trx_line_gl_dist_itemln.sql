{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_ar_trx_line_gl_dist_itemln', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_WI_AR_TRX_LINE_GL_DIST_ITEMLN',
        'target_table': 'WI_AR_TRX_LINE_GL_DIST_ITEMLN',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.668480+00:00'
    }
) }}

WITH 

source_wi_ar_trx_line_gl_dist_itemln AS (
    SELECT
        bk_ar_trx_line_gl_distrib_key,
        account_class_code,
        account_set_flag,
        ar_trx_key,
        ar_trx_line_key,
        general_ledger_account_key,
        gl_datetime,
        transaction_line_percentage,
        ru_ar_trx_line_func_debit_amt,
        ru_ar_trx_line_trxl_debit_amt,
        ru_ar_trx_line_trxl_credit_amt,
        ru_ar_trx_line_func_credit_amt,
        bk_je_ln_company_code,
        bk_je_ln_journal_entry_num_int,
        bk_je_ln_je_line_number_int,
        bk_je_ln_set_of_books_key,
        bk_fiscal_calendar_code,
        bk_fiscal_year_number_int,
        bk_fiscal_month_number_int,
        credit_debit_type,
        cogs_pct,
        sk_cst_trx_lin_gl_dist_id_lint,
        ss_code,
        edw_create_datetime,
        edw_create_user,
        edw_update_datetime,
        edw_update_user,
        ar_posting_control_key,
        gl_posted_dt,
        parent_ar_trx_line_key,
        customer_trx_line_id_lint,
        par_customer_trx_line_id_lint,
        offer_attributed_flg,
        dv_sales_order_line_key,
        offer_attribution_key,
        dv_product_key
    FROM {{ source('raw', 'wi_ar_trx_line_gl_dist_itemln') }}
),

final AS (
    SELECT
        bk_ar_trx_line_gl_distrib_key,
        account_class_code,
        account_set_flag,
        ar_trx_key,
        ar_trx_line_key,
        general_ledger_account_key,
        gl_datetime,
        transaction_line_percentage,
        ru_ar_trx_line_func_debit_amt,
        ru_ar_trx_line_trxl_debit_amt,
        ru_ar_trx_line_trxl_credit_amt,
        ru_ar_trx_line_func_credit_amt,
        bk_je_ln_company_code,
        bk_je_ln_journal_entry_num_int,
        bk_je_ln_je_line_number_int,
        bk_je_ln_set_of_books_key,
        bk_fiscal_calendar_code,
        bk_fiscal_year_number_int,
        bk_fiscal_month_number_int,
        credit_debit_type,
        cogs_pct,
        sk_cst_trx_lin_gl_dist_id_lint,
        ss_code,
        edw_create_datetime,
        edw_create_user,
        edw_update_datetime,
        edw_update_user,
        ar_posting_control_key,
        gl_posted_dt,
        parent_ar_trx_line_key,
        customer_trx_line_id_lint,
        par_customer_trx_line_id_lint,
        offer_attributed_flg,
        dv_sales_order_line_key,
        offer_attribution_key,
        dv_product_key
    FROM source_wi_ar_trx_line_gl_dist_itemln
)

SELECT * FROM final