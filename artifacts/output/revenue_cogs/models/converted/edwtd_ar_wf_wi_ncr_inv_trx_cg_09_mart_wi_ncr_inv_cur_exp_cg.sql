{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_ncr_inv_trx_cg', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_WI_NCR_INV_TRX_CG',
        'target_table': 'WI_NCR_INV_CUR_EXP_CG',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.350042+00:00'
    }
) }}

WITH 

source_wi_ncr_inv_val_inv_cg AS (
    SELECT
        ar_trx_line_key,
        gl_date,
        general_ledger_account_key
    FROM {{ source('raw', 'wi_ncr_inv_val_inv_cg') }}
),

source_wi_ncr_inv_val_inv_cdc AS (
    SELECT
        ar_trx_line_key,
        gl_date,
        sk_code_combination_id_int,
        general_ledger_account_key
    FROM {{ source('raw', 'wi_ncr_inv_val_inv_cdc') }}
),

source_wi_ncr_inv_cur_exp_cg AS (
    SELECT
        bk_ar_trx_line_gl_distrib_key,
        ss_code,
        gl_date,
        ar_trx_key,
        ru_bk_adjd_ar_trx_key,
        ar_trx_line_key,
        bk_adjd_ar_trx_line_key,
        ar_trx_line_reason_code,
        bk_company_code,
        bk_set_of_books_key,
        bk_ar_trx_type_code,
        sk_customer_trx_line_id_lint,
        sk_customer_trx_id_lint,
        general_ledger_account_key,
        bk_trxl_currency_code,
        trx_date,
        fiscal_id,
        manual_transaction_flg,
        gl_posted_date,
        functional_currency_code,
        account_class,
        acctd_amount,
        amount,
        bk_financial_account_code,
        sk_code_combination_id_int,
        cust_trx_line_gl_dist_id,
        par_customer_trx_line_id_lint
    FROM {{ source('raw', 'wi_ncr_inv_cur_exp_cg') }}
),

source_wi_ncr_inv_acct_rule_cg AS (
    SELECT
        bk_ar_trx_line_gl_distrib_key,
        ar_trx_line_key,
        accounting_rule_name,
        accounting_rule_start_date
    FROM {{ source('raw', 'wi_ncr_inv_acct_rule_cg') }}
),

source_wi_ncr_inv_cur_cg AS (
    SELECT
        bk_ar_trx_line_gl_distrib_key,
        ar_trx_key,
        ar_trx_line_key,
        ar_trx_type_short_code,
        ru_bk_adjd_ar_trx_key,
        bk_adjd_ar_trx_line_key,
        ru_bk_rule_account_rule_name,
        ru_accounting_rule_start_date,
        bk_batch_source_name,
        ar_trx_line_reason_code,
        gl_date,
        ss_code,
        context,
        item_key,
        general_ledger_account_key,
        amount,
        acctd_amount,
        fiscal_id,
        org_id,
        manual_transaction_flg,
        bk_set_of_books_key,
        currency_code,
        gl_posted_date,
        functional_currency_code,
        account_class,
        bk_financial_account_code,
        sk_code_combination_id_int,
        cust_trx_line_gl_dist_id
    FROM {{ source('raw', 'wi_ncr_inv_cur_cg') }}
),

source_wi_ncr_inv_sls_cre_flg_cg AS (
    SELECT
        bk_ar_trx_line_gl_distrib_key,
        ar_trx_line_key,
        ar_trx_type_short_code,
        context,
        source_type,
        line_id,
        metrix_flag,
        sales_credit_flag
    FROM {{ source('raw', 'wi_ncr_inv_sls_cre_flg_cg') }}
),

source_wi_ncr_inv_get_lines_cg AS (
    SELECT
        bk_ar_trx_line_gl_distrib_key,
        ar_trx_line_key,
        line_id,
        order_number
    FROM {{ source('raw', 'wi_ncr_inv_get_lines_cg') }}
),

source_wi_ncr_inv_trx_cg AS (
    SELECT
        account_class,
        account_code,
        accounting_rule_id,
        accounting_rule_name,
        acctd_amount,
        action_code,
        adjustment_id,
        adjustment_number,
        adjustment_type,
        amount,
        batch_id,
        bill_to_customer_id,
        bill_to_site_use_id,
        code_combination_id,
        cogs_percent,
        comments,
        context,
        create_datetime,
        created_by,
        creation_date,
        cust_trx_line_gl_dist_id,
        customer_trx_id,
        customer_trx_line_id,
        default_sc_flag,
        extended_amount,
        extract_type,
        fiscal_id,
        forward_reverse_flag,
        func_currency_code,
        ges_update_date,
        gl_date,
        gl_posted_date,
        global_name,
        grouping_id,
        inventory_item_id,
        invoice_currency_code,
        invoice_percent,
        invoicing_rule_id,
        last_update_date,
        last_updated_by,
        latest_record_flag,
        line_percent,
        line_seq_id,
        line_type,
        link_to_cust_trx_line_id,
        order_header_id,
        order_line_id,
        order_number,
        org_id,
        previous_customer_trx_id,
        previous_customer_trx_line_id,
        quota_flag,
        reason_code,
        rebate_amount,
        rebate_percentage_id,
        request_id,
        rule_start_date,
        sales_credit_type_id,
        salesrep_id,
        ship_to_customer_id,
        ship_to_site_use_id,
        sold_to_customer_id,
        source_type,
        split_percent,
        territory_id,
        transaction_date,
        transaction_grouping_type,
        transaction_quantity,
        trx_date,
        trx_name,
        trx_number,
        trx_type,
        unit_selling_price,
        unit_standard_price,
        dv_transaction_source_cd,
        dv_transaction_key,
        ar_batch_source_key
    FROM {{ source('raw', 'wi_ncr_inv_trx_cg') }}
),

source_ex_ncr_inv_cur_exp_cg AS (
    SELECT
        bk_ar_trx_line_gl_distrib_key,
        ar_trx_key,
        ar_trx_line_key,
        ss_code,
        ar_posting_control_key,
        general_ledger_account_key,
        account_set_flag,
        account_class_code,
        gl_datetime,
        sk_cst_trx_lin_gl_dist_id_lint
    FROM {{ source('raw', 'ex_ncr_inv_cur_exp_cg') }}
),

source_n_ar_posting_control AS (
    SELECT
        ar_posting_control_key,
        gl_posted_dt,
        posting_control_dtm,
        sk_gl_posting_control_id_int,
        ss_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'n_ar_posting_control') }}
),

source_n_source_system_codes AS (
    SELECT
        source_system_code,
        source_system_name,
        database_name,
        company,
        edw_create_date,
        edw_create_user,
        edw_update_date,
        edw_update_user,
        global_name,
        gmt_offset
    FROM {{ source('raw', 'n_source_system_codes') }}
),

source_n_ar_trx AS (
    SELECT
        ar_trx_key,
        bk_ar_trx_number,
        bk_company_code,
        bk_set_of_books_key,
        bk_ar_trx_type_code,
        ar_trx_class_type,
        ar_trx_datetime,
        ar_trx_reason_code,
        ar_trx_complete_flag,
        bk_trxl_currency_code,
        ru_cust_purchase_order_number,
        ru_ar_cmdm_adjustment_type,
        ship_to_customer_key,
        bill_to_customer_key,
        sold_to_customer_key,
        ru_bk_adjd_ar_trx_key,
        sk_customer_trx_id_lint,
        ss_code,
        edw_create_user,
        edw_update_user,
        edw_create_datetime,
        edw_update_datetime
    FROM {{ source('raw', 'n_ar_trx') }}
),

final AS (
    SELECT
        bk_ar_trx_line_gl_distrib_key,
        ss_code,
        gl_date,
        ar_trx_key,
        ru_bk_adjd_ar_trx_key,
        ar_trx_line_key,
        bk_adjd_ar_trx_line_key,
        ar_trx_line_reason_code,
        bk_company_code,
        bk_set_of_books_key,
        bk_ar_trx_type_code,
        sk_customer_trx_line_id_lint,
        sk_customer_trx_id_lint,
        general_ledger_account_key,
        bk_trxl_currency_code,
        trx_date,
        fiscal_id,
        manual_transaction_flg,
        gl_posted_date,
        functional_currency_code,
        account_class,
        acctd_amount,
        amount,
        bk_financial_account_code,
        sk_code_combination_id_int,
        cust_trx_line_gl_dist_id,
        par_customer_trx_line_id_lint
    FROM source_n_ar_trx
)

SELECT * FROM final