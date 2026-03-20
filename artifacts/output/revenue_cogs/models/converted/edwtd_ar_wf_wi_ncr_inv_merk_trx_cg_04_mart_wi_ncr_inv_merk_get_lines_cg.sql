{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_ncr_inv_merk_trx_cg', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_WI_NCR_INV_MERK_TRX_CG',
        'target_table': 'WI_NCR_INV_MERK_GET_LINES_CG',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.735618+00:00'
    }
) }}

WITH 

source_wi_ncr_inv_merk_trx_cg AS (
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
        bk_sales_credit_type_code,
        sales_rep_number,
        sales_territory_key,
        sk_sc_agent_id_int
    FROM {{ source('raw', 'wi_ncr_inv_merk_trx_cg') }}
),

source_wi_ncr_inv_merk_val_cg AS (
    SELECT
        meraki_rev_trx_key,
        gl_date,
        general_ledger_account_key
    FROM {{ source('raw', 'wi_ncr_inv_merk_val_cg') }}
),

source_wi_ncr_inv_merk_cur_cg AS (
    SELECT
        meraki_rev_trx_key,
        ar_trx_key,
        ar_trx_line_key,
        ar_trx_type_short_code,
        bk_accounting_rule_name,
        accounting_rule_start_dt,
        bk_batch_source_name,
        ar_trx_line_reason_code,
        gl_dt,
        ss_code,
        product_key,
        general_ledger_account_key,
        amount,
        acctd_amount,
        fiscal_id,
        org_id,
        set_of_books_key,
        bk_trx_iso_currency_cd,
        gl_posted_dt,
        functional_currency_code,
        account_class_cd,
        bk_financial_account_code,
        sk_code_combination_id_int,
        dv_product_key,
        meraki_trx_qty
    FROM {{ source('raw', 'wi_ncr_inv_merk_cur_cg') }}
),

source_wi_ncr_inv_merk_cur_exp_cg AS (
    SELECT
        meraki_rev_trx_key,
        account_class_cd,
        ar_trx_key,
        ar_trx_line_key,
        bk_company_cd,
        general_ledger_account_key,
        gl_dt,
        gl_posted_dt,
        bk_trx_iso_currency_cd,
        functional_currency_code,
        product_key,
        set_of_books_key,
        bk_ar_trx_type_code,
        acctd_amount,
        amount,
        ru_accounting_rule_end_dt,
        bk_accounting_rule_name,
        accounting_rule_start_dt,
        fiscal_id,
        org_id,
        trx_date,
        trx_number,
        sk_customer_trx_line_id_lint,
        bk_financial_account_code,
        sk_code_combination_id_int,
        ar_trx_line_reason_code,
        ar_trx_type_short_code,
        bk_batch_source_name,
        ss_code,
        dv_product_key,
        meraki_trx_qty
    FROM {{ source('raw', 'wi_ncr_inv_merk_cur_exp_cg') }}
),

source_wi_ncr_inv_sls_cre_flg_merk_cg AS (
    SELECT
        meraki_rev_trx_key,
        ar_trx_line_key,
        ar_trx_type_short_code,
        source_type,
        line_id,
        sales_credit_flag
    FROM {{ source('raw', 'wi_ncr_inv_sls_cre_flg_merk_cg') }}
),

source_wi_ncr_inv_merk_get_lines_cg AS (
    SELECT
        meraki_rev_trx_key,
        ru_ar_trx_line_key,
        line_id,
        order_number
    FROM {{ source('raw', 'wi_ncr_inv_merk_get_lines_cg') }}
),

source_ex_ncr_inv_merk_cur_exp_cg AS (
    SELECT
        meraki_rev_trx_key,
        account_class_cd,
        ar_trx_key,
        ar_trx_line_key,
        bk_company_cd,
        general_ledger_account_key,
        gl_dt,
        gl_posted_dt,
        bk_trx_iso_currency_cd,
        product_key,
        set_of_books_key,
        debit_functional_amt,
        credit_functional_amt,
        debit_trxl_amt,
        credit_trxl_amt,
        accounting_rule_end_dt,
        accounting_rule_name,
        accounting_rule_start_dt,
        ss_cd,
        dv_bundle_prdt_key,
        meraki_trx_qty
    FROM {{ source('raw', 'ex_ncr_inv_merk_cur_exp_cg') }}
),

final AS (
    SELECT
        meraki_rev_trx_key,
        ru_ar_trx_line_key,
        line_id,
        order_number
    FROM source_ex_ncr_inv_merk_cur_exp_cg
)

SELECT * FROM final