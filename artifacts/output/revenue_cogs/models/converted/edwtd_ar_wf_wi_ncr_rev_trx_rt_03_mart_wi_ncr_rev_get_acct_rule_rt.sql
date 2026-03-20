{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_ncr_rev_trx_rt', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_WI_NCR_REV_TRX_RT',
        'target_table': 'WI_NCR_REV_GET_ACCT_RULE_RT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.709836+00:00'
    }
) }}

WITH 

source_ex_ncr_rev_trx_cur_rt AS (
    SELECT
        revenue_transfer_key,
        account_class_cd,
        ar_transaction_line_key,
        bk_company_cd,
        debit_credit_type,
        element_type_cd,
        general_ledger_account_key,
        gl_dt,
        gl_posted_dt,
        bk_iso_currency_cd,
        product_key,
        rte_deal_id,
        rte_source_type,
        set_of_books_key,
        ep_transaction_id_int,
        transfer_group_type_cd,
        trx_functional_curr_conv_dt,
        ru_rev_trnsfr_fnctnl_crdt_amt,
        ru_rev_trnsfr_trxnl_crdt_amt,
        ru_rev_trnsfr_fnctnl_debit_amt,
        ru_rev_trnsfr_trxnl_debit_amt,
        ru_manual_rev_transfer_type_cd,
        ru_rte_contract_num,
        ru_accounting_rule_drtn_cnt,
        ru_accounting_rule_end_dt,
        ru_accounting_rule_name,
        ru_accounting_rule_start_dt,
        ru_sales_order_line_key
    FROM {{ source('raw', 'ex_ncr_rev_trx_cur_rt') }}
),

source_wi_ncr_rev_get_acct_rule_rt AS (
    SELECT
        revenue_transfer_key,
        accounting_rule_name,
        accounting_rule_start_date
    FROM {{ source('raw', 'wi_ncr_rev_get_acct_rule_rt') }}
),

source_wi_ncr_rev_trx_cur_rt AS (
    SELECT
        revenue_transfer_key,
        account_class_cd,
        ar_trx_key,
        ar_trx_line_key,
        bk_company_cd,
        debit_credit_type,
        element_type_cd,
        general_ledger_account_key,
        gl_dt,
        gl_posted_dt,
        trxl_curr_code,
        item_key,
        rte_deal_id,
        rte_source_type,
        set_of_books_key,
        ep_transaction_id_int,
        transfer_group_type_cd,
        trx_functional_curr_conv_dt,
        acctd_amount,
        amount,
        ru_manual_rev_transfer_type_cd,
        ru_rte_contract_num,
        ru_accounting_rule_drtn_cnt,
        ru_accounting_rule_end_dt,
        ru_accounting_rule_name,
        ru_accounting_rule_start_dt,
        ru_sales_order_line_key,
        context,
        fiscal_id,
        functional_currency_code,
        org_id,
        trx_date,
        trx_number,
        sk_customer_trx_line_id_lint,
        bk_financial_account_code,
        sk_code_combination_id_int
    FROM {{ source('raw', 'wi_ncr_rev_trx_cur_rt') }}
),

source_wi_ncr_rev_get_lines_rt AS (
    SELECT
        revenue_transfer_key,
        line_id,
        order_number
    FROM {{ source('raw', 'wi_ncr_rev_get_lines_rt') }}
),

source_wi_ncr_rev_trx_rt AS (
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
        ep_transaction_id_int,
        bk_sales_credit_type_code,
        sales_rep_number,
        sales_territory_key
    FROM {{ source('raw', 'wi_ncr_rev_trx_rt') }}
),

final AS (
    SELECT
        revenue_transfer_key,
        accounting_rule_name,
        accounting_rule_start_date
    FROM source_wi_ncr_rev_trx_rt
)

SELECT * FROM final