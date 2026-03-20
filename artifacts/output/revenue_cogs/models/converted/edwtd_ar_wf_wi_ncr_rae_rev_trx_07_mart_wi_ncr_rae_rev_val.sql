{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_ncr_rae_rev_trx', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_WI_NCR_RAE_REV_TRX',
        'target_table': 'WI_NCR_RAE_REV_VAL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.550749+00:00'
    }
) }}

WITH 

source_n_ar_batch_source AS (
    SELECT
        ar_batch_source_key,
        bk_batch_source_name,
        bk_operating_unit_name_cd,
        ar_batch_source_descr,
        batch_source_type_cd,
        ar_batch_source_start_dtm,
        ar_batch_source_end_dtm,
        ar_batch_source_status_cd,
        ss_cd,
        sk_org_id_int,
        sk_batch_source_id_int,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm
    FROM {{ source('raw', 'n_ar_batch_source') }}
),

source_ex_ncr_rae_rev_cur AS (
    SELECT
        net_revenue_id,
        schedule_line_id,
        gl_date,
        gl_posted_date,
        amount,
        acctd_amount,
        code_combination_id,
        customer_trx_line_id,
        ncr_extracted_flag,
        org_id,
        attribute2,
        global_name,
        edw_create_dtm,
        edw_update_dtm
    FROM {{ source('raw', 'ex_ncr_rae_rev_cur') }}
),

source_wi_ncr_rae_rev_cur AS (
    SELECT
        net_revenue_id,
        source_system_code,
        bk_ar_trx_number,
        ar_trx_datetime,
        sk_customer_trx_id_lint,
        context,
        ru_bk_adjd_ar_trx_key,
        ar_trx_type_short_code,
        bk_ar_trx_type_code,
        ship_to_customer_key,
        bill_to_customer_key,
        sold_to_customer_key,
        bk_trxl_currency_code,
        bk_operating_unit_name_code,
        functional_currency_code,
        manual_transaction_flg,
        bk_adjd_ar_trx_line_key,
        sk_customer_trx_line_id_lint,
        item_key,
        ar_trx_line_reason_code,
        ru_bk_rule_account_rule_name,
        ru_accounting_rule_start_date,
        bk_batch_source_name,
        gl_date,
        bk_ar_trx_line_type,
        rev_pct,
        gl_posted_date,
        general_ledger_account_key,
        schedule_line_id,
        amount,
        acctd_amount,
        ar_trx_line_key,
        ar_trx_key,
        code_combination_id,
        fiscal_id,
        org_id,
        invoice_percent
    FROM {{ source('raw', 'wi_ncr_rae_rev_cur') }}
),

source_el_ar_month_process_date AS (
    SELECT
        fiscal_id,
        start_date,
        end_date,
        sc_last_process_date
    FROM {{ source('raw', 'el_ar_month_process_date') }}
),

source_el_om_xxcfi_rae_net_revenue AS (
    SELECT
        net_revenue_id,
        schedule_line_id,
        gl_date,
        gl_posted_date,
        amount,
        acctd_amount,
        code_combination_id,
        customer_trx_line_id,
        ncr_extracted_flag,
        org_id,
        attribute2,
        global_name,
        edw_create_dtm,
        edw_update_dtm
    FROM {{ source('raw', 'el_om_xxcfi_rae_net_revenue') }}
),

source_n_ar_trx_item_line AS (
    SELECT
        ar_trx_line_key,
        product_key,
        ru_accounting_rule_role,
        ru_unit_of_measure_code,
        ru_unit_std_price_local_amt,
        ru_accounting_rule_start_date,
        ru_debit_quantity,
        ru_debit_unit_amount,
        ru_debit_extended_amount,
        ru_credit_quantity,
        ru_credit_unit_amount,
        ru_credit_extended_amount,
        ru_bk_rule_account_rule_name,
        ar_trx_item_line_cr_dr_type,
        money_only_type,
        sk_customer_trx_line_id_lint,
        ss_code,
        edw_create_user,
        edw_update_user,
        edw_create_datetime,
        edw_update_datetime,
        revenue_source_cd,
        service_contract_note_txt,
        ru_acctg_rule_duration_mth_cnt
    FROM {{ source('raw', 'n_ar_trx_item_line') }}
),

source_n_ar_trx_line AS (
    SELECT
        ar_trx_line_key,
        ar_trx_key,
        bk_adjd_ar_trx_line_key,
        bk_company_code,
        bk_set_of_books_key,
        bk_ar_trx_number,
        bk_ar_trx_type_code,
        bk_ar_trx_line_type,
        bk_ar_trx_line_number_int,
        ar_trx_line_reason_code,
        bk_saf_set_of_books_key,
        bk_saf_company_code,
        bk_saf_id_int,
        applied_transaction_type,
        ar_trx_adjustment_line_role,
        ar_trx_line_creation_datetime,
        sk_customer_trx_line_id_lint,
        ss_code,
        edw_create_user,
        edw_update_user,
        edw_create_datetime,
        edw_update_datetime
    FROM {{ source('raw', 'n_ar_trx_line') }}
),

source_el_om_ra_cust_trx_lines AS (
    SELECT
        customer_trx_line_id,
        customer_trx_id,
        interface_line_context,
        interface_line_attribute14,
        interface_line_attribute1,
        interface_line_attribute6,
        org_id,
        line_number,
        global_name
    FROM {{ source('raw', 'el_om_ra_cust_trx_lines') }}
),

source_n_general_ledger_account AS (
    SELECT
        general_ledger_account_key,
        bk_company_code,
        bk_department_code,
        bk_fin_acct_locality_int,
        bk_financial_account_code,
        bk_financial_location_code,
        bk_project_code,
        bk_project_locality_int,
        bk_subaccount_code,
        bk_subaccount_locality_int,
        gl_account_enabled_flag,
        gl_account_end_date,
        gl_account_start_date,
        gl_account_type_code,
        set_of_books_key,
        sk_code_combination_id_int,
        ss_code,
        edw_create_datetime,
        edw_update_datetime,
        edw_create_user,
        edw_update_user
    FROM {{ source('raw', 'n_general_ledger_account') }}
),

source_n_ar_accounting_rule AS (
    SELECT
        bk_accounting_rule_name,
        ar_accounting_rule_description,
        sk_rule_id_int,
        ss_code,
        edw_create_user,
        edw_update_user,
        edw_create_datetime,
        edw_update_datetime
    FROM {{ source('raw', 'n_ar_accounting_rule') }}
),

source_n_ar_trx_type AS (
    SELECT
        bk_company_code,
        bk_set_of_books_key,
        bk_ar_trx_type_code,
        ar_trx_type_short_code,
        ar_trx_type_description,
        sk_cust_trx_type_id_int,
        ss_code,
        edw_create_user,
        edw_update_user,
        edw_create_datetime,
        edw_update_datetime
    FROM {{ source('raw', 'n_ar_trx_type') }}
),

source_ev_op_unit_gaap_company AS (
    SELECT
        sk_organization_id_int,
        bk_company_code,
        set_of_books_key,
        bk_operating_unit_name_code
    FROM {{ source('raw', 'ev_op_unit_gaap_company') }}
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
        ar_trx_line_key,
        gl_date,
        general_ledger_account_key
    FROM source_n_ar_trx
)

SELECT * FROM final