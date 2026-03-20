{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_future_acrl_trxnl_data', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_WI_FUTURE_ACRL_TRXNL_DATA',
        'target_table': 'WI_FBS_ACRL_INV_DATA_MAX_MTH',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.210701+00:00'
    }
) }}

WITH 

source_wi_fbs_acrl_inv_data_discont_mth AS (
    SELECT
        dv_fisc_yr_mnth_int,
        sales_order_line_key,
        product_key,
        dv_bundle_product_key,
        sk_line_reference_num,
        attribution_cd,
        bk_accounting_rule_name,
        accounting_rule_start_date,
        accounting_rule_end_date,
        credit_debit_type,
        account_class_cd,
        dv_invoice_dt,
        dv_bill_rel_to_invoicing_dt
    FROM {{ source('raw', 'wi_fbs_acrl_inv_data_discont_mth') }}
),

source_wi_fbs_month_ctrl AS (
    SELECT
        current_month,
        previous_month
    FROM {{ source('raw', 'wi_fbs_month_ctrl') }}
),

source_wi_fbs_accrual_invoicing_calc_final AS (
    SELECT
        curr_fiscal_month,
        pre_fiscal_month,
        sales_order_line_key,
        product_key,
        dv_bundle_product_key,
        sk_line_reference_num,
        attribution_cd,
        bk_accounting_rule_name,
        accounting_rule_start_date,
        accounting_rule_end_date,
        credit_debit_type,
        account_class_cd,
        dv_invoice_dt,
        dv_bill_rel_to_invoicing_dt,
        tsv_transactional_amt,
        ru_credit_transactional_amt,
        curr_fbs_accrual_increase,
        pre_fbs_accrual_increase,
        curr_cumulative_contra_dr,
        pre_cumulative_contra_dr,
        curr_invoicing,
        pre_invoicing,
        curr_cumulative_invoicing,
        pre_cumulative_invoicing,
        curr_remaining_ca,
        pre_remaining_ca,
        curr_defered_invoice_amount,
        pre_defered_invoice_amount,
        fbs_accrual_decrease,
        cumulative_accrual_decrease,
        chec,
        accrual_net
    FROM {{ source('raw', 'wi_fbs_accrual_invoicing_calc_final') }}
),

source_wi_fbs_acrl_inv_trxnl_data AS (
    SELECT
        dv_fisc_yr_mnth_int,
        product_key,
        dv_bundle_product_key,
        sales_order_line_key,
        sk_line_reference_num,
        attribution_cd,
        bk_accounting_rule_name,
        accounting_rule_start_date,
        accounting_rule_end_date,
        credit_debit_type,
        account_class_cd,
        identifier,
        tsv_transactional_amt,
        ru_credit_transactional_amt,
        dv_invoice_dt,
        dv_bill_rel_to_invoicing_dt,
        fbs_accrual_increase,
        cumulative_contra_dr,
        invoicing,
        cumulative_invoicing
    FROM {{ source('raw', 'wi_fbs_acrl_inv_trxnl_data') }}
),

source_wi_fbs_acrl_inv_data_max_mth AS (
    SELECT
        dv_fisc_yr_mnth_int,
        sales_order_line_key,
        product_key,
        dv_bundle_product_key,
        sk_line_reference_num,
        attribution_cd,
        bk_accounting_rule_name,
        accounting_rule_start_date,
        accounting_rule_end_date,
        credit_debit_type,
        account_class_cd,
        dv_invoice_dt,
        dv_bill_rel_to_invoicing_dt
    FROM {{ source('raw', 'wi_fbs_acrl_inv_data_max_mth') }}
),

source_wi_fbs_accrual_invoicing_calc AS (
    SELECT
        dv_fisc_yr_mnth_int,
        sales_order_line_key,
        product_key,
        dv_bundle_product_key,
        sk_line_reference_num,
        attribution_cd,
        bk_accounting_rule_name,
        accounting_rule_start_date,
        accounting_rule_end_date,
        credit_debit_type,
        account_class_cd,
        dv_invoice_dt,
        dv_bill_rel_to_invoicing_dt,
        tsv_transactional_amt,
        ru_credit_transactional_amt,
        fbs_accrual_increase,
        cumulative_contra,
        invoicing,
        cumulative_invoice,
        remaining_ca,
        defered_invoice_amount
    FROM {{ source('raw', 'wi_fbs_accrual_invoicing_calc') }}
),

source_wi_fbs_acrl_inv_data AS (
    SELECT
        dv_fisc_yr_mnth_int,
        sales_order_line_key,
        product_key,
        dv_bundle_product_key,
        sk_line_reference_num,
        attribution_cd,
        bk_accounting_rule_name,
        accounting_rule_start_date,
        accounting_rule_end_date,
        credit_debit_type,
        account_class_cd,
        dv_invoice_dt,
        dv_bill_rel_to_invoicing_dt,
        tsv_transactional_amt,
        ru_credit_transactional_amt,
        fbs_accrual_increase,
        cumulative_contra_dr,
        invoicing,
        cumulative_invoicing
    FROM {{ source('raw', 'wi_fbs_acrl_inv_data') }}
),

source_wi_fbs_acrl_trxnl_data AS (
    SELECT
        dv_fisc_yr_mnth_int,
        product_key,
        dv_bundle_product_key,
        sales_order_line_key,
        sk_line_reference_num,
        attribution_cd,
        bk_accounting_rule_name,
        accounting_rule_start_date,
        accounting_rule_end_date,
        credit_debit_type,
        account_class_cd,
        tsv_transactional_amt,
        ru_credit_transactional_amt,
        fbs_accrual_increase,
        cumulative_contra_dr,
        identifier
    FROM {{ source('raw', 'wi_fbs_acrl_trxnl_data') }}
),

source_wi_fbs_inv_trxnl_data AS (
    SELECT
        dv_fisc_yr_mnth_int,
        product_key,
        dv_bundle_product_key,
        sales_order_line_key,
        sk_line_reference_num,
        attribution_cd,
        bk_accounting_rule_name,
        accounting_rule_start_date,
        accounting_rule_end_date,
        credit_debit_type,
        account_class_cd,
        tsv_transactional_amt,
        ru_credit_transactional_amt,
        dv_invoice_dt,
        dv_bill_rel_to_invoicing_dt,
        invoicing,
        cumulative_invoicing,
        identifier
    FROM {{ source('raw', 'wi_fbs_inv_trxnl_data') }}
),

final AS (
    SELECT
        dv_fisc_yr_mnth_int,
        sales_order_line_key,
        product_key,
        dv_bundle_product_key,
        sk_line_reference_num,
        attribution_cd,
        bk_accounting_rule_name,
        accounting_rule_start_date,
        accounting_rule_end_date,
        credit_debit_type,
        account_class_cd,
        dv_invoice_dt,
        dv_bill_rel_to_invoicing_dt
    FROM source_wi_fbs_inv_trxnl_data
)

SELECT * FROM final