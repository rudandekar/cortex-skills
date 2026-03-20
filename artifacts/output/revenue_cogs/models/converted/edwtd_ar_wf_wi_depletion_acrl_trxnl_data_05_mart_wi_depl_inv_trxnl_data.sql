{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_depletion_acrl_trxnl_data', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_WI_DEPLETION_ACRL_TRXNL_DATA',
        'target_table': 'WI_DEPL_INV_TRXNL_DATA',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:43.986071+00:00'
    }
) }}

WITH 

source_wi_depl_inv_trxnl_data AS (
    SELECT
        dv_fiscal_year_mth_int,
        product_key,
        dv_bundle_prdt_key,
        sales_order_line_key,
        sk_line_ref_num,
        accounting_rule_name,
        tsv_trxl_amt,
        ru_credit_trxl_amt,
        dv_invoice_dt,
        dv_bill_rel_to_invoicing_dt,
        invoicing,
        cumulative_invoicing,
        identifier
    FROM {{ source('raw', 'wi_depl_inv_trxnl_data') }}
),

source_wi_depl_accrual_invoicing_calc AS (
    SELECT
        dv_fiscal_year_mth_int,
        sales_order_line_key,
        product_key,
        dv_bundle_prdt_key,
        sk_line_ref_num,
        accounting_rule_name,
        dv_invoice_dt,
        dv_bill_rel_to_invoicing_dt,
        tsv_trxl_amt,
        ru_credit_trxl_amt,
        contra_asset,
        cumulative_contra,
        invoicing,
        cumulative_invoice,
        remaining_ca
    FROM {{ source('raw', 'wi_depl_accrual_invoicing_calc') }}
),

source_wi_depl_acrl_trxnl_data AS (
    SELECT
        dv_fiscal_year_mth_int,
        product_key,
        dv_bundle_prdt_key,
        sales_order_line_key,
        sk_line_ref_num,
        accounting_rule_name,
        tsv_trxl_amt,
        ru_credit_trxl_amt,
        contra_asset,
        cumulative_contra_dr,
        identifier
    FROM {{ source('raw', 'wi_depl_acrl_trxnl_data') }}
),

source_wi_depl_fut_inv AS (
    SELECT
        dv_fiscal_year_mth_int,
        sales_order_line_key,
        product_key,
        dv_bundle_prdt_key,
        dv_invoice_dt,
        dv_bill_rel_to_invoicing_dt,
        invoicing,
        cumulative_invoice,
        next_invoice_month
    FROM {{ source('raw', 'wi_depl_fut_inv') }}
),

source_wi_depl_acrl_inv_data_discont_mth AS (
    SELECT
        dv_fiscal_year_mth_int,
        sales_order_line_key,
        product_key,
        dv_bundle_prdt_key,
        sk_line_ref_num,
        accounting_rule_name,
        dv_invoice_dt,
        dv_bill_rel_to_invoicing_dt
    FROM {{ source('raw', 'wi_depl_acrl_inv_data_discont_mth') }}
),

source_wi_depl_fut_inv_seq AS (
    SELECT
        dv_fiscal_year_mth_int,
        sales_order_line_key,
        product_key,
        dv_bundle_prdt_key,
        dv_invoice_dt,
        dv_bill_rel_to_invoicing_dt,
        invoicing,
        cumulative_invoice,
        next_invoice_month,
        invoicing_csum,
        inv_seq_no
    FROM {{ source('raw', 'wi_depl_fut_inv_seq') }}
),

source_wi_depl_acrl_inv_trxnl_data AS (
    SELECT
        dv_fiscal_year_mth_int,
        product_key,
        dv_bundle_prdt_key,
        sales_order_line_key,
        sk_line_ref_num,
        accounting_rule_name,
        identifier,
        tsv_trxl_amt,
        ru_credit_trxl_amt,
        dv_invoice_dt,
        dv_bill_rel_to_invoicing_dt,
        contra_asset,
        cumulative_contra_dr,
        invoicing,
        cumulative_invoicing
    FROM {{ source('raw', 'wi_depl_acrl_inv_trxnl_data') }}
),

source_wi_depl_accrued_rev AS (
    SELECT
        processed_fiscal_month,
        invoice_fiscal_month,
        sales_order_line_key,
        product_key,
        dv_bundle_prdt_key,
        accounting_rule_name,
        dv_invoice_dt,
        dv_bill_rel_to_invoicing_dt,
        invoicing,
        cumulative_invoice,
        actual_remaining_ca,
        contra_asset,
        original_accrued,
        remaining_accrued,
        invoiced_accrual_release
    FROM {{ source('raw', 'wi_depl_accrued_rev') }}
),

source_wi_depl_acrl_inv_data AS (
    SELECT
        dv_fiscal_year_mth_int,
        sales_order_line_key,
        product_key,
        dv_bundle_prdt_key,
        sk_line_ref_num,
        accounting_rule_name,
        dv_invoice_dt,
        dv_bill_rel_to_invoicing_dt,
        tsv_trxl_amt,
        ru_credit_trxl_amt,
        contra_asset,
        cumulative_contra_dr,
        invoicing,
        cumulative_invoicing
    FROM {{ source('raw', 'wi_depl_acrl_inv_data') }}
),

source_wi_depl_acrl_inv_data_max_mth AS (
    SELECT
        dv_fiscal_year_mth_int,
        sales_order_line_key,
        product_key,
        dv_bundle_prdt_key,
        sk_line_ref_num,
        accounting_rule_name,
        dv_invoice_dt,
        dv_bill_rel_to_invoicing_dt
    FROM {{ source('raw', 'wi_depl_acrl_inv_data_max_mth') }}
),

source_wi_depl_acrl_seq AS (
    SELECT
        processed_fiscal_month,
        invoice_fiscal_month,
        sales_order_line_key,
        product_key,
        dv_bundle_prdt_key,
        accounting_rule_name,
        dv_invoice_dt,
        dv_bill_rel_to_invoicing_dt,
        invoicing,
        cumulative_invoice,
        actual_remaining_ca,
        contra_asset,
        original_accrued,
        remaining_accrued,
        invoiced_accrual_release,
        remaining_accrued_csum,
        rev_seq_no
    FROM {{ source('raw', 'wi_depl_acrl_seq') }}
),

final AS (
    SELECT
        dv_fiscal_year_mth_int,
        product_key,
        dv_bundle_prdt_key,
        sales_order_line_key,
        sk_line_ref_num,
        accounting_rule_name,
        tsv_trxl_amt,
        ru_credit_trxl_amt,
        dv_invoice_dt,
        dv_bill_rel_to_invoicing_dt,
        invoicing,
        cumulative_invoicing,
        identifier
    FROM source_wi_depl_acrl_seq
)

SELECT * FROM final