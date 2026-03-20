{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_depletion_sbp_acrl_trxnl_data', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_WI_DEPLETION_SBP_ACRL_TRXNL_DATA',
        'target_table': 'WI_DEPL_SBP_ACRL_INV_DATA_MAX_MTH',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.952899+00:00'
    }
) }}

WITH 

source_wi_depl_sbp_inv_trxnl_data AS (
    SELECT
        dv_fiscal_year_mth_int,
        product_key,
        dv_bundle_prdt_key,
        accounting_rule_name,
        subscription_ref_id,
        subscription_id,
        tsv_trxl_amt,
        ru_credit_trxl_amt,
        dv_bill_rel_to_invoicing_dt,
        invoicing,
        cumulative_invoicing,
        identifier
    FROM {{ source('raw', 'wi_depl_sbp_inv_trxnl_data') }}
),

source_wi_sbp_future_invoice_seq AS (
    SELECT
        dv_fiscal_year_mth_int,
        product_key,
        dv_bundle_prdt_key,
        subscription_ref_id,
        subscription_id,
        dv_invoice_dt,
        dv_bill_rel_to_invoicing_dt,
        invoicing,
        cumulative_invoicing,
        next_invoice_month,
        invoicing_csum,
        inv_seq_no
    FROM {{ source('raw', 'wi_sbp_future_invoice_seq') }}
),

source_wi_depl_sbp_acrl_inv_trxnl_data AS (
    SELECT
        dv_fiscal_year_mth_int,
        product_key,
        dv_bundle_prdt_key,
        accounting_rule_name,
        subscription_ref_id,
        subscription_id,
        identifier,
        tsv_trxl_amt,
        ru_credit_trxl_amt,
        dv_invoice_dt,
        dv_bill_rel_to_invoicing_dt,
        contra_asset,
        cumulative_contra_dr,
        invoicing,
        cumulative_invoicing
    FROM {{ source('raw', 'wi_depl_sbp_acrl_inv_trxnl_data') }}
),

source_wi_sbp_accrual_depletion AS (
    SELECT
        processed_fiscal_month,
        invoice_fiscal_month,
        product_key,
        dv_bundle_prdt_key,
        accounting_rule_name,
        subscription_ref_id,
        subscription_id,
        dv_invoice_dt,
        dv_bill_rel_to_invoicing_dt,
        invoicing,
        cumulative_invoicing,
        actual_remaining_ca,
        contra_asset,
        original_accrued,
        remaining_accrued,
        invoiced_accrual_release
    FROM {{ source('raw', 'wi_sbp_accrual_depletion') }}
),

source_wi_depl_sbp_remaining_ca AS (
    SELECT
        dv_fiscal_year_mth_int,
        product_key,
        dv_bundle_prdt_key,
        accounting_rule_name,
        subscription_ref_id,
        subscription_id,
        remaining_ca_amount
    FROM {{ source('raw', 'wi_depl_sbp_remaining_ca') }}
),

source_wi_depl_sbp_acrl_inv_data_discont_mth AS (
    SELECT
        fiscal_year_month_int,
        product_key,
        dv_bundle_prdt_key,
        accounting_rule_name,
        subscription_ref_id,
        subscription_id,
        dv_invoice_dt,
        dv_bill_rel_to_invoicing_dt
    FROM {{ source('raw', 'wi_depl_sbp_acrl_inv_data_discont_mth') }}
),

source_wi_depl_sbp_acrl_inv_data_max_mth AS (
    SELECT
        dv_fiscal_year_mth_int,
        product_key,
        dv_bundle_prdt_key,
        accounting_rule_name,
        subscription_ref_id,
        subscription_id,
        dv_invoice_dt,
        dv_bill_rel_to_invoicing_dt
    FROM {{ source('raw', 'wi_depl_sbp_acrl_inv_data_max_mth') }}
),

source_wi_depl_sbp_acrl_inv_data AS (
    SELECT
        dv_fiscal_year_mth_int,
        product_key,
        dv_bundle_prdt_key,
        accounting_rule_name,
        subscription_ref_id,
        subscription_id,
        dv_invoice_dt,
        dv_bill_rel_to_invoicing_dt,
        tsv_trxl_amt,
        ru_credit_trxl_amt,
        contra_asset,
        cumulative_contra_dr,
        invoicing,
        cumulative_invoicing
    FROM {{ source('raw', 'wi_depl_sbp_acrl_inv_data') }}
),

source_wi_depl_sbp_acrl_trxnl_data AS (
    SELECT
        dv_fiscal_year_mth_int,
        product_key,
        dv_bundle_prdt_key,
        accounting_rule_name,
        subscription_ref_id,
        subscription_id,
        tsv_trxl_amt,
        ru_credit_trxl_amt,
        contra_asset,
        cumulative_contra_dr,
        identifier
    FROM {{ source('raw', 'wi_depl_sbp_acrl_trxnl_data') }}
),

source_wi_depl_sbp_accrual_invoicing_calc AS (
    SELECT
        dv_fiscal_year_mth_int,
        product_key,
        dv_bundle_prdt_key,
        accounting_rule_name,
        subscription_ref_id,
        subscription_id,
        dv_invoice_dt,
        dv_bill_rel_to_invoicing_dt,
        tsv_trxl_amt,
        ru_credit_trxl_amt,
        contra_asset,
        cumulative_contra,
        invoicing,
        cumulative_invoice,
        remaining_ca
    FROM {{ source('raw', 'wi_depl_sbp_accrual_invoicing_calc') }}
),

source_wi_sbp_accrual_depletion_rev_seq AS (
    SELECT
        processed_fiscal_month,
        invoice_fiscal_month,
        product_key,
        dv_bundle_prdt_key,
        accounting_rule_name,
        subscription_ref_id,
        subscription_id,
        dv_invoice_dt,
        dv_bill_rel_to_invoicing_dt,
        invoicing,
        cumulative_invoicing,
        actual_remaining_ca,
        contra_asset,
        original_accrued,
        remaining_accrued,
        invoiced_accrual_release,
        remaining_accrued_csum,
        rev_seq_no
    FROM {{ source('raw', 'wi_sbp_accrual_depletion_rev_seq') }}
),

source_wi_sbp_future_invoice AS (
    SELECT
        dv_fiscal_year_mth_int,
        subscription_ref_id,
        subscription_id,
        product_key,
        dv_bundle_prdt_key,
        dv_invoice_dt,
        dv_bill_rel_to_invoicing_dt,
        invoicing,
        cumulative_invoicing,
        next_invoice_month
    FROM {{ source('raw', 'wi_sbp_future_invoice') }}
),

final AS (
    SELECT
        dv_fiscal_year_mth_int,
        product_key,
        dv_bundle_prdt_key,
        accounting_rule_name,
        subscription_ref_id,
        subscription_id,
        dv_invoice_dt,
        dv_bill_rel_to_invoicing_dt
    FROM source_wi_sbp_future_invoice
)

SELECT * FROM final