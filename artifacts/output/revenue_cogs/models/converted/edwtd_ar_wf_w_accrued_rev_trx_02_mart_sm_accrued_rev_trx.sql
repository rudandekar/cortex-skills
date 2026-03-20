{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_accrued_rev_trx', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_W_ACCRUED_REV_TRX',
        'target_table': 'SM_ACCRUED_REV_TRX',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.778339+00:00'
    }
) }}

WITH 

source_w_accrued_rev_trx AS (
    SELECT
        accrued_rev_trx_key,
        ar_trx_key,
        ar_trx_line_key,
        bk_web_order_id,
        product_key,
        bk_gl_trx_fscl_cal_cd,
        bk_gl_trx_fscl_yr_num_int,
        bk_gl_trx_fscl_mth_num_int,
        sales_order_line_key,
        sk_line_ref_num,
        sk_so_line_id_int,
        dv_bundle_prdt_key,
        bk_trxl_iso_curr_cd,
        gl_dt,
        accrual_trx_qty,
        credit_debit_type,
        gl_posted_dt,
        ss_cd,
        pob_type_cd,
        offr_attr_id_int,
        attribution_cd,
        accounting_rule_name,
        accounting_rule_start_dt,
        accounting_rule_end_dt,
        tsv_trxl_amt,
        rev_recog_flg,
        sk_original_line_ref_num,
        sequence_id,
        offr_attr_pct,
        business_application_name,
        acct_class_cd,
        dv_fiscal_year_mth_int,
        ru_cdt_general_ledger_acct_key,
        ru_dbt_general_ledger_acct_key,
        ru_credit_functional_amt,
        ru_credit_trxl_amt,
        ru_debit_functional_amt,
        ru_debit_trxl_amt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        future_billing_process_flg,
        flexup_resv_trans_flg,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_accrued_rev_trx') }}
),

source_sm_accrued_rev_trx AS (
    SELECT
        accrued_rev_trx_key,
        ar_trx_line_key,
        sk_line_ref_num,
        sk_so_line_id_int,
        gl_dt,
        sequence_id,
        offr_attr_id_int,
        ru_cdt_general_ledger_acct_key,
        ru_dbt_general_ledger_acct_key,
        acct_class_cd,
        transaction_id,
        edw_create_dtm,
        edw_create_user
    FROM {{ source('raw', 'sm_accrued_rev_trx') }}
),

final AS (
    SELECT
        accrued_rev_trx_key,
        ar_trx_line_key,
        sk_line_ref_num,
        sk_so_line_id_int,
        gl_dt,
        sequence_id,
        offr_attr_id_int,
        ru_cdt_general_ledger_acct_key,
        ru_dbt_general_ledger_acct_key,
        acct_class_cd,
        edw_create_dtm,
        edw_create_user,
        trxn_source,
        transaction_id
    FROM source_sm_accrued_rev_trx
)

SELECT * FROM final