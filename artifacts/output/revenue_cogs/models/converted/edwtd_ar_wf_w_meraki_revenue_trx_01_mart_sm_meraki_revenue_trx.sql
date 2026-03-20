{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_meraki_revenue_trx', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_W_MERAKI_REVENUE_TRX',
        'target_table': 'SM_MERAKI_REVENUE_TRX',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.338366+00:00'
    }
) }}

WITH 

source_sm_meraki_revenue_trx AS (
    SELECT
        meraki_rev_trx_key,
        gl_dt,
        credit_general_ledger_acct_key,
        debit_general_ledger_acct_key,
        offr_attr_id_int,
        ar_trx_line_key,
        acct_class_cd,
        retro_flg,
        reclaim_ver_num_int,
        edw_create_dtm,
        edw_create_user
    FROM {{ source('raw', 'sm_meraki_revenue_trx') }}
),

source_w_meraki_revenue_trx AS (
    SELECT
        meraki_rev_trx_key,
        ar_trx_key,
        ar_trx_line_key,
        sales_order_line_key,
        bk_gl_trx_fscl_cal_cd,
        bk_gl_trx_fscl_yr_num_int,
        bk_gl_trx_fscl_mth_num_int,
        product_key,
        credit_general_ledger_acct_key,
        debit_general_ledger_acct_key,
        gl_dt,
        bk_trxl_iso_curr_cd,
        acct_class_cd,
        meraki_trx_qty,
        credit_debit_type_cd,
        gl_posted_dt,
        dv_bundle_prdt_key,
        offer_attr_id_int,
        attribution_cd,
        ss_cd,
        credit_trxl_amt,
        credit_functional_amt,
        debit_trxl_amt,
        debit_functional_amt,
        dv_fiscal_mth_yr_num_int,
        accounting_rule_name,
        rev_recognized_flg,
        sequence_id,
        offr_attr_pct,
        business_application_name,
        claim_start_dt,
        claim_end_dt,
        trxl_line_pct,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        retro_flg,
        reclaim_ver_num_int,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_meraki_revenue_trx') }}
),

final AS (
    SELECT
        meraki_rev_trx_key,
        gl_dt,
        credit_general_ledger_acct_key,
        debit_general_ledger_acct_key,
        offr_attr_id_int,
        ar_trx_line_key,
        acct_class_cd,
        retro_flg,
        reclaim_ver_num_int,
        edw_create_dtm,
        edw_create_user
    FROM source_w_meraki_revenue_trx
)

SELECT * FROM final