{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_sls_crdt_derivation', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_WI_SLS_CRDT_DERIVATION',
        'target_table': 'WI_SLS_CRDT_DERIVATION',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.015388+00:00'
    }
) }}

WITH 

source_wi_unearn_def_rebates AS (
    SELECT
        sk_cst_trx_lin_gl_dist_id_lint,
        general_ledger_account_key,
        account_class_code,
        unearn_financial_account_code,
        invoice_line_usd_amount,
        edw_create_datetime,
        edw_create_user,
        product_key,
        gl_datetime
    FROM {{ source('raw', 'wi_unearn_def_rebates') }}
),

source_wi_unearn_def_meraki AS (
    SELECT
        ar_trx_key,
        ar_trx_line_key,
        ss_code,
        general_ledger_account_key,
        account_class_code,
        unearn_financial_account_code,
        edw_create_datetime,
        edw_create_user,
        gl_datetime,
        sk_offer_attribution_id_int,
        meraki_rev_trx_key
    FROM {{ source('raw', 'wi_unearn_def_meraki') }}
),

source_wi_unearn_def_accrual AS (
    SELECT
        ar_trx_key,
        ar_trx_line_key,
        ss_code,
        general_ledger_account_key,
        account_class_code,
        unearn_financial_account_code,
        edw_create_datetime,
        edw_create_user,
        gl_datetime,
        accrued_rev_trx_key,
        sk_offer_attribution_id_int
    FROM {{ source('raw', 'wi_unearn_def_accrual') }}
),

source_wi_unearn_def_oa_s AS (
    SELECT
        sk_cst_trx_lin_gl_dist_id_lint,
        general_ledger_account_key,
        account_class_code,
        unearn_financial_account_code,
        invoice_line_usd_amount,
        edw_create_datetime,
        edw_create_user
    FROM {{ source('raw', 'wi_unearn_def_oa_s') }}
),

source_wi_sls_crdt_derivation AS (
    SELECT
        bk_ar_trx_line_gl_distrib_key,
        ar_trx_key,
        ar_trx_line_key,
        ss_code,
        sales_order_key,
        sales_order_line_key,
        sales_territory_key,
        sls_comm_pct,
        service_contract_number,
        edw_create_datetime,
        edw_create_user,
        edw_update_datetime,
        edw_update_user,
        source_code,
        sk_offer_attribution_id_int,
        accrued_rev_trx_key,
        dv_attribution_cd,
        product_key,
        dv_product_key,
        meraki_rev_trx_key
    FROM {{ source('raw', 'wi_sls_crdt_derivation') }}
),

source_wi_def_curr_conv_rate AS (
    SELECT
        bk_from_currency_code,
        pl_conversion_rate,
        funcnl_trxnl_code,
        edw_create_datetime,
        edw_create_user,
        edw_update_datetime,
        edw_update_user
    FROM {{ source('raw', 'wi_def_curr_conv_rate') }}
),

source_wi_unearn_def_rte AS (
    SELECT
        sk_cst_trx_lin_gl_dist_id_lint,
        general_ledger_account_key,
        account_class_code,
        unearn_financial_account_code,
        invoice_line_usd_amount,
        edw_create_datetime,
        edw_create_user
    FROM {{ source('raw', 'wi_unearn_def_rte') }}
),

source_wi_unearn_def_non_rte AS (
    SELECT
        ar_trx_key,
        ar_trx_line_key,
        ss_code,
        general_ledger_account_key,
        account_class_code,
        unearn_financial_account_code,
        invoice_line_usd_amount,
        edw_create_datetime,
        edw_create_user,
        sub_prd_ar_trx_line_key,
        gl_datetime
    FROM {{ source('raw', 'wi_unearn_def_non_rte') }}
),

final AS (
    SELECT
        bk_ar_trx_line_gl_distrib_key,
        ar_trx_key,
        ar_trx_line_key,
        ss_code,
        sales_order_key,
        sales_order_line_key,
        sales_territory_key,
        sls_comm_pct,
        service_contract_number,
        edw_create_datetime,
        edw_create_user,
        edw_update_datetime,
        edw_update_user,
        source_code,
        sk_offer_attribution_id_int,
        dv_adj_rebate_trx_key,
        pos_trx_id_int,
        adjustment_trx_type_cd,
        accrued_rev_trx_key,
        dv_attribution_cd,
        product_key,
        dv_product_key,
        meraki_rev_trx_key
    FROM source_wi_unearn_def_non_rte
)

SELECT * FROM final