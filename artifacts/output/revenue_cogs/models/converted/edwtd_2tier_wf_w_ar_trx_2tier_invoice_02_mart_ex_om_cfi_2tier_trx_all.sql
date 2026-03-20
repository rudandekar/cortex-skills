{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_ar_trx_2tier_invoice', 'batch', 'edwtd_2tier'],
    meta={
        'source_workflow': 'wf_m_W_AR_TRX_2TIER_INVOICE',
        'target_table': 'EX_OM_CFI_2TIER_TRX_ALL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.588031+00:00'
    }
) }}

WITH 

source_ex_cg_cfi_2tier_trx_all AS (
    SELECT
        ae_header_id,
        ae_line_num,
        application_id,
        code_combination_id,
        gl_sl_link_id,
        account_class,
        party_id,
        party_site_id,
        party_type_code,
        entered_dr,
        entered_cr,
        accounted_dr,
        accounted_cr,
        currency_code,
        attribute_category,
        order_number,
        order_line_id,
        trx_number,
        trx_line_id,
        trx_date,
        transaction_dist_id,
        dist_type,
        quantity_invoiced,
        transaction_id,
        gl_sl_link_table,
        unrounded_accounted_dr,
        unrounded_accounted_cr,
        unrounded_entered_dr,
        unrounded_entered_cr,
        accounting_date,
        ledger_id,
        source_table,
        source_id,
        creation_date,
        last_update_date,
        global_name,
        edw_create_datetime,
        edw_create_user,
        edw_update_datetime,
        edw_update_user,
        exception_type
    FROM {{ source('raw', 'ex_cg_cfi_2tier_trx_all') }}
),

source_w_ar_trx_2tier_invoice AS (
    SELECT
        ar_trx_2tier_invoice_key,
        two_tier_invoice_usd_amt,
        two_tier_invoice_trxl_amt,
        ar_transaction_key,
        fiscal_calendar_cd,
        fiscal_year_num_int,
        fiscal_month_num_int,
        dd_two_tier_theater_name,
        dd_erp_sales_channel_cd,
        dd_operating_unit_name_cd,
        sk_transaction_id_int,
        ss_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_ar_trx_2tier_invoice') }}
),

source_st_om_cfi_2tier_trx_all AS (
    SELECT
        amount,
        created_by,
        creation_date,
        customer_id,
        customer_trx_id,
        ges_update_date,
        global_name,
        last_updated_by,
        last_update_date,
        last_update_login,
        latest_flag,
        org_id,
        period_num,
        period_year,
        quantity_invoiced,
        set_of_books_id,
        transaction_id,
        trx_number,
        trx_type,
        batch_id,
        create_datetime,
        action_cd
    FROM {{ source('raw', 'st_om_cfi_2tier_trx_all') }}
),

transformed_exp_w_ar_trx_2tier_invoice AS (
    SELECT
    rol_trx_line_gl_distri_key,
    ar_trx_2tier_invoice_key,
    two_tier_invoice_usd_amt,
    two_tier_invoice_trxl_amt,
    ar_transaction_key,
    fiscal_calendar_cd,
    fiscal_year_num_int,
    fiscal_month_num_int,
    dd_two_tier_theater_name,
    dd_erp_sales_channel_cd,
    dd_operating_unit_name_cd,
    sk_transaction_id_int,
    ss_cd,
    edw_create_dtm,
    edw_create_user,
    edw_update_dtm,
    edw_update_user,
    'I' AS action_code,
    '=' AS dml_type
    FROM source_st_om_cfi_2tier_trx_all
),

final AS (
    SELECT
        amount,
        created_by,
        creation_date,
        customer_id,
        customer_trx_id,
        ges_update_date,
        global_name,
        last_updated_by,
        last_update_date,
        last_update_login,
        latest_flag,
        org_id,
        period_num,
        period_year,
        quantity_invoiced,
        set_of_books_id,
        transaction_id,
        trx_number,
        trx_type,
        batch_id,
        create_datetime,
        action_cd,
        exception_type
    FROM transformed_exp_w_ar_trx_2tier_invoice
)

SELECT * FROM final