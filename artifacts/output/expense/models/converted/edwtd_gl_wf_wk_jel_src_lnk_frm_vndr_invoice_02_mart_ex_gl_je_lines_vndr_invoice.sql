{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wk_jel_src_lnk_frm_vndr_invoice', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_WK_JEL_SRC_LNK_FRM_VNDR_INVOICE',
        'target_table': 'EX_GL_JE_LINES_VNDR_INVOICE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.883298+00:00'
    }
) }}

WITH 

source_wi_jrnl_entry_line_12i_cg1 AS (
    SELECT
        bk_journal_entry_number_int,
        bk_journal_entry_line_num_int,
        global_name,
        vendor_invoice_distrib_key,
        vndr_inv_distrib_amt,
        invoice_id,
        sk_invoice_distribution_id_int,
        vndr_inv_distrib_trx_type_cd,
        bk_company_cd,
        set_of_books_key,
        bk_functional_currency_cd,
        transactional_currency_cd,
        dv_fiscal_year_month_num_int,
        dv_usd_amt,
        dv_functional_amt,
        invoice_global_name,
        ae_header_id,
        ae_line_num
    FROM {{ source('raw', 'wi_jrnl_entry_line_12i_cg1') }}
),

source_wi_gl_je_lines_vndr_invoice AS (
    SELECT
        bk_journal_entry_number_int,
        bk_journal_entry_line_num_int,
        bk_company_code,
        set_of_books_key,
        dv_fiscal_year_month_num_int,
        action_code,
        reference_2,
        reference_3,
        ss_cd,
        vendor_invoice_key,
        dv_functional_amt,
        dv_usd_amt,
        application_version_cd,
        functional_currency_cd,
        dv_transactional_amt,
        transactional_currency_cd
    FROM {{ source('raw', 'wi_gl_je_lines_vndr_invoice') }}
),

source_st_mf_gl_je_lines AS (
    SELECT
        batch_id,
        accounted_cr,
        accounted_dr,
        amount_includes_tax_flag,
        attribute1,
        attribute2,
        attribute3,
        attribute4,
        attribute5,
        attribute6,
        attribute7,
        attribute8,
        attribute9,
        code_combination_id,
        created_by,
        creation_date,
        description,
        effective_date,
        entered_cr,
        entered_dr,
        gl_sl_link_id,
        gl_sl_link_table,
        invoice_amount,
        invoice_date,
        invoice_identifier,
        je_header_id,
        je_line_num,
        line_type_code,
        last_updated_by,
        last_update_date,
        no1,
        period_name,
        reference_1,
        reference_10,
        reference_2,
        reference_3,
        reference_4,
        reference_5,
        reference_6,
        reference_7,
        reference_8,
        reference_9,
        status,
        jebe_vat_alloc_flag,
        set_of_books_id,
        global_name,
        ges_update_date,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_mf_gl_je_lines') }}
),

transformed_exp_wk_jel_src_lnk_frm_vndr_invoice AS (
    SELECT
    bk_journal_entry_num_int,
    bk_journal_entry_line_num_int,
    bk_company_cd,
    set_of_books_key,
    vendor_invoice_distrib_key,
    dv_fiscal_year_month_num_int,
    dv_functional_amt,
    dv_usd_amt,
    application_version_cd,
    functional_currency_cd,
    dv_transactional_amt,
    transactional_currency_cd,
    action_code,
    'I' AS dml_type
    FROM source_st_mf_gl_je_lines
),

transformed_exp_w_jel_src_lnk_frm_vndr_invoice_cg11 AS (
    SELECT
    bk_journal_entry_number_int,
    bk_journal_entry_line_num_int,
    bk_company_code,
    set_of_books_key,
    vendor_invoice_distrib_key,
    dv_fiscal_year_month_num_int,
    dv_functional_amt,
    dv_usd_amt,
    application_version_cd,
    functional_currency_cd,
    dv_transactional_amt,
    transactional_currency_cd,
    action_code,
    'I' AS dml_type
    FROM transformed_exp_wk_jel_src_lnk_frm_vndr_invoice
),

final AS (
    SELECT
        batch_id,
        accounted_cr,
        accounted_dr,
        created_by,
        creation_date,
        code_combination_id,
        effective_date,
        entered_cr,
        entered_dr,
        invoice_amount,
        invoice_date,
        invoice_identifier,
        je_header_id,
        je_line_num,
        last_updated_by,
        last_update_date,
        period_name,
        reference_2,
        reference_3,
        reference_6,
        set_of_books_id,
        global_name,
        ges_update_date,
        create_datetime,
        action_code,
        exception_type,
        reference_7,
        reference_8
    FROM transformed_exp_w_jel_src_lnk_frm_vndr_invoice_cg11
)

SELECT * FROM final