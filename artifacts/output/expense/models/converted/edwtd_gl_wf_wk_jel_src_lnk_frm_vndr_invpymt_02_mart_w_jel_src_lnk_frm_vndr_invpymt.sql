{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wk_jel_src_lnk_frm_vndr_invpymt', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_WK_JEL_SRC_LNK_FRM_VNDR_INVPYMT',
        'target_table': 'W_JEL_SRC_LNK_FRM_VNDR_INVPYMT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.746429+00:00'
    }
) }}

WITH 

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

final AS (
    SELECT
        bk_journal_entry_number_int,
        bk_journal_entry_line_num_int,
        bk_company_cd,
        set_of_books_key,
        vendor_invoice_payment_key,
        dv_fiscal_year_month_num_int,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm,
        action_code,
        dml_type
    FROM source_st_mf_gl_je_lines
)

SELECT * FROM final