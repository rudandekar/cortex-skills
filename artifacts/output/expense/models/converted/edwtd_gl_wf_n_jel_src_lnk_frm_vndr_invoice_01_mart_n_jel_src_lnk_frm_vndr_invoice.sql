{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_jel_src_lnk_frm_vndr_invoice', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_N_JEL_SRC_LNK_FRM_VNDR_INVOICE',
        'target_table': 'N_JEL_SRC_LNK_FRM_VNDR_INVOICE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.994656+00:00'
    }
) }}

WITH 

source_w_jel_src_lnk_frm_vndr_invoice AS (
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
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_jel_src_lnk_frm_vndr_invoice') }}
),

final AS (
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
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_jel_src_lnk_frm_vndr_invoice
)

SELECT * FROM final