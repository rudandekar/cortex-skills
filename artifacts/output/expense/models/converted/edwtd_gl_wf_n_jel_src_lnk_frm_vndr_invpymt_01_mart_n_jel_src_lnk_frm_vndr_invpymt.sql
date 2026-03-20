{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_jel_src_lnk_frm_vndr_invpymt', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_N_JEL_SRC_LNK_FRM_VNDR_INVPYMT',
        'target_table': 'N_JEL_SRC_LNK_FRM_VNDR_INVPYMT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.625361+00:00'
    }
) }}

WITH 

source_w_jel_src_lnk_frm_vndr_invpymt AS (
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
    FROM {{ source('raw', 'w_jel_src_lnk_frm_vndr_invpymt') }}
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
        edw_update_dtm
    FROM source_w_jel_src_lnk_frm_vndr_invpymt
)

SELECT * FROM final