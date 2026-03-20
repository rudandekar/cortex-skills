{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_je_ln_src_lnk_fa_trx_dtl', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_N_JE_LN_SRC_LNK_FA_TRX_DTL',
        'target_table': 'N_JE_LN_SRC_LNK_FA_TRX_DTL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.598477+00:00'
    }
) }}

WITH 

source_w_je_ln_src_lnk_fa_trx_dtl AS (
    SELECT
        bk_journal_entry_num_int,
        bk_journal_entry_line_num_int,
        bk_company_cd,
        set_of_books_key,
        fixed_asset_trx_detail_key,
        application_version_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_je_ln_src_lnk_fa_trx_dtl') }}
),

final AS (
    SELECT
        bk_journal_entry_num_int,
        bk_journal_entry_line_num_int,
        bk_company_cd,
        set_of_books_key,
        fixed_asset_trx_detail_key,
        application_version_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_je_ln_src_lnk_fa_trx_dtl
)

SELECT * FROM final