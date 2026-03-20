{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_rae_adjstmnt_gl_dstrbtn', 'batch', 'edwtd_ngccrm'],
    meta={
        'source_workflow': 'wf_m_N_RAE_ADJSTMNT_GL_DSTRBTN',
        'target_table': 'N_RAE_ADJSTMNT_GL_DSTRBTN',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.458257+00:00'
    }
) }}

WITH 

source_w_rae_adjstmnt_gl_dstrbtn AS (
    SELECT
        rae_adjustment_gl_dstrbtn_key,
        credit_debit_type,
        gl_dtm,
        gl_posted_flg,
        journal_entry_batch_id_int,
        journal_entry_source_name,
        company_cd,
        set_of_books_key,
        general_ledger_account_key,
        sk_summary_id_int,
        ss_cd,
        ru_functional_credit_amt,
        ru_transactional_credit_amt,
        ru_functional_debit_amt,
        ru_transactional_debit_amt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_rae_adjstmnt_gl_dstrbtn') }}
),

final AS (
    SELECT
        rae_adjustment_gl_dstrbtn_key,
        credit_debit_type,
        gl_dtm,
        gl_posted_flg,
        journal_entry_batch_id_int,
        journal_entry_source_name,
        company_cd,
        set_of_books_key,
        general_ledger_account_key,
        sk_summary_id_int,
        ss_cd,
        ru_functional_credit_amt,
        ru_transactional_credit_amt,
        ru_functional_debit_amt,
        ru_transactional_debit_amt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_rae_adjstmnt_gl_dstrbtn
)

SELECT * FROM final