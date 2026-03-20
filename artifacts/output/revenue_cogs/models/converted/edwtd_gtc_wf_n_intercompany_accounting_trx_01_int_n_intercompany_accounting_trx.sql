{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_n_intercompany_accounting_trx', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_N_INTERCOMPANY_ACCOUNTING_TRX',
        'target_table': 'N_INTERCOMPANY_ACCOUNTING_TRX',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.003430+00:00'
    }
) }}

WITH 

source_n_intercompany_accounting_trx AS (
    SELECT
        ic_accounting_trx_key,
        ep_process_summary_id,
        credit_debit_type,
        partner_owner_type_cd,
        accounting_dt,
        sk_process_history_id_int,
        ss_cd,
        intercompany_transaction_key,
        bk_journal_entry_number_int,
        bk_journal_entry_line_num_int,
        bk_company_code,
        set_of_books_key,
        bk_ic_accounting_rule_type_cd,
        ru_credit_functional_amt,
        transactional_currency_cd,
        ru_credit_transactional_amt,
        general_ledger_account_key,
        ru_debit_functional_amt,
        functional_currency_cd,
        ru_debit_transactional_amt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        ep_source_assignment_id_int,
        vt_gl_batch_name,
        ep_attribute_id
    FROM {{ source('raw', 'n_intercompany_accounting_trx') }}
),

final AS (
    SELECT
        ic_accounting_trx_key,
        ep_process_summary_id,
        credit_debit_type,
        partner_owner_type_cd,
        accounting_dt,
        sk_process_history_id_int,
        ss_cd,
        intercompany_transaction_key,
        bk_journal_entry_number_int,
        bk_journal_entry_line_num_int,
        bk_company_code,
        set_of_books_key,
        bk_ic_accounting_rule_type_cd,
        ru_credit_functional_amt,
        transactional_currency_cd,
        ru_credit_transactional_amt,
        general_ledger_account_key,
        ru_debit_functional_amt,
        functional_currency_cd,
        ru_debit_transactional_amt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        ep_source_assignment_id_int,
        vt_gl_batch_name,
        ep_attribute_id
    FROM source_n_intercompany_accounting_trx
)

SELECT * FROM final