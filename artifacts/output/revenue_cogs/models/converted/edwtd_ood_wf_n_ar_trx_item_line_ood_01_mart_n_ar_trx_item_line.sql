{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_ar_trx_item_line_ood', 'batch', 'edwtd_ood'],
    meta={
        'source_workflow': 'wf_m_N_AR_TRX_ITEM_LINE_OOD',
        'target_table': 'N_AR_TRX_ITEM_LINE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.805581+00:00'
    }
) }}

WITH 

source_w_ar_trx_item_line AS (
    SELECT
        ar_trx_line_key,
        product_key,
        ru_accounting_rule_role,
        ru_unit_of_measure_code,
        ru_unit_std_price_local_amt,
        ru_accounting_rule_start_date,
        ru_debit_quantity,
        ru_debit_unit_amount,
        ru_debit_extended_amount,
        ru_credit_quantity,
        ru_credit_unit_amount,
        ru_credit_extended_amount,
        ru_bk_rule_account_rule_name,
        ar_trx_item_line_cr_dr_type,
        money_only_type,
        sk_customer_trx_line_id_lint,
        ss_code,
        edw_create_user,
        edw_update_user,
        edw_create_datetime,
        edw_update_datetime,
        revenue_source_cd,
        service_contract_note_txt,
        ru_acctg_rule_duration_mth_cnt,
        ru_rev_trnsfr_cntrct_start_dt,
        ru_rev_trnsfr_cntrct_end_dt,
        ru_offer_atrbtd_dbt_trx_amt,
        ru_offer_atrbtd_crdt_trx_amt,
        with_parent_role,
        offer_attributed_flg,
        ru_parent_ar_trx_line_key,
        action_code,
        dml_type,
        pricing_term_mths_cnt
    FROM {{ source('raw', 'w_ar_trx_item_line') }}
),

final AS (
    SELECT
        ar_trx_line_key,
        product_key,
        ru_accounting_rule_role,
        ru_unit_of_measure_code,
        ru_unit_std_price_local_amt,
        ru_accounting_rule_start_date,
        ru_debit_quantity,
        ru_debit_unit_amount,
        ru_debit_extended_amount,
        ru_credit_quantity,
        ru_credit_unit_amount,
        ru_credit_extended_amount,
        ru_bk_rule_account_rule_name,
        ar_trx_item_line_cr_dr_type,
        money_only_type,
        sk_customer_trx_line_id_lint,
        ss_code,
        edw_create_user,
        edw_update_user,
        edw_create_datetime,
        edw_update_datetime,
        revenue_source_cd,
        service_contract_note_txt,
        ru_acctg_rule_duration_mth_cnt,
        ru_rev_trnsfr_cntrct_start_dt,
        ru_rev_trnsfr_cntrct_end_dt,
        ru_offer_atrbtd_dbt_trx_amt,
        ru_offer_atrbtd_crdt_trx_amt,
        with_parent_role,
        offer_attributed_flg,
        ru_parent_ar_trx_line_key,
        pricing_term_mths_cnt
    FROM source_w_ar_trx_item_line
)

SELECT * FROM final