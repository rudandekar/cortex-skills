{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wk_je_ln_src_lnk_fa_trx_dtl', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_WK_JE_LN_SRC_LNK_FA_TRX_DTL',
        'target_table': 'EX_FA_ADJUSTMENTS_LINK',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:29.062526+00:00'
    }
) }}

WITH 

source_st_mf_fa_deprn_detail AS (
    SELECT
        batch_id,
        addition_cost_to_clear,
        asset_id,
        bonus_deprn_adjustment_amount,
        bonus_deprn_amount,
        bonus_deprn_exp_je_line_num,
        bonus_deprn_reserve,
        bonus_deprn_rsv_je_line_num,
        bonus_ytd_deprn,
        book_type_code,
        cost,
        deprn_adjustment_amount,
        deprn_amount,
        deprn_expense_je_line_num,
        deprn_reserve,
        deprn_reserve_je_line_num,
        deprn_run_date,
        deprn_source_code,
        distribution_id,
        ges_update_date,
        global_name,
        je_header_id,
        period_counter,
        reval_amortization,
        reval_amort_je_line_num,
        reval_deprn_expense,
        reval_reserve,
        reval_reserve_je_line_num,
        ytd_deprn,
        ytd_reval_deprn_expense,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_mf_fa_deprn_detail') }}
),

source_st_mf_fa_adjustments AS (
    SELECT
        batch_id,
        adjustment_amount,
        adjustment_type,
        annualized_adjustment,
        asset_id,
        asset_invoice_id,
        book_type_code,
        code_combination_id,
        debit_credit_flag,
        deprn_override_flag,
        distribution_id,
        ges_pk_id,
        ges_update_date,
        global_attribute13,
        global_attribute14,
        global_attribute15,
        global_attribute3,
        global_name,
        je_header_id,
        je_line_num,
        last_updated_by,
        last_update_date,
        last_update_login,
        period_counter_adjusted,
        period_counter_created,
        source_type_code,
        transaction_header_id,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_mf_fa_adjustments') }}
),

final AS (
    SELECT
        batch_id,
        adjustment_amount,
        adjustment_type,
        annualized_adjustment,
        asset_id,
        asset_invoice_id,
        book_type_code,
        code_combination_id,
        debit_credit_flag,
        deprn_override_flag,
        distribution_id,
        ges_pk_id,
        ges_update_date,
        global_attribute13,
        global_attribute14,
        global_attribute15,
        global_attribute3,
        global_name,
        je_header_id,
        je_line_num,
        last_updated_by,
        last_update_date,
        last_update_login,
        period_counter_adjusted,
        period_counter_created,
        source_type_code,
        transaction_header_id,
        create_datetime,
        action_code,
        adjustment_line_id,
        exception_type
    FROM source_st_mf_fa_adjustments
)

SELECT * FROM final