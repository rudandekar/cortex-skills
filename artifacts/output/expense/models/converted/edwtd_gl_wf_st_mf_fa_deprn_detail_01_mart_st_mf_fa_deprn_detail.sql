{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_mf_fa_deprn_detail', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_ST_MF_FA_DEPRN_DETAIL',
        'target_table': 'ST_MF_FA_DEPRN_DETAIL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.688892+00:00'
    }
) }}

WITH 

source_ff_mf_fa_deprn_detail AS (
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
        event_id,
        create_datetime,
        action_code
    FROM {{ source('raw', 'ff_mf_fa_deprn_detail') }}
),

final AS (
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
        event_id,
        create_datetime,
        action_code
    FROM source_ff_mf_fa_deprn_detail
)

SELECT * FROM final