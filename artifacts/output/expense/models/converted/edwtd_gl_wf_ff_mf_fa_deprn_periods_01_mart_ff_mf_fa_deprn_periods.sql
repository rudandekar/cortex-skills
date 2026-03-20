{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_mf_fa_deprn_periods', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_FF_MF_FA_DEPRN_PERIODS',
        'target_table': 'FF_MF_FA_DEPRN_PERIODS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:29.102633+00:00'
    }
) }}

WITH 

source_mf_fa_deprn_periods AS (
    SELECT
        addition_batch_id,
        adjustment_batch_id,
        book_type_code,
        calendar_period_close_date,
        calendar_period_open_date,
        cip_addition_batch_id,
        cip_adjustment_batch_id,
        cip_reclass_batch_id,
        cip_retirement_batch_id,
        cip_reval_batch_id,
        cip_transfer_batch_id,
        deferred_deprn_batch_id,
        depreciation_batch_id,
        deprn_adjustment_batch_id,
        deprn_run,
        fiscal_year,
        ges_update_date,
        global_name,
        period_close_date,
        period_counter,
        period_name,
        period_num,
        period_open_date,
        reclass_batch_id,
        retirement_batch_id,
        reval_batch_id,
        transfer_batch_id
    FROM {{ source('raw', 'mf_fa_deprn_periods') }}
),

transformed_exp_mf_fa_deprn_periods AS (
    SELECT
    addition_batch_id,
    adjustment_batch_id,
    book_type_code,
    calendar_period_close_date,
    calendar_period_open_date,
    cip_addition_batch_id,
    cip_adjustment_batch_id,
    cip_reclass_batch_id,
    cip_retirement_batch_id,
    cip_reval_batch_id,
    cip_transfer_batch_id,
    deferred_deprn_batch_id,
    depreciation_batch_id,
    deprn_adjustment_batch_id,
    deprn_run,
    fiscal_year,
    ges_update_date,
    global_name,
    period_close_date,
    period_counter,
    period_name,
    period_num,
    period_open_date,
    reclass_batch_id,
    retirement_batch_id,
    reval_batch_id,
    transfer_batch_id,
    'BatchId' AS batch_id,
    CURRENT_TIMESTAMP() AS create_datetime,
    'I' AS action_code
    FROM source_mf_fa_deprn_periods
),

final AS (
    SELECT
        batch_id,
        addition_batch_id,
        adjustment_batch_id,
        book_type_code,
        calendar_period_close_date,
        calendar_period_open_date,
        cip_addition_batch_id,
        cip_adjustment_batch_id,
        cip_reclass_batch_id,
        cip_retirement_batch_id,
        cip_reval_batch_id,
        cip_transfer_batch_id,
        deferred_deprn_batch_id,
        depreciation_batch_id,
        deprn_adjustment_batch_id,
        deprn_run,
        fiscal_year,
        ges_update_date,
        global_name,
        period_close_date,
        period_counter,
        period_name,
        period_num,
        period_open_date,
        reclass_batch_id,
        retirement_batch_id,
        reval_batch_id,
        transfer_batch_id,
        create_datetime,
        action_code
    FROM transformed_exp_mf_fa_deprn_periods
)

SELECT * FROM final