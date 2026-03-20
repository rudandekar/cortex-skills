{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wk_fa_depreciation_period', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_WK_FA_DEPRECIATION_PERIOD',
        'target_table': 'W_FA_DEPRECIATION_PERIOD',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:29.116122+00:00'
    }
) }}

WITH 

source_st_mf_fa_deprn_periods AS (
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
    FROM {{ source('raw', 'st_mf_fa_deprn_periods') }}
),

final AS (
    SELECT
        bk_fa_book_type_cd,
        bk_dprctn_period_seq_num_int,
        sk_period_counter_int,
        ss_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type,
        period_actual_open_dtm,
        period_actual_closed_dtm,
        dv_fiscal_year_month_int
    FROM source_st_mf_fa_deprn_periods
)

SELECT * FROM final