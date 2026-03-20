{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_mf_fa_adjustments', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_ST_MF_FA_ADJUSTMENTS',
        'target_table': 'ST_MF_FA_ADJUSTMENTS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.676276+00:00'
    }
) }}

WITH 

source_ff_mf_fa_adjustments AS (
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
        adjustment_line_id
    FROM {{ source('raw', 'ff_mf_fa_adjustments') }}
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
        adjustment_line_id
    FROM source_ff_mf_fa_adjustments
)

SELECT * FROM final