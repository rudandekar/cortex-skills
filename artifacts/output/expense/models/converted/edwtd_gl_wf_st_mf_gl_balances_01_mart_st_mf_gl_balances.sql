{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_mf_gl_balances', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_ST_MF_GL_BALANCES',
        'target_table': 'ST_MF_GL_BALANCES',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.816857+00:00'
    }
) }}

WITH 

source_ff_mf_gl_balances AS (
    SELECT
        batch_id,
        actual_flag,
        begin_balance_cr,
        begin_balance_dr,
        code_combination_id,
        currency_code,
        ges_pk_id,
        ges_update_date,
        global_name,
        last_updated_by,
        last_update_date,
        period_name,
        period_net_cr,
        period_net_dr,
        period_num,
        period_type,
        period_year,
        revaluation_status,
        set_of_books_id,
        translated_flag,
        create_datetime,
        action_code
    FROM {{ source('raw', 'ff_mf_gl_balances') }}
),

final AS (
    SELECT
        batch_id,
        actual_flag,
        begin_balance_cr,
        begin_balance_dr,
        code_combination_id,
        currency_code,
        ges_pk_id,
        ges_update_date,
        global_name,
        last_updated_by,
        last_update_date,
        period_name,
        period_net_cr,
        period_net_dr,
        period_num,
        period_type,
        period_year,
        revaluation_status,
        set_of_books_id,
        translated_flag,
        create_datetime,
        action_code
    FROM source_ff_mf_gl_balances
)

SELECT * FROM final