{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_mf_gl_balances', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_FF_MF_GL_BALANCES',
        'target_table': 'FF_MF_GL_BALANCES',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.930646+00:00'
    }
) }}

WITH 

source_mf_gl_balances AS (
    SELECT
        actual_flag,
        begin_balance_cr,
        begin_balance_cr_beq,
        begin_balance_dr,
        begin_balance_dr_beq,
        budget_version_id,
        code_combination_id,
        currency_code,
        encumbrance_doc_id,
        encumbrance_line_num,
        encumbrance_type_id,
        ges_pk_id,
        ges_update_date,
        global_name,
        last_updated_by,
        last_update_date,
        period_name,
        period_net_cr,
        period_net_cr_beq,
        period_net_dr,
        period_net_dr_beq,
        period_num,
        period_to_date_adb,
        period_type,
        period_year,
        project_to_date_adb,
        project_to_date_cr,
        project_to_date_cr_beq,
        project_to_date_dr,
        project_to_date_dr_beq,
        quarter_to_date_adb,
        quarter_to_date_cr,
        quarter_to_date_cr_beq,
        quarter_to_date_dr,
        quarter_to_date_dr_beq,
        revaluation_status,
        set_of_books_id,
        template_id,
        translated_flag,
        year_to_date_adb
    FROM {{ source('raw', 'mf_gl_balances') }}
),

transformed_exp_mf_gl_balances AS (
    SELECT
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
    'BatchId' AS batch_id,
    CURRENT_TIMESTAMP() AS create_datetime,
    'I' AS action_code
    FROM source_mf_gl_balances
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
    FROM transformed_exp_mf_gl_balances
)

SELECT * FROM final