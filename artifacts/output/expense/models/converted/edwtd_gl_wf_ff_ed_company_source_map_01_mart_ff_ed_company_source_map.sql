{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_ed_company_source_map', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_FF_ED_COMPANY_SOURCE_MAP',
        'target_table': 'FF_ED_COMPANY_SOURCE_MAP',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.842140+00:00'
    }
) }}

WITH 

source_ed_company_source_map AS (
    SELECT
        company,
        set_of_books_id,
        global_name,
        currency_code,
        start_fiscal_period_id,
        end_fiscal_period_id
    FROM {{ source('raw', 'ed_company_source_map') }}
),

transformed_exp_default_values AS (
    SELECT
    company,
    set_of_books_id,
    currency_code,
    start_fiscal_period_id,
    end_fiscal_period_id,
    global_name,
    'BatchId' AS batch_id,
    CURRENT_TIMESTAMP() AS create_datetime,
    'I' AS action_code,
    TO_INTEGER(START_FISCAL_PERIOD_ID) AS o_start_fiscal_period_id,
    TO_INTEGER(END_FISCAL_PERIOD_ID) AS o_end_fiscal_period_id
    FROM source_ed_company_source_map
),

final AS (
    SELECT
        batch_id,
        company,
        set_of_books_id,
        currency_code,
        global_name,
        start_fiscal_period_id,
        end_fiscal_period_id,
        create_datetime,
        action_code
    FROM transformed_exp_default_values
)

SELECT * FROM final