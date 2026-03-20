{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_ed_company_source_map', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_ST_ED_COMPANY_SOURCE_MAP',
        'target_table': 'ST_ED_COMPANY_SOURCE_MAP',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.608198+00:00'
    }
) }}

WITH 

source_ff_ed_company_source_map AS (
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
    FROM {{ source('raw', 'ff_ed_company_source_map') }}
),

final AS (
    SELECT
        batch_id,
        company,
        global_name,
        set_of_books_id,
        currency_code,
        start_fiscal_period_id,
        end_fiscal_period_id,
        create_datetime,
        action_code
    FROM source_ff_ed_company_source_map
)

SELECT * FROM final