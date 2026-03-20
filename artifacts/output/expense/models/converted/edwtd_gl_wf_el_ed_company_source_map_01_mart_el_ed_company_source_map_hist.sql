{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_ed_company_source_map', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_EL_ED_COMPANY_SOURCE_MAP',
        'target_table': 'el_ed_company_source_map_hist',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:29.006872+00:00'
    }
) }}

WITH 

source_st_ed_company_source_map AS (
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
    FROM {{ source('raw', 'st_ed_company_source_map') }}
),

source_el_ed_company_source_map AS (
    SELECT
        company,
        global_name,
        set_of_books_id,
        currency_code,
        start_fiscal_period_id,
        end_fiscal_period_id,
        create_datetime,
        update_datetime
    FROM {{ source('raw', 'el_ed_company_source_map') }}
),

final AS (
    SELECT
        company,
        global_name,
        set_of_books_id,
        currency_code,
        start_fiscal_period_id,
        end_fiscal_period_id,
        create_datetime,
        create_user
    FROM source_el_ed_company_source_map
)

SELECT * FROM final