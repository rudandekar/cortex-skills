{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_general_ledger_tv', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_N_GENERAL_LEDGER_TV',
        'target_table': 'N_GENERAL_LEDGER_TV',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.964016+00:00'
    }
) }}

WITH 

source_n_general_ledger_tv AS (
    SELECT
        gl_type_code,
        bk_company_code,
        start_tv_date,
        end_tv_date,
        set_of_books_key,
        bk_functional_currency_code,
        edw_create_datetime,
        edw_update_datetime,
        edw_create_user,
        edw_update_user
    FROM {{ source('raw', 'n_general_ledger_tv') }}
),

source_w_general_ledger AS (
    SELECT
        gl_type_code,
        bk_company_code,
        start_tv_date,
        end_tv_date,
        set_of_books_key,
        bk_functional_currency_code,
        edw_create_datetime,
        edw_update_datetime,
        edw_create_user,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_general_ledger') }}
),

final AS (
    SELECT
        gl_type_code,
        bk_company_code,
        start_tv_date,
        end_tv_date,
        set_of_books_key,
        bk_functional_currency_code,
        edw_create_datetime,
        edw_update_datetime,
        edw_create_user,
        edw_update_user
    FROM source_w_general_ledger
)

SELECT * FROM final