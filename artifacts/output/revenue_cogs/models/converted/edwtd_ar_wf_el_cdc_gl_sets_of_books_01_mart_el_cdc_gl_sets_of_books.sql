{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_cdc_gl_sets_of_books', 'realtime', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_EL_CDC_GL_SETS_OF_BOOKS',
        'target_table': 'EL_CDC_GL_SETS_OF_BOOKS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.867897+00:00'
    }
) }}

WITH 

source_el_cdc_gl_sets_of_books AS (
    SELECT
        set_of_books_id,
        allow_intercompany_post_flag,
        chart_of_accounts_id,
        currency_code,
        description,
        name,
        short_name,
        attribute1,
        create_datetime,
        global_name,
        creation_date,
        last_update_date
    FROM {{ source('raw', 'el_cdc_gl_sets_of_books') }}
),

final AS (
    SELECT
        set_of_books_id,
        allow_intercompany_post_flag,
        chart_of_accounts_id,
        currency_code,
        description,
        name,
        short_name,
        attribute1,
        create_datetime,
        global_name,
        creation_date,
        last_update_date
    FROM source_el_cdc_gl_sets_of_books
)

SELECT * FROM final