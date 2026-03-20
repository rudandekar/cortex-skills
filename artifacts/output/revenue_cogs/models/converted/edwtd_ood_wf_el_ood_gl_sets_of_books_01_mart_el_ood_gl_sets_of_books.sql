{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_ood_gl_sets_of_books', 'batch', 'edwtd_ood'],
    meta={
        'source_workflow': 'wf_m_EL_OOD_GL_SETS_OF_BOOKS',
        'target_table': 'EL_OOD_GL_SETS_OF_BOOKS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.464604+00:00'
    }
) }}

WITH 

source_st_ood_gl_sets_of_books AS (
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
        mrc_sob_type_code,
        creation_date,
        last_update_date,
        action_code
    FROM {{ source('raw', 'st_ood_gl_sets_of_books') }}
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
        mrc_sob_type_code,
        creation_date,
        last_update_date,
        identifier
    FROM source_st_ood_gl_sets_of_books
)

SELECT * FROM final