{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_ood_fusn_gl_sets_of_books', 'batch', 'edwtd_ood'],
    meta={
        'source_workflow': 'wf_m_ST_OOD_FUSN_GL_SETS_OF_BOOKS',
        'target_table': 'ST_OOD_FUSN_GL_SETS_OF_BOOKS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.183943+00:00'
    }
) }}

WITH 

source_ff_ood_fusn_gl_sets_of_books AS (
    SELECT
        set_of_books_id,
        name,
        short_name,
        description,
        allow_intercompany_post_flag,
        chart_of_accounts_id,
        currency_code,
        mrc_sob_type_code,
        attribute1,
        creation_date,
        last_update_date,
        create_datetime,
        action_code
    FROM {{ source('raw', 'ff_ood_fusn_gl_sets_of_books') }}
),

final AS (
    SELECT
        set_of_books_id,
        name,
        short_name,
        description,
        allow_intercompany_post_flag,
        chart_of_accounts_id,
        currency_code,
        mrc_sob_type_code,
        attribute1,
        creation_date,
        last_update_date,
        create_datetime,
        action_code
    FROM source_ff_ood_fusn_gl_sets_of_books
)

SELECT * FROM final