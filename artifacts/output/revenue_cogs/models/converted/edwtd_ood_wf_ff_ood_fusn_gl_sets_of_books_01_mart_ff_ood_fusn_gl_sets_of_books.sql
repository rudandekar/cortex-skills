{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_ood_fusn_gl_sets_of_books', 'batch', 'edwtd_ood'],
    meta={
        'source_workflow': 'wf_m_FF_OOD_FUSN_GL_SETS_OF_BOOKS',
        'target_table': 'FF_OOD_FUSN_GL_SETS_OF_BOOKS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.722102+00:00'
    }
) }}

WITH 

source_saas_gl_sets_of_books AS (
    SELECT
        xpk_root,
        xpk_gl_sob,
        fk_root,
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
        last_update_date
    FROM {{ source('raw', 'saas_gl_sets_of_books') }}
),

xml_parsed_xmldsq_saas_gl_sets_of_books AS (
    SELECT
        src.*,
        xml_record.VALUE AS xml_content
    FROM source_saas_gl_sets_of_books src,
    LATERAL FLATTEN(INPUT => PARSE_XML(src.xml_data):"root") xml_record
),

transformed_exp_saas_gl_sets_of_books AS (
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
    CURRENT_TIMESTAMP() AS create_datetime,
    'I' AS action_code
    FROM xml_parsed_xmldsq_saas_gl_sets_of_books
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
    FROM transformed_exp_saas_gl_sets_of_books
)

SELECT * FROM final