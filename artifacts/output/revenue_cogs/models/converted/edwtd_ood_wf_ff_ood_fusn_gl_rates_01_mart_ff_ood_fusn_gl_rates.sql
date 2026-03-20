{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_ood_fusn_gl_rates', 'batch', 'edwtd_ood'],
    meta={
        'source_workflow': 'wf_m_FF_OOD_FUSN_GL_RATES',
        'target_table': 'FF_OOD_FUSN_GL_RATES',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.177306+00:00'
    }
) }}

WITH 

source_saas_gl_rates AS (
    SELECT
        xpk_root,
        xpk_gl_daily_rates,
        fk_root,
        conversion_rate,
        conversion_date,
        conversion_type,
        from_currency,
        to_currency,
        creation_date,
        last_update_date
    FROM {{ source('raw', 'saas_gl_rates') }}
),

transformed_exp_saas_gl_rates AS (
    SELECT
    conversion_rate,
    conversion_date,
    conversion_type,
    from_currency,
    to_currency,
    creation_date,
    last_update_date,
    CURRENT_TIMESTAMP() AS create_datetime,
    'I' AS action_code
    FROM source_saas_gl_rates
),

xml_parsed_xmldsq_saas_gl_rates AS (
    SELECT
        src.*,
        xml_record.VALUE AS xml_content
    FROM transformed_exp_saas_gl_rates src,
    LATERAL FLATTEN(INPUT => PARSE_XML(src.xml_data):"root") xml_record
),

final AS (
    SELECT
        conversion_rate,
        conversion_date,
        conversion_type,
        from_currency,
        to_currency,
        creation_date,
        last_update_date,
        create_datetime,
        action_code
    FROM xml_parsed_xmldsq_saas_gl_rates
)

SELECT * FROM final