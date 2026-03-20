{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_ood_fusn_gl_rates', 'batch', 'edwtd_ood'],
    meta={
        'source_workflow': 'wf_m_ST_OOD_FUSN_GL_RATES',
        'target_table': 'ST_OOD_FUSN_GL_RATES',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.444808+00:00'
    }
) }}

WITH 

source_ff_ood_fusn_gl_rates AS (
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
    FROM {{ source('raw', 'ff_ood_fusn_gl_rates') }}
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
    FROM source_ff_ood_fusn_gl_rates
)

SELECT * FROM final