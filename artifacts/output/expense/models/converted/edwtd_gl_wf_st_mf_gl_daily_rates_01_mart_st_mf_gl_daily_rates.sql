{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_mf_gl_daily_rates', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_ST_MF_GL_DAILY_RATES',
        'target_table': 'ST_MF_GL_DAILY_RATES',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:29.065504+00:00'
    }
) }}

WITH 

source_ff_mf_gl_daily_rates AS (
    SELECT
        batch_id,
        from_currency,
        to_currency,
        conversion_date,
        global_name,
        conversion_type,
        conversion_rate,
        ges_update_date,
        create_datetime,
        action_code
    FROM {{ source('raw', 'ff_mf_gl_daily_rates') }}
),

final AS (
    SELECT
        batch_id,
        from_currency,
        to_currency,
        conversion_date,
        global_name,
        conversion_type,
        conversion_rate,
        ges_update_date,
        create_datetime,
        action_code
    FROM source_ff_mf_gl_daily_rates
)

SELECT * FROM final