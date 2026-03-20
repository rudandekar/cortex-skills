{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_mf_gl_daily_rates', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_FF_MF_GL_DAILY_RATES',
        'target_table': 'FF_MF_GL_DAILY_RATES',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.604773+00:00'
    }
) }}

WITH 

source_mf_gl_daily_rates AS (
    SELECT
        attribute1,
        attribute10,
        attribute11,
        attribute12,
        attribute13,
        attribute14,
        attribute15,
        attribute2,
        attribute3,
        attribute4,
        attribute5,
        attribute6,
        attribute7,
        attribute8,
        attribute9,
        context,
        conversion_date,
        conversion_rate,
        conversion_type,
        created_by,
        creation_date,
        from_currency,
        ges_update_date,
        global_name,
        last_updated_by,
        last_update_date,
        last_update_login,
        status_code,
        to_currency
    FROM {{ source('raw', 'mf_gl_daily_rates') }}
),

transformed_exp_mf_gl_daily_rates AS (
    SELECT
    from_currency,
    to_currency,
    conversion_date,
    global_name,
    conversion_type,
    conversion_rate,
    ges_update_date,
    'BatchId' AS batch_id,
    CURRENT_TIMESTAMP() AS o_create_datetime,
    'I' AS o_action_code
    FROM source_mf_gl_daily_rates
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
    FROM transformed_exp_mf_gl_daily_rates
)

SELECT * FROM final