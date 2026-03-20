{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_mf_fnd_currencies_tl', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_FF_MF_FND_CURRENCIES_TL',
        'target_table': 'FF_MF_FND_CURRENCIES_TL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.692126+00:00'
    }
) }}

WITH 

source_mf_fnd_currencies_tl AS (
    SELECT
        created_by,
        creation_date,
        currency_code,
        description,
        ges_update_date,
        global_name,
        language,
        last_updated_by,
        last_update_date,
        last_update_login,
        name,
        source_lang
    FROM {{ source('raw', 'mf_fnd_currencies_tl') }}
),

transformed_exp_set_default_value AS (
    SELECT
    currency_code,
    'BatchId' AS o_batch_id,
    CURRENT_TIMESTAMP() AS o_create_datetime,
    'I' AS o_action_code
    FROM source_mf_fnd_currencies_tl
),

final AS (
    SELECT
        batch_id,
        currency_code,
        global_name,
        language_code,
        description,
        ges_update_date,
        create_datetime,
        action_code
    FROM transformed_exp_set_default_value
)

SELECT * FROM final