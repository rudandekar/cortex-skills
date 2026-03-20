{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_si_market_lob', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_FF_SI_MARKET_LOB',
        'target_table': 'FF_SI_MARKET_LOB',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:29.016121+00:00'
    }
) }}

WITH 

source_si_market_lob AS (
    SELECT
        market_lob_id,
        market_lob_value,
        market_lob_desc,
        created_by,
        create_date,
        last_updated_by,
        last_update_date,
        enabled_flag
    FROM {{ source('raw', 'si_market_lob') }}
),

transformed_exp_si_market_lob AS (
    SELECT
    market_lob_id,
    market_lob_value,
    market_lob_desc,
    enabled_flag,
    last_update_date,
    'BatchId' AS batch_id,
    CURRENT_TIMESTAMP() AS create_datetime,
    'I' AS action_code
    FROM source_si_market_lob
),

final AS (
    SELECT
        batch_id,
        market_lob_id,
        market_lob_value,
        market_lob_desc,
        enabled_flag,
        last_update_date,
        create_datetime,
        action_code
    FROM transformed_exp_si_market_lob
)

SELECT * FROM final