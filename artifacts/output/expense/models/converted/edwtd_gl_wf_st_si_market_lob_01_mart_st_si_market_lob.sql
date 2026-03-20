{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_si_market_lob', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_ST_SI_MARKET_LOB',
        'target_table': 'ST_SI_MARKET_LOB',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.805499+00:00'
    }
) }}

WITH 

source_ff_si_market_lob AS (
    SELECT
        batch_id,
        market_lob_id,
        market_lob_value,
        market_lob_desc,
        enabled_flag,
        last_update_date,
        create_datetime,
        action_code
    FROM {{ source('raw', 'ff_si_market_lob') }}
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
    FROM source_ff_si_market_lob
)

SELECT * FROM final