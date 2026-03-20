{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_mf_fnd_currencies', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_ST_MF_FND_CURRENCIES',
        'target_table': 'ST_MF_FND_CURRENCIES',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.639173+00:00'
    }
) }}

WITH 

source_ff_mf_fnd_currencies AS (
    SELECT
        batch_id,
        currency_code,
        global_name,
        issuing_territory_code,
        enabled_flag,
        iso_flag,
        ges_update_date,
        create_datetime,
        action_code
    FROM {{ source('raw', 'ff_mf_fnd_currencies') }}
),

final AS (
    SELECT
        batch_id,
        currency_code,
        global_name,
        issuing_territory_code,
        enabled_flag,
        iso_flag,
        ges_update_date,
        create_datetime,
        action_code
    FROM source_ff_mf_fnd_currencies
)

SELECT * FROM final