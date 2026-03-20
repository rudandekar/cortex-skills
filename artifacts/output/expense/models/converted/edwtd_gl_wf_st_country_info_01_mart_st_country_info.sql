{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_country_info', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_ST_COUNTRY_INFO',
        'target_table': 'ST_COUNTRY_INFO',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.897377+00:00'
    }
) }}

WITH 

source_ff_country_info AS (
    SELECT
        segment1,
        country,
        last_update_date,
        create_datetime,
        action_code
    FROM {{ source('raw', 'ff_country_info') }}
),

final AS (
    SELECT
        segment1,
        country,
        last_update_date,
        create_datetime,
        action_code
    FROM source_ff_country_info
)

SELECT * FROM final