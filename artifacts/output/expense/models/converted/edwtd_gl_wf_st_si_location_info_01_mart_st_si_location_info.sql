{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_si_location_info', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_ST_SI_LOCATION_INFO',
        'target_table': 'ST_SI_LOCATION_INFO',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.805188+00:00'
    }
) }}

WITH 

source_ff_si_location_info AS (
    SELECT
        batch_id,
        location_value,
        location_name,
        company_value,
        last_update_date,
        create_datetime,
        action_code
    FROM {{ source('raw', 'ff_si_location_info') }}
),

final AS (
    SELECT
        batch_id,
        location_value,
        location_name,
        company_value,
        last_update_date,
        create_datetime,
        action_code
    FROM source_ff_si_location_info
)

SELECT * FROM final