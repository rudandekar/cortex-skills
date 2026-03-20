{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_wips_disti_supported_info', 'batch', 'edwtd_pos'],
    meta={
        'source_workflow': 'wf_m_ST_WIPS_DISTI_SUPPORTED_INFO',
        'target_table': 'ST_WIPS_DISTI_SUPPORTED_INFO',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.828434+00:00'
    }
) }}

WITH 

source_ff_wips_disti_supported_info_intf AS (
    SELECT
        disti_profile_id,
        country,
        supported_flag,
        contractual_flag,
        legal_flag,
        last_updated_date
    FROM {{ source('raw', 'ff_wips_disti_supported_info_intf') }}
),

final AS (
    SELECT
        disti_profile_id,
        country,
        supported_flag,
        contractual_flag,
        legal_flag,
        last_updated_date
    FROM source_ff_wips_disti_supported_info_intf
)

SELECT * FROM final