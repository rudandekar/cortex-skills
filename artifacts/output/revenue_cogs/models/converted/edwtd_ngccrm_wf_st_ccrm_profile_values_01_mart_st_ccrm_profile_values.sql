{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_ccrm_profile_values', 'batch', 'edwtd_ngccrm'],
    meta={
        'source_workflow': 'wf_m_ST_CCRM_PROFILE_VALUES',
        'target_table': 'ST_CCRM_PROFILE_VALUES',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.428926+00:00'
    }
) }}

WITH 

source_ff_ccrm_profile_values AS (
    SELECT
        batch_id,
        profile_id,
        type,
        deal_value,
        last_updated_by,
        last_update_date,
        current_datetime,
        action_code
    FROM {{ source('raw', 'ff_ccrm_profile_values') }}
),

final AS (
    SELECT
        batch_id,
        profile_id,
        profile_type,
        deal_value,
        last_updated_by,
        last_update_date,
        creation_datetime,
        action_code
    FROM source_ff_ccrm_profile_values
)

SELECT * FROM final