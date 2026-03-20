{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_ccrm_profile_values', 'batch', 'edwtd_ngccrm'],
    meta={
        'source_workflow': 'wf_m_FF_CCRM_PROFILE_VALUES',
        'target_table': 'FF_CCRM_PROFILE_VALUES',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.193454+00:00'
    }
) }}

WITH 

source_ccrm_profile_values AS (
    SELECT
        profile_id,
        type,
        deal_value,
        last_updated_by,
        last_update_date
    FROM {{ source('raw', 'ccrm_profile_values') }}
),

transformed_exp_ccrm_profile_values AS (
    SELECT
    profile_id,
    type,
    deal_value,
    last_updated_by,
    last_update_date,
    'BatchId' AS batchid_out,
    CURRENT_TIMESTAMP() AS create_datetime,
    'I' AS action_code
    FROM source_ccrm_profile_values
),

final AS (
    SELECT
        batch_id,
        profile_id,
        type,
        deal_value,
        last_updated_by,
        last_update_date,
        current_datetime,
        action_code
    FROM transformed_exp_ccrm_profile_values
)

SELECT * FROM final