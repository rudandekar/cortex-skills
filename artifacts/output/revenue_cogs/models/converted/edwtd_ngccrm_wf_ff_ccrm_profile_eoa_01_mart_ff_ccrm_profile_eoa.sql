{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_ccrm_profile_eoa', 'batch', 'edwtd_ngccrm'],
    meta={
        'source_workflow': 'wf_m_FF_CCRM_PROFILE_EOA',
        'target_table': 'FF_CCRM_PROFILE_EOA',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.647159+00:00'
    }
) }}

WITH 

source_ccrm_profile_eoa AS (
    SELECT
        eoa_id,
        profile_id,
        fiscal_period,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        eoa_type,
        reason_code,
        comments
    FROM {{ source('raw', 'ccrm_profile_eoa') }}
),

transformed_ccrm_profile_eoa AS (
    SELECT
    eoa_id,
    profile_id,
    fiscal_period,
    created_by,
    creation_date,
    last_updated_by,
    last_update_date,
    eoa_type,
    reason_code,
    comments,
    'Batch_Id' AS batch_id,
    CURRENT_TIMESTAMP() AS current_datetime,
    'I' AS action_code
    FROM source_ccrm_profile_eoa
),

final AS (
    SELECT
        batch_id,
        eoa_id,
        profile_id,
        fiscal_period,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        eoa_type,
        reason_code,
        comments,
        current_datetime,
        action_code
    FROM transformed_ccrm_profile_eoa
)

SELECT * FROM final