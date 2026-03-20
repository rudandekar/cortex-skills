{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_ccrm_profile_eoa', 'batch', 'edwtd_ngccrm'],
    meta={
        'source_workflow': 'wf_m_ST_CCRM_PROFILE_EOA',
        'target_table': 'ST_CCRM_PROFILE_EOA',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.774299+00:00'
    }
) }}

WITH 

source_ff_ccrm_profile_eoa AS (
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
    FROM {{ source('raw', 'ff_ccrm_profile_eoa') }}
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
        creation_datetime,
        action_code
    FROM source_ff_ccrm_profile_eoa
)

SELECT * FROM final