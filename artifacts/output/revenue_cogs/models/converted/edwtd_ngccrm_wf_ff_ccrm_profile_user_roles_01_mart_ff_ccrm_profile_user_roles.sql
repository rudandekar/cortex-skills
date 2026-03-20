{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_ccrm_profile_user_roles', 'batch', 'edwtd_ngccrm'],
    meta={
        'source_workflow': 'wf_m_FF_CCRM_PROFILE_USER_ROLES',
        'target_table': 'FF_CCRM_PROFILE_USER_ROLES',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.026501+00:00'
    }
) }}

WITH 

source_ccrm_profile_user_roles AS (
    SELECT
        user_id,
        role_id,
        profile_id,
        last_updated_by,
        last_update_date
    FROM {{ source('raw', 'ccrm_profile_user_roles') }}
),

transformed_ccrm_profile_user_roles AS (
    SELECT
    user_id,
    role_id,
    profile_id,
    last_updated_by,
    last_update_date,
    'Batch_Id' AS batch_id,
    CURRENT_TIMESTAMP() AS current_datetime,
    'I' AS action_code
    FROM source_ccrm_profile_user_roles
),

final AS (
    SELECT
        batch_id,
        user_id,
        role_id,
        profile_id,
        last_updated_by,
        last_update_date,
        current_datetime,
        action_code
    FROM transformed_ccrm_profile_user_roles
)

SELECT * FROM final