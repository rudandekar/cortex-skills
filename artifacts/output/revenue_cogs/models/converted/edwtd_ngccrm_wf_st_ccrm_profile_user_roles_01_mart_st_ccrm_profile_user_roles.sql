{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_ccrm_profile_user_roles', 'batch', 'edwtd_ngccrm'],
    meta={
        'source_workflow': 'wf_m_ST_CCRM_PROFILE_USER_ROLES',
        'target_table': 'ST_CCRM_PROFILE_USER_ROLES',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.354588+00:00'
    }
) }}

WITH 

source_ff_ccrm_profile_user_roles AS (
    SELECT
        batch_id,
        user_id,
        role_id,
        profile_id,
        last_updated_by,
        last_update_date,
        current_datetime,
        action_code
    FROM {{ source('raw', 'ff_ccrm_profile_user_roles') }}
),

final AS (
    SELECT
        batch_id,
        user_id,
        role_id,
        profile_id,
        last_updated_by,
        last_update_date,
        creation_datetime,
        action_code
    FROM source_ff_ccrm_profile_user_roles
)

SELECT * FROM final