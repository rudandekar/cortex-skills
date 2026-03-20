{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_ccrm_profile_user_roles', 'batch', 'edwtd_ngccrm'],
    meta={
        'source_workflow': 'wf_m_W_CCRM_PROFILE_USER_ROLES',
        'target_table': 'EX_CCRM_PROFILE_USER_ROLES',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.057604+00:00'
    }
) }}

WITH 

source_el_ccrm_role AS (
    SELECT
        role_id,
        role_key,
        display_name,
        active,
        last_updated_by,
        last_update_date,
        grant_level_limit,
        role_type,
        creation_datetime
    FROM {{ source('raw', 'el_ccrm_role') }}
),

source_st_ccrm_profile_user_roles AS (
    SELECT
        batch_id,
        user_id,
        role_id,
        profile_id,
        last_updated_by,
        last_update_date,
        creation_datetime,
        action_code
    FROM {{ source('raw', 'st_ccrm_profile_user_roles') }}
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
        action_code,
        exception_type
    FROM source_st_ccrm_profile_user_roles
)

SELECT * FROM final