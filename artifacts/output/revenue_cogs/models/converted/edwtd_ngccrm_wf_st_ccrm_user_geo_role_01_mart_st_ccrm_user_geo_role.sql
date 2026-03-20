{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_ccrm_user_geo_role', 'batch', 'edwtd_ngccrm'],
    meta={
        'source_workflow': 'wf_m_ST_CCRM_USER_GEO_ROLE',
        'target_table': 'ST_CCRM_USER_GEO_ROLE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.699190+00:00'
    }
) }}

WITH 

source_ccrm_role AS (
    SELECT
        role_id,
        role_key,
        display_name,
        active,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        grant_level_limit,
        role_type
    FROM {{ source('raw', 'ccrm_role') }}
),

source_ccrm_hr_employee AS (
    SELECT
        user_id,
        init_cap_fname,
        init_cap_lname,
        dept_name
    FROM {{ source('raw', 'ccrm_hr_employee') }}
),

source_ff_ccrm_user_geo_role AS (
    SELECT
        batch_id,
        user_id,
        share_node_id,
        role_id,
        created_by,
        creation_date,
        active,
        create_timestamp,
        action_code
    FROM {{ source('raw', 'ff_ccrm_user_geo_role') }}
),

final AS (
    SELECT
        batch_id,
        user_id,
        share_node_id,
        role_id,
        created_by,
        creation_date,
        active,
        create_timestamp,
        action_code
    FROM source_ff_ccrm_user_geo_role
)

SELECT * FROM final