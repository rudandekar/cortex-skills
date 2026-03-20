{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_ccrm_role', 'batch', 'edwtd_ngccrm'],
    meta={
        'source_workflow': 'wf_m_EL_CCRM_ROLE',
        'target_table': 'EL_CCRM_ROLE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.563485+00:00'
    }
) }}

WITH 

source_st_ccrm_role AS (
    SELECT
        batch_id,
        role_id,
        role_key,
        display_name,
        active,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        grant_level_limit,
        role_type,
        ranking,
        rank_id,
        creation_datetime,
        action_code
    FROM {{ source('raw', 'st_ccrm_role') }}
),

final AS (
    SELECT
        role_id,
        role_key,
        display_name,
        active,
        grant_level_limit,
        role_type,
        creation_datetime,
        created_by,
        last_updated_by,
        last_update_date
    FROM source_st_ccrm_role
)

SELECT * FROM final