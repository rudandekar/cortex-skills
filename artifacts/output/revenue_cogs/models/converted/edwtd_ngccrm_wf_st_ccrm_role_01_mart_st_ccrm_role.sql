{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_ccrm_role', 'batch', 'edwtd_ngccrm'],
    meta={
        'source_workflow': 'wf_m_ST_CCRM_ROLE',
        'target_table': 'ST_CCRM_ROLE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.178967+00:00'
    }
) }}

WITH 

source_ff_ccrm_role AS (
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
        current_datetime,
        action_code
    FROM {{ source('raw', 'ff_ccrm_role') }}
),

final AS (
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
    FROM source_ff_ccrm_role
)

SELECT * FROM final