{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_ccrm_role', 'batch', 'edwtd_ngccrm'],
    meta={
        'source_workflow': 'wf_m_FF_CCRM_ROLE',
        'target_table': 'FF_CCRM_ROLE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.946336+00:00'
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
        role_type,
        ranking,
        rank_id
    FROM {{ source('raw', 'ccrm_role') }}
),

transformed_exp_ccrm_role AS (
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
    role_type,
    ranking,
    rank_id,
    'BatchId' AS batchid_out,
    CURRENT_TIMESTAMP() AS create_datetime,
    'I' AS action_code
    FROM source_ccrm_role
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
        create_datetime,
        action_code
    FROM transformed_exp_ccrm_role
)

SELECT * FROM final