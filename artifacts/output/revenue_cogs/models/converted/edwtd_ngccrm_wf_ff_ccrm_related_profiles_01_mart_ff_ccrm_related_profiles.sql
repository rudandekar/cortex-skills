{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_ccrm_related_profiles', 'batch', 'edwtd_ngccrm'],
    meta={
        'source_workflow': 'wf_m_FF_CCRM_RELATED_PROFILES',
        'target_table': 'FF_CCRM_RELATED_PROFILES',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.089404+00:00'
    }
) }}

WITH 

source_ccrm_related_profiles AS (
    SELECT
        child_profile_id,
        profile_id,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        linkage_type,
        link_type,
        processed,
        comments
    FROM {{ source('raw', 'ccrm_related_profiles') }}
),

transformed_ccrm_related_profiles AS (
    SELECT
    child_profile_id,
    profile_id,
    created_by,
    creation_date,
    last_updated_by,
    last_update_date,
    linkage_type,
    link_type,
    processed,
    comments,
    'Batch_Id' AS batch_id,
    CURRENT_TIMESTAMP() AS current_datetime,
    'I' AS action_code
    FROM source_ccrm_related_profiles
),

final AS (
    SELECT
        batch_id,
        child_profile_id,
        profile_id,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        linkage_type,
        link_type,
        processed,
        comments,
        current_datetime,
        action_code
    FROM transformed_ccrm_related_profiles
)

SELECT * FROM final