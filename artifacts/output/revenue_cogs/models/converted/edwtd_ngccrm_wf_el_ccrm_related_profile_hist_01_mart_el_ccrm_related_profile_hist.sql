{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_ccrm_related_profiles_hist', 'batch', 'edwtd_ngccrm'],
    meta={
        'source_workflow': 'wf_m_EL_CCRM_RELATED_PROFILES_HIST',
        'target_table': 'EL_CCRM_RELATED_PROFILE_HIST',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.193195+00:00'
    }
) }}

WITH 

source_st_ccrm_related_profile_hist AS (
    SELECT
        batch_id,
        history_id,
        child_profile_id,
        profile_id,
        link_flag,
        fiscal_period,
        last_updated_by,
        last_update_date,
        linkage_type,
        link_type,
        processed,
        comments,
        creation_datetime,
        action_code
    FROM {{ source('raw', 'st_ccrm_related_profile_hist') }}
),

final AS (
    SELECT
        history_id,
        child_profile_id,
        profile_id,
        link_flag,
        linkage_type,
        link_type,
        created_by,
        creation_date,
        last_updated_by,
        last_updated_date,
        processed
    FROM source_st_ccrm_related_profile_hist
)

SELECT * FROM final