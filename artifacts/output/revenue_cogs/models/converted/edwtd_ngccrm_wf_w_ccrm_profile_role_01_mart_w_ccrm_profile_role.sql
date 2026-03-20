{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_ccrm_profile_role', 'batch', 'edwtd_ngccrm'],
    meta={
        'source_workflow': 'wf_m_W_CCRM_PROFILE_ROLE',
        'target_table': 'W_CCRM_PROFILE_ROLE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.980874+00:00'
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
        bk_ccrm_profile_role_cd,
        ccrm_profile_role_descr,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM source_st_ccrm_role
)

SELECT * FROM final