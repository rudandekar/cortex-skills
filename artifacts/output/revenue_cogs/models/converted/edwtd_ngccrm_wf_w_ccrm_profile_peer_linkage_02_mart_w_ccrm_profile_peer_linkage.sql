{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_ccrm_profile_peer_ linkage', 'batch', 'edwtd_ngccrm'],
    meta={
        'source_workflow': 'wf_m_W_CCRM_PROFILE_PEER_ LINKAGE',
        'target_table': 'W_CCRM_PROFILE_PEER_LINKAGE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.000794+00:00'
    }
) }}

WITH 

source_el_ccrm_related_profile_hist AS (
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
        last_updated_date
    FROM {{ source('raw', 'el_ccrm_related_profile_hist') }}
),

source_st_ccrm_related_profiles AS (
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
        creation_datetime,
        action_code
    FROM {{ source('raw', 'st_ccrm_related_profiles') }}
),

final AS (
    SELECT
        bk_peer1_profile_id_int,
        bk_peer2_profile_id_int,
        linkage_dt,
        source_deleted_flg,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM source_st_ccrm_related_profiles
)

SELECT * FROM final