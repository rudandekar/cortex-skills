{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_ccrm_profile_peer_linkage', 'batch', 'edwtd_ngccrm'],
    meta={
        'source_workflow': 'wf_m_N_CCRM_PROFILE_PEER_LINKAGE',
        'target_table': 'N_CCRM_PROFILE_PEER_LINKAGE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.823438+00:00'
    }
) }}

WITH 

source_w_ccrm_profile_peer_linkage AS (
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
    FROM {{ source('raw', 'w_ccrm_profile_peer_linkage') }}
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
        edw_update_user
    FROM source_w_ccrm_profile_peer_linkage
)

SELECT * FROM final