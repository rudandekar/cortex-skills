{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_ccrm_user_geo_role', 'batch', 'edwtd_ngccrm'],
    meta={
        'source_workflow': 'wf_m_EL_CCRM_USER_GEO_ROLE',
        'target_table': 'EL_CCRM_USER_GEO_ROLE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.339920+00:00'
    }
) }}

WITH 

source_st_ccrm_user_geo_role AS (
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
    FROM {{ source('raw', 'st_ccrm_user_geo_role') }}
),

final AS (
    SELECT
        user_id,
        share_node_id,
        role_id,
        created_by,
        creation_datetime,
        last_updated_by,
        last_update_date
    FROM source_st_ccrm_user_geo_role
)

SELECT * FROM final