{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_ccrm_user_geo_role', 'batch', 'edwtd_ngccrm'],
    meta={
        'source_workflow': 'wf_m_FF_CCRM_USER_GEO_ROLE',
        'target_table': 'FF_CCRM_USER_GEO_ROLE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.340138+00:00'
    }
) }}

WITH 

source_ccrm_user_geo_role AS (
    SELECT
        user_id,
        share_node_id,
        role_id,
        created_by,
        creation_date,
        active
    FROM {{ source('raw', 'ccrm_user_geo_role') }}
),

transformed_exp_ccrm_user_geo_role AS (
    SELECT
    user_id,
    share_node_id,
    role_id,
    created_by,
    creation_date,
    active,
    'BATCH_ID' AS batch_id,
    CURRENT_TIMESTAMP() AS create_timestamp,
    'I' AS action_code
    FROM source_ccrm_user_geo_role
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
    FROM transformed_exp_ccrm_user_geo_role
)

SELECT * FROM final