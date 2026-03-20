{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_xxcts_dm_fnd_user_map', 'batch', 'edwtd_ca_services'],
    meta={
        'source_workflow': 'wf_m_FF_XXCTS_DM_FND_USER_MAP',
        'target_table': 'FF_XXCTS_DM_FND_USER_MAP',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.571641+00:00'
    }
) }}

WITH 

source_xxcts_dm_fnd_user_map AS (
    SELECT
        r11_user_id,
        r12_user_id
    FROM {{ source('raw', 'xxcts_dm_fnd_user_map') }}
),

final AS (
    SELECT
        r11_user_id,
        r12_user_id
    FROM source_xxcts_dm_fnd_user_map
)

SELECT * FROM final