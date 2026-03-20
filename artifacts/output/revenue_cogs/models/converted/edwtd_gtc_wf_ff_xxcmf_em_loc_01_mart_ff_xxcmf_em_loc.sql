{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_xxcmf_em_loc', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_FF_XXCMF_EM_LOC',
        'target_table': 'FF_XXCMF_EM_LOC',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.442484+00:00'
    }
) }}

WITH 

source_xxcmf_em_loc AS (
    SELECT
        site_id,
        location,
        country,
        created_by,
        last_updated_by,
        created_date,
        last_updated_date,
        status
    FROM {{ source('raw', 'xxcmf_em_loc') }}
),

transformed_exp_xxcmf_em_loc AS (
    SELECT
    site_id,
    location,
    country,
    created_by,
    last_updated_by,
    created_date,
    last_updated_date,
    status,
    'BatchId' AS batch_id,
    CURRENT_TIMESTAMP() AS create_datetime,
    'I' AS action_code
    FROM source_xxcmf_em_loc
),

final AS (
    SELECT
        batch_id,
        site_id,
        location,
        country,
        created_by,
        last_updated_by,
        created_date,
        last_updated_date,
        status,
        create_datetime,
        action_code
    FROM transformed_exp_xxcmf_em_loc
)

SELECT * FROM final