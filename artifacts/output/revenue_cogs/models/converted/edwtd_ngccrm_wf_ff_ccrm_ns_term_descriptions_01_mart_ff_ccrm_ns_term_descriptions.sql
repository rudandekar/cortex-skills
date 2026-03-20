{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_ccrm_ns_term_descriptions', 'batch', 'edwtd_ngccrm'],
    meta={
        'source_workflow': 'wf_m_FF_CCRM_NS_TERM_DESCRIPTIONS',
        'target_table': 'FF_CCRM_NS_TERM_DESCRIPTIONS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.218333+00:00'
    }
) }}

WITH 

source_ccrm_ns_term_descriptions AS (
    SELECT
        profile_id,
        term_id,
        last_updated_by,
        last_update_date,
        description,
        created_by,
        creation_date
    FROM {{ source('raw', 'ccrm_ns_term_descriptions') }}
),

transformed_exp_ccrm_ns_term_descriptions AS (
    SELECT
    profile_id,
    term_id,
    last_updated_by,
    last_update_date,
    description,
    created_by,
    creation_date,
    'BATCH_ID' AS batch_id,
    CURRENT_TIMESTAMP() AS create_timestamp,
    'I' AS action_code
    FROM source_ccrm_ns_term_descriptions
),

final AS (
    SELECT
        batch_id,
        profile_id,
        term_id,
        last_updated_by,
        last_update_date,
        description,
        created_by,
        creation_date,
        create_timestamp,
        action_code
    FROM transformed_exp_ccrm_ns_term_descriptions
)

SELECT * FROM final