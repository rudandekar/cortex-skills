{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_ccrm_ns_term_types', 'batch', 'edwtd_ngccrm'],
    meta={
        'source_workflow': 'wf_m_FF_CCRM_NS_TERM_TYPES',
        'target_table': 'FF_CCRM_NS_TERM_TYPES',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.458333+00:00'
    }
) }}

WITH 

source_ccrm_ns_term_types AS (
    SELECT
        ccrm_ns_term_id,
        term_name,
        active,
        created_by,
        creation_date,
        term_code,
        last_update_date,
        last_updated_by,
        profile_catg,
        nonstddesc,
        nonstdcomment,
        startdate,
        enddate
    FROM {{ source('raw', 'ccrm_ns_term_types') }}
),

transformed_exp_ccrm_ns_term_types AS (
    SELECT
    ccrm_ns_term_id,
    term_name,
    active,
    created_by,
    creation_date,
    term_code,
    last_update_date,
    last_updated_by,
    profile_catg,
    nonstddesc,
    nonstdcomment,
    startdate,
    enddate,
    'BATCH_ID' AS batch_id,
    CURRENT_TIMESTAMP() AS create_timestamp,
    'I' AS action_code
    FROM source_ccrm_ns_term_types
),

final AS (
    SELECT
        batch_id,
        ccrm_ns_term_id,
        term_name,
        active,
        created_by,
        creation_date,
        term_code,
        last_update_date,
        last_updated_by,
        profile_catg,
        nonstddesc,
        nonstdcomment,
        startdate,
        enddate,
        create_timestamp,
        action_code
    FROM transformed_exp_ccrm_ns_term_types
)

SELECT * FROM final