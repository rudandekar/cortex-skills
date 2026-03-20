{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_ccrm_ns_term_types', 'batch', 'edwtd_ngccrm'],
    meta={
        'source_workflow': 'wf_m_ST_CCRM_NS_TERM_TYPES',
        'target_table': 'ST_CCRM_NS_TERM_TYPES',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.618262+00:00'
    }
) }}

WITH 

source_ff_ccrm_ns_term_types AS (
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
    FROM {{ source('raw', 'ff_ccrm_ns_term_types') }}
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
    FROM source_ff_ccrm_ns_term_types
)

SELECT * FROM final