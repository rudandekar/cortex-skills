{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_ccrm_non_standard_term', 'batch', 'edwtd_ngccrm'],
    meta={
        'source_workflow': 'wf_m_W_CCRM_NON_STANDARD_TERM',
        'target_table': 'W_CCRM_NON_STANDARD_TERM',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.757590+00:00'
    }
) }}

WITH 

source_st_ccrm_ns_term_types AS (
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
    FROM {{ source('raw', 'st_ccrm_ns_term_types') }}
),

final AS (
    SELECT
        bk_ccrm_non_std_term_name,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM source_st_ccrm_ns_term_types
)

SELECT * FROM final