{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_ccrm_profile_non_std_term', 'batch', 'edwtd_ngccrm'],
    meta={
        'source_workflow': 'wf_m_W_CCRM_PROFILE_NON_STD_TERM',
        'target_table': 'W_CCRM_PROFILE_NON_STD_TERM',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.394304+00:00'
    }
) }}

WITH 

source_el_ccrm_ns_term_types AS (
    SELECT
        ccrm_ns_term_id,
        term_name,
        created_by,
        creation_date,
        last_updated_by,
        last_updated_date
    FROM {{ source('raw', 'el_ccrm_ns_term_types') }}
),

source_st_ccrm_ns_term_descriptions AS (
    SELECT
        batch_id,
        profile_id,
        term_id,
        last_update_date,
        last_updated_by,
        description,
        created_by,
        creation_date,
        create_timestamp,
        action_code
    FROM {{ source('raw', 'st_ccrm_ns_term_descriptions') }}
),

final AS (
    SELECT
        bk_ccrm_non_std_term_name,
        bk_ccrm_profile_id_int,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM source_st_ccrm_ns_term_descriptions
)

SELECT * FROM final