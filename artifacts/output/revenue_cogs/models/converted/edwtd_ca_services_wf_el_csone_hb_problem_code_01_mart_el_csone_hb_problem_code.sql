{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_csone_hb_problem_code', 'batch', 'edwtd_ca_services'],
    meta={
        'source_workflow': 'wf_m_EL_CSONE_HB_PROBLEM_CODE',
        'target_table': 'EL_CSONE_HB_PROBLEM_CODE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.883077+00:00'
    }
) }}

WITH 

source_st_csone_hb_problem_code AS (
    SELECT
        hb_key,
        createddate,
        id,
        isdeleted,
        lastmodifieddate,
        problem_code_description,
        problem_code_sequence_id,
        problem_code
    FROM {{ source('raw', 'st_csone_hb_problem_code') }}
),

final AS (
    SELECT
        hb_key,
        createddate,
        id,
        isdeleted,
        lastmodifieddate,
        problem_code_description,
        problem_code_sequence_id,
        problem_code,
        edw_create_user,
        edw_update_user,
        edw_create_datetime,
        edw_update_datetime
    FROM source_st_csone_hb_problem_code
)

SELECT * FROM final