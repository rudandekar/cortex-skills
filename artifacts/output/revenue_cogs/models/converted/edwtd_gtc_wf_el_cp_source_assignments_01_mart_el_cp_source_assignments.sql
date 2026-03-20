{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_cp_source_assignments', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_EL_CP_SOURCE_ASSIGNMENTS',
        'target_table': 'EL_CP_SOURCE_ASSIGNMENTS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.720379+00:00'
    }
) }}

WITH 

source_el_cp_source_assignments AS (
    SELECT
        source_id,
        source_assignment_id,
        source_assignment_name,
        ss_code,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm
    FROM {{ source('raw', 'el_cp_source_assignments') }}
),

final AS (
    SELECT
        source_id,
        source_assignment_id,
        source_assignment_name,
        ss_code,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm
    FROM source_el_cp_source_assignments
)

SELECT * FROM final