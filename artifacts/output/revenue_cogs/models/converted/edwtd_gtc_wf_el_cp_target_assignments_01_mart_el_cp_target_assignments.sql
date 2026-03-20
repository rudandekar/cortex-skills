{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_cp_target_assignments', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_EL_CP_TARGET_ASSIGNMENTS',
        'target_table': 'EL_CP_TARGET_ASSIGNMENTS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.718149+00:00'
    }
) }}

WITH 

source_el_cp_target_assignments AS (
    SELECT
        target_assignment_id,
        target_acct_currency,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm,
        ss_code
    FROM {{ source('raw', 'el_cp_target_assignments') }}
),

final AS (
    SELECT
        target_assignment_id,
        target_acct_currency,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm,
        ss_code
    FROM source_el_cp_target_assignments
)

SELECT * FROM final