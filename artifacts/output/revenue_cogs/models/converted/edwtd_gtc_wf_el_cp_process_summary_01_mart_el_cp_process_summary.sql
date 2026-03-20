{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_cp_process_summary', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_EL_CP_PROCESS_SUMMARY',
        'target_table': 'EL_CP_PROCESS_SUMMARY',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.540476+00:00'
    }
) }}

WITH 

source_el_cp_process_summary AS (
    SELECT
        process_summary_id,
        batch_name,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm,
        ss_code,
        set_of_books_id,
        accounted_dr,
        accounted_cr,
        entered_dr,
        entered_cr,
        segment1,
        segment2,
        segment3,
        segment4,
        segment5,
        segment6
    FROM {{ source('raw', 'el_cp_process_summary') }}
),

final AS (
    SELECT
        process_summary_id,
        batch_name,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm,
        ss_code,
        set_of_books_id,
        accounted_dr,
        accounted_cr,
        entered_dr,
        entered_cr,
        segment1,
        segment2,
        segment3,
        segment4,
        segment5,
        segment6
    FROM source_el_cp_process_summary
)

SELECT * FROM final