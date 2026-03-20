{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_sr_tac_work_theater', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_W_SR_TAC_WORK_THEATER',
        'target_table': 'W_SR_TAC_WORK_THEATER',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.106403+00:00'
    }
) }}

WITH 

source_w_sr_tac_work_theater AS (
    SELECT
        sr_tac_work_theater_code,
        work_theater_is_valid_flag,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_sr_tac_work_theater') }}
),

final AS (
    SELECT
        sr_tac_work_theater_code,
        work_theater_is_valid_flag,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM source_w_sr_tac_work_theater
)

SELECT * FROM final